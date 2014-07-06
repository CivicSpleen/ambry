"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import os
from .  import Cache
from ..util import copy_file_or_flo, get_logger

from ..identity import Identity

global_logger = get_logger(__name__)

def make_metadata(file_name):
    from ambry.util import md5_for_file
    return dict(
        md5 = md5_for_file(file_name)
    )


class FsCache(Cache):
    '''A cache that transfers files to and from a remote filesystem
    
    The `FsCache` stores files in a filesystem, possily retrieving and storing
    files to an upstream cache. 
    
    When files are written , they are written through to the upstream. If a file
    is requested that does not exist, it is fetched from the upstream. 
    
    When a file is added that causes the disk usage to exceed `maxsize`, the oldest
    files are deleted to free up space. 
    '''

    base_priority = 10  # Priority for this class of cache.

    def __init__(self, dir=None,  options=None, upstream=None,**kwargs):
        '''Init a new FileSystem Cache
        
        Args:
            cache_dir
            maxsize. Maximum size of the cache, in GB
        
        '''

        from ambry.dbexceptions import ConfigurationError
        
        
        super(FsCache, self).__init__(upstream)
        
        self._cache_dir = dir

        if not os.path.isdir(self._cache_dir):
            os.makedirs(self._cache_dir)
        
        if not os.path.isdir(self._cache_dir):
            raise ConfigurationError("Cache dir '{}' is not valid".format(self._cache_dir)) 

    ##


    def put(self, source, rel_path, metadata=None):
        '''Copy a file to the repository

        Args:
            source: Absolute path to the source file, or a file-like object
            rel_path: path relative to the root of the repository

        '''

        if isinstance(rel_path, Identity):
            rel_path = rel_path.cache_key

        sink = self.put_stream(rel_path, metadata=metadata)

        try:
            copy_file_or_flo(source, sink)
        except (KeyboardInterrupt, SystemExit):
            path_ = self.path(rel_path)
            if os.path.exists(path_):
                os.remove(path_)
            raise

        sink.close()

        return os.path.join(self.cache_dir, rel_path)

    def put_stream(self,rel_path, metadata=None):
        """return a file object to write into the cache. The caller
        is responsibile for closing the stream
        """
        from io import IOBase

        if isinstance(rel_path, Identity):
            rel_path = rel_path.cache_key

        repo_path = os.path.join(self.cache_dir, rel_path.strip("/"))

        if not os.path.isdir(os.path.dirname(repo_path)):
            os.makedirs(os.path.dirname(repo_path))

        if os.path.exists(repo_path):
            os.remove(repo_path)

        sink = open(repo_path,'wb')

        upstream = self.upstream

        class flo(IOBase):
            '''This File-Like-Object class ensures that the file is also
            sent to the upstream after it is stored in the FSCache. '''
            def __init__(self, sink, upstream, repo_path, rel_path):

                self._sink = sink
                self._upstream = upstream
                self._repo_path = repo_path
                self._rel_path = rel_path


            def write(self, str_):
                self._sink.write(str_)

            def close(self):
                if not self._sink.closed:
                    #print "Closing put_stream.flo {} is_closed={}!".format(self._repo_path, self._sink.closed)

                    self._sink.close()

                    if self._upstream and not self._upstream.readonly and not self._upstream.usreadonly:
                        self._upstream.put(self._repo_path, self._rel_path, metadata=metadata)


        self.put_metadata(rel_path, metadata)

        return flo(sink, upstream, repo_path, rel_path)

    ##
    ## Retrieve
    ##

    def get(self, rel_path, cb=None):
        '''Return the file path referenced but rel_path, or None if
        it can't be found. If an upstream is declared, it will try to get the file
        from the upstream before declaring failure. 
        '''
        import shutil

        global_logger.debug("FC {} get looking for {}".format(self.repo_id,rel_path))
               
        path = os.path.join(self.cache_dir, rel_path)
      
        # If is already exists in the repo, just return it. 
        if  os.path.exists(path):
            
            if not os.path.isfile(path):
                raise ValueError("Path does not point to a file")
            
            global_logger.debug("FC {} get {} found ".format(self.repo_id, path))
            return path
            
        if not self.upstream:
            # If we don't have an upstream, then we are done. 
            return None
     
        stream = self.upstream.get_stream(rel_path, cb=cb)
        
        if not stream:
            global_logger.debug("FC {} get not found in upstream ()".format(self.repo_id,rel_path))
            return None
        
        # Got a stream from upstream, so put the file in this cache. 
        dirname = os.path.dirname(path)
        if not os.path.isdir(dirname):
            os.makedirs(dirname)

        try:
            with open(path,'w') as f:
                #shutil.copyfileobj(stream, f)
                copy_file_or_flo(stream, f, cb=cb)
        except:
            os.remove(path)
            raise

        try:
            stream.close()
        except AttributeError:
            pass # HTTPResponse objects don't have close()
        
        if not os.path.exists(path):
            raise Exception("Failed to copy upstream data to {} ".format(path))
        
        global_logger.debug("FC {} got return from upstream {}".format(self.repo_id,rel_path))
        return path

    def get_stream(self, rel_path, cb=None):
        p = self.get(rel_path)
        
        if p:
            from ..util.flo import MetadataFlo
            return MetadataFlo(open(p),self.metadata(rel_path))
     
        if not self.upstream:
            return None

        return self.upstream.get_stream(rel_path, cb=cb)

    ##
    ## Information
    ##

    def list(self, path=None,with_metadata=False, include_partitions=False):
        '''get a list of all of the files in the repository'''
        from os import walk
        import json

        l = {}


        for root, dirs, files in os.walk(self._cache_dir):
            root = root.replace(self._cache_dir,'',1).strip('/')

            if root.startswith('meta'):
                continue

            if not include_partitions and root.count('/') > 0:
                continue  # partition files

            for f in files:
                rel_path = os.path.join(root,f)
                d = self.metadata(rel_path) if with_metadata else {}

                if d and 'identity' in d:
                    d['identity'] = json.loads(d['identity'])

                d['caches'] = [self.repo_id]

                l[rel_path] = d

        if self.upstream:
            for k, v in self.upstream.list(path, with_metadata, include_partitions).items():
                upstreams = (l[k]['caches'] if k in l else []) + v.get('caches', self.upstream.repo_id)

                l[k] = v
                l[k]['caches'] = upstreams

        return l


    def path(self, rel_path, propagate = True, **kwargs):

        abs_path = os.path.join(self.cache_dir, rel_path)

        if os.path.exists(abs_path):
            return abs_path

        if self.upstream and propagate:
            return self.upstream.path(rel_path, **kwargs)

        if not os.path.exists(abs_path):
            return False

        return abs_path

    def md5(self, rel_path):
        from ambry.util import md5_for_file

        abs_path = self.path(rel_path)

        if not abs_path:
            return None

        return md5_for_file(abs_path)


    def metadata(self,rel_path):
        import json

        if rel_path.startswith('meta'):
            return None

        path = self.get(os.path.join('meta',rel_path))

        if path and os.path.exists(path):
            try:
                with open(path) as f:
                    return json.load(f)
            except ValueError as e:
                raise ValueError("Failed to decode json for key '{}',  {}".format(rel_path, self.path(os.path.join('meta',rel_path))))
        else:
            return {}

    def has(self, rel_path, md5=None, propagate=True):
        from ..util import md5_for_file

        abs_path = os.path.join(self.cache_dir, rel_path)


        if os.path.exists(abs_path) and ( not md5 or md5 == md5_for_file(abs_path)):
            return abs_path

        if self.upstream and propagate:
            return self.upstream.has(rel_path, md5=md5, propagate=propagate)

        return False

    ##
    ## Delete
    ##

    def remove(self,rel_path, propagate = False):
        '''Delete the file from the cache, and from the upstream'''
        repo_path = os.path.join(self.cache_dir, rel_path)

        if os.path.exists(repo_path):
            os.remove(repo_path)

        if self.upstream and propagate :
            self.upstream.remove(rel_path, propagate)    
            
    def clean(self):
        from ..util import rm_rf
        
        global_logger.info("Purging: {} ".format(self.cache_dir))
        rm_rf(self.cache_dir)

        if self.upstream:
            self.upstream.clean()


    @property
    def cache_dir(self):
        return self._cache_dir

    @property
    def repo_id(self):
        '''Return the ID for this repository'''

        return self.cache_dir

        #import hashlib
        #m = hashlib.md5()
        #m.update(self.cache_dir)

        #return m.hexdigest()


    def __repr__(self):
        return "FsCache: dir={} upstream=({})".format(self.cache_dir, self.upstream)




class FsLimitedCache(FsCache):
    '''A cache that transfers files to and from a remote filesystem
    
    The `FsCache` stores files in a filesystem, possily retrieving and storing
    files to an upstream cache. 
    
    When files are written , they are written through to the upstream. If a file
    is requested that does not exist, it is fetched from the upstream. 
    
    When a file is added that causes the disk usage to exceed `maxsize`, the oldest
    files are deleted to free up space. 
    
     '''

    def __init__(self, dir=dir, size=10000, upstream=None,**kwargs):
        '''Init a new FileSystem Cache
        
        Args:
            cache_dir
            maxsize. Maximum size of the cache, in GB
        
        '''
        
        from ambry.dbexceptions import ConfigurationError

        super(FsLimitedCache, self).__init__(dir, upstream=upstream,**kwargs)
        
        self.maxsize = int(size) * 1048578  # size in MB

        self.readonly = False
        self.usreadonly = False
        self._database = None
        
        self.use_db = True
   
        if not os.path.isdir(self.cache_dir):
            os.makedirs(self.cache_dir)
        
        if not os.path.isdir(self.cache_dir):
            raise ConfigurationError("Cache dir '{}' is not valid".format(self.cache_dir)) 
        
    @property
    def database_path(self):
        return  os.path.join(self.cache_dir, 'file_database.db')
        
    @property
    def database(self):
        import sqlite3
        
        if not self.use_db:
            raise Exception("Shoundn't get here")
        
        if not self._database:
            db_path = self.database_path
            
            if not os.path.exists(db_path):
                create_sql = """
                CREATE TABLE files(
                path TEXT UNIQUE ON CONFLICT REPLACE, 
                size INTEGER, 
                time REAL)
                """
                conn = sqlite3.connect(db_path)
                conn.execute(create_sql)
                conn.close()
                
            # Long timeout to deal with contention during multiprocessing use
            self._database = sqlite3.connect(db_path,60)
            
        return self._database
            
    @property
    def size(self):
        '''Return the size of all of the files referenced in the database'''
        c = self.database.cursor()
        r = c.execute("SELECT sum(size) FROM files")
     
        try:
            size = int(r.fetchone()[0])
        except TypeError:
            size = 0
    
        return size

    def _free_up_space(self, size, this_rel_path=None):
        '''If there are not size bytes of space left, delete files
        until there is 
        
        Args:
            size: size of the current file
            this_rel_path: rel_pat to the current file, so we don't delete it. 
        
        ''' 
        
        space = self.size + size - self.maxsize # Amount of space we are over ( bytes ) for next put
        
        if space <= 0:
            return

        removes = []

        for row in self.database.execute("SELECT path, size, time FROM files ORDER BY time ASC"):

            if space > 0:
                removes.append(row[0])
                space -= row[1]
            else:
                break
  
        for rel_path in removes:
            if rel_path != this_rel_path:
                global_logger.debug("Deleting {}".format(rel_path))
                self.remove(rel_path)
            
    def add_record(self, rel_path, size):
        import time
        c = self.database.cursor()
        try:
            c.execute("insert into files(path, size, time) values (?, ?, ?)", 
                        (rel_path, size, time.time()))
            self.database.commit()
        except Exception as e:
            from  ..dbexceptions import FilesystemError

            raise FilesystemError("Failed to write to cache database '{}': {}"
                                               .format(self.database_path, e.message))

    def verify(self):
        '''Check that the database accurately describes the state of the repository'''
        
        c = self.database.cursor()
        non_exist = set()
        
        no_db_entry = set(os.listdir(self.cache_dir))
        try:
            no_db_entry.remove('file_database.db')
            no_db_entry.remove('file_database.db-journal')
        except: 
            pass
        
        for row in c.execute("SELECT path FROM files"):
            path = row[0]
            
            repo_path = os.path.join(self.cache_dir, path)
        
            if os.path.exists(repo_path):
                no_db_entry.remove(path)
            else:
                non_exist.add(path)
            
        if len(non_exist) > 0:
            raise Exception("Found {} records in db for files that don't exist: {}"
                            .format(len(non_exist), ','.join(non_exist)))
            
        if len(no_db_entry) > 0:
            raise Exception("Found {} files that don't have db entries: {}"
                            .format(len(no_db_entry), ','.join(no_db_entry)))

    def get_stream(self, rel_path, cb=None):
        p = self.get(rel_path, cb=cb)
        
        if p:
            from ..util.flo import MetadataFlo
            return MetadataFlo(open(p),self.metadata(rel_path))
     
        if not self.upstream:
            return None

        return self.upstream.get_stream(rel_path, cb=cb)
        
    
    def get(self, rel_path, cb=None):
        '''Return the file path referenced but rel_path, or None if
        it can't be found. If an upstream is declared, it will try to get the file
        from the upstream before declaring failure. 
        '''
        import shutil
        from ambry.util import bundle_file_type

        global_logger.debug("LC {} get looking for {}".format(self.repo_id,rel_path))
               
        path = os.path.join(self.cache_dir, rel_path)

        # If is already exists in the repo, just return it. 
        if  os.path.exists(path):
            
            if not os.path.isfile(path):
                raise ValueError("Path does not point to a file")
            
            global_logger.debug("LC {} get {} found ".format(self.repo_id, path))
            return path
            
        if not self.upstream:
            # If we don't have an upstream, then we are done. 
            return None
     
        
        stream = self.upstream.get_stream(rel_path, cb=cb)
        
        if not stream:
            global_logger.debug("LC {} get not found in upstream ()".format(self.repo_id,rel_path))
            return None
        
        # Got a stream from upstream, so put the file in this cache. 
        dirname = os.path.dirname(path)
        if not os.path.isdir(dirname):
            os.makedirs(dirname)
        
        # Copy the file from the lower cache into this cache. 
        with open(path,'w') as f:
            shutil.copyfileobj(stream, f)

                
        # Since we've added a file, must keep track of the sizes. 
        size = os.path.getsize(path)
        self._free_up_space(size, this_rel_path=rel_path)
        self.add_record(rel_path, size)
        
        stream.close()
        
        if not os.path.exists(path):
            raise Exception("Failed to copy upstream data to {} ".format(path))
        
        global_logger.debug("LC {} got return from upstream {} -> {} ".format(self.repo_id,rel_path, path))
        return path


    def put(self, source, rel_path, metadata=None):
        '''Copy a file to the repository
        
        Args:
            source: Absolute path to the source file, or a file-like object
            rel_path: path relative to the root of the repository
        
        '''
        if isinstance(rel_path, Identity):
            rel_path = rel_path.cache_key


        sink = self.put_stream(rel_path, metadata=metadata)
        
        try:
            copy_file_or_flo(source, sink)
        except (KeyboardInterrupt, SystemExit):
            path_ = self.path(rel_path)
            if os.path.exists(path_):
                os.remove(path_)
            raise
        
        sink.close()


        if self.upstream:
            self.upstream.put(source, rel_path, metadata=metadata)

        return self.get(rel_path)

    def put_stream(self,rel_path, metadata=None):
        """return a file object to write into the cache. The caller
        is responsibile for closing the stream. Bad things happen
        if you dont close the stream
        """
        
        class flo:
            def __init__(self, this, sink, upstream, repo_path):
                self.this = this
                self.sink = sink
                self.upstream = upstream
                self.repo_path = repo_path
            
            @property
            def repo_path(self):
                return self.repo_path
            
            def write(self, d):
                self.sink.write(d)
                
                if self.upstream:
                    self.upstream.write(d)
                
            def writelines(self, lines):
                raise NotImplemented()
            
            def close(self):
                self.sink.close()
                
                size = os.path.getsize(self.repo_path)
                
                self.this.add_record(rel_path, size)
                self.this._free_up_space(size, this_rel_path=rel_path)
                
                if self.upstream:
                    self.upstream.close()
        
        if isinstance(rel_path, Identity):
            rel_path = rel_path.cache_key
        
        repo_path = os.path.join(self.cache_dir, rel_path.strip('/'))
      
        if not os.path.isdir(os.path.dirname(repo_path)):
            os.makedirs(os.path.dirname(repo_path))
     
             
        self.put_metadata(rel_path, metadata=metadata) 
           
        sink = open(repo_path,'w+')
        upstream = self.upstream.put_stream(rel_path, metadata=metadata) if self.upstream else None
       
        return flo(self, sink, upstream, repo_path)
    
    def find(self,query):
        '''Passes the query to the upstream, if it exists'''
        if self.upstream:
            return self.upstream.find(query)
        else:
            return False
    
    def remove(self,rel_path, propagate = False):
        '''Delete the file from the cache, and from the upstream'''
        repo_path = os.path.join(self.cache_dir, rel_path)
        
        c = self.database.cursor()
        c.execute("DELETE FROM  files WHERE path = ?", (rel_path,) )
        
        if os.path.exists(repo_path):
            os.remove(repo_path)

        self.database.commit()
            
        if self.upstream and propagate :
            self.upstream.remove(rel_path, propagate)    


    def list(self, path=None,with_metadata=False):
        '''get a list of all of the files in the repository'''
        
        
        path = path.strip('/') if path else ''
        
        if self.upstream:
            return self.upstream.list(path, with_metadata=with_metadata)
        else:
            raise NotImplementedError()

    def clean(self):
        if self._database:
            self._database.close()
            self._database = None
        super(FsLimitedCache, self).clean()

    @property
    def priority(self):
        return self.upstream.priority - 1  # give a slightly better priority

    def set_priority(self, i):
        self.upstream.set_priority(i)


    def __repr__(self):
        return "FsLimitedCache: dir={} size={} upstream=({})".format(self.cache_dir, self.maxsize, self.upstream)
    

class FsCompressionCache(Cache):
    
    '''A Cache Adapter that compresses files before sending  them to
    another cache.
     '''

    def __init__(self, upstream=None,**kwargs):
        
        super(FsCompressionCache, self).__init__(upstream)


    @property
    def priority(self):
        return self.upstream.priority - 1 # give a slightly better priority

    def set_priority(self,i):
        self.upstream.set_priority(i)

    ##
    ## Put
    ##


    def put(self, source, rel_path, metadata=None):
        from ambry.util import bundle_file_type
        import gzip

        # Pass through if the file is already compressed


        if not metadata:
            metadata = {}

        metadata['Content-Encoding'] = 'gzip'

        bft = bundle_file_type(source)

        sink = self.upstream.put_stream(self._rename(rel_path), metadata = metadata)

        if bft == 'gzip':
            copy_file_or_flo(source,  sink)
        else:
            copy_file_or_flo(source,  gzip.GzipFile(fileobj=sink,  mode='wb'))

        sink.close()

        #self.put_metadata(rel_path, metadata)

        return self.path(self._rename(rel_path))

    def put_stream(self, rel_path,  metadata=None):
        import gzip

        if not metadata:
            metadata = {}

        metadata['Content-Encoding'] = 'gzip'

        sink = self.upstream.put_stream(self._rename(rel_path),  metadata=metadata)


        self.put_metadata(rel_path, metadata)

        return gzip.GzipFile(fileobj=sink,  mode='wb')

    ##
    ## Get
    ##

    def get_stream(self, rel_path, cb=None):
        from ..util import bundle_file_type
        from ..util.flo import MetadataFlo
        import gzip

        source = self.upstream.get_stream(self._rename(rel_path))

        if not source:
            return None

        if bundle_file_type(source) == 'gzip':
            global_logger.debug("CC returning {} with decompression".format(rel_path))
            return MetadataFlo(gzip.GzipFile(fileobj=source), source.meta)
        else:
            global_logger.debug("CC returning {} with passthrough".format(rel_path))
            return source

    def get(self, rel_path, cb=None):

        source = self.get_stream(rel_path)

        if not source:
            raise Exception('Failed to get a source for rel_path {} for {}, upstream {} '.format(rel_path, self, self.upstream))

        uc_rel_path = os.path.join('uncompressed',rel_path)

        # The compression cache doesn't ahve storage, so it writes the uncompressed file back to the upstream,
        # hoping that it is a real filesystem.
        sink = self.upstream.put_stream(uc_rel_path)

        try:
            copy_file_or_flo(source, sink, cb=cb)
        except (KeyboardInterrupt, SystemExit):
            path_ = self.upstream.path(uc_rel_path)
            if os.path.exists(path_):
                os.remove(path_)
            raise

        return self.path(self._rename(rel_path))

    def find(self,query):
        '''Passes the query to the upstream, if it exists'''
        if not self.upstream:
            raise Exception("CompressionCache must have an upstream")

        return self.upstream.find(query)


    ##
    ## Delete
    ##


    def remove(self,rel_path, propagate = False):
        '''Delete the file from the cache, and from the upstream'''

        if not self.upstream:
            raise Exception("CompressionCache must have an upstream")

        # Must always propagate, since this is really just a filter.
        self.upstream.remove(self._rename(rel_path), propagate)

        # In case someone called get()
        uc_rel_path = os.path.join('uncompressed',rel_path)

        self.upstream.remove(uc_rel_path)


    ##
    ## Information
    ##

    @property
    def remote(self):
        '''Return a reference to an inner cache that is a remote'''
        raise DeprecationWarning()
        return self.upstream.upstream

    def path(self, rel_path, **kwargs):
        return self.upstream.path(self._rename(rel_path), **kwargs)

    def md5(self, rel_path):
        '''Return the MD5 for the file. Since the file is compressed,
        this will rely on the metadata'''

        md = self.metadata(rel_path)

        return md['md5']

    def list(self, path=None,with_metadata=False, include_partitions=False):
        '''get a list of all of the files in the repository'''
        l =  self.upstream.list(path,with_metadata=with_metadata, include_partitions=include_partitions)


        lp = {}
        for k,v in l.items():
            lp[k.replace('.gz','')] = v


        return lp


    def store_list(self,  cb=None):
        """List the cache and store it as metadata. This allows for getting the list from HTTP caches
        and other types where it is not possible to traverse the tree"""
        return self.upstream.store_list( cb=cb)

    def has(self, rel_path, md5=None, propagate=True):
        
        # This odd structure is because the MD5 check won't work if it is computed on a uncompressed
        # file and checked on a compressed file. But it will work if the check is done on an s#
        # file, which stores the md5 as metadada

        r =  self.upstream.has(self._rename(rel_path), md5=md5, propagate=propagate)

        if r:
            return True

        return self.upstream.has(self._rename(rel_path), md5=None, propagate=propagate)

    def metadata(self, rel_path):
        return self.upstream.metadata(self._rename(rel_path))


    @staticmethod
    def _rename( rel_path):
        return rel_path+".gz" if not rel_path.endswith('.gz') else rel_path


    @property
    def repo_id(self):
        return self.upstream.repo_id+'#compressed'

    @property
    def cache_dir(self):
        return self.upstream.cache_dir



    def __repr__(self):
        return "FsCompressionCache: upstream=({})".format(self.upstream)
    

