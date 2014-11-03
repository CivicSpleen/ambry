
from .  import Cache
from ..util import copy_file_or_flo, get_logger
import os
import logging

global_logger = get_logger(__name__)

#global_logger.setLevel(logging.DEBUG)

class S3Cache(Cache):
    '''A cache that transfers files to and from an S3 bucket
    
     '''

    base_priority = 30  # Priority for this class of cache.

    def __init__(self, bucket=None, prefix=None, account=None, upstream=None, cdn=None, options = None, **kwargs):
        '''Init a new S3Cache Cache

        '''
        from boto.s3.connection import S3Connection

        super(S3Cache, self).__init__(upstream=upstream)


        self.is_remote = False
        self.bucket_name = bucket
        self.prefix = prefix

        self.account = account
        self.access_key = account['access']
        self.secret = account['secret']

        self.conn = S3Connection(self.access_key, self.secret, is_secure = False )
        self.bucket = self.conn.get_bucket(self.bucket_name)

        self.options = options

        self.cdn = None
        self.cdn_config = None

        if cdn:
            self._init_cdn(cdn)

    def clone(self):
        from boto.s3.connection import S3Connection

        return S3Cache(bucket=self.bucket, prefix=self.prefix, account=self.account,
                       options = self.options, upstream=self.upstream, cdn=self.cdn, **self.args)

    def _init_cdn(self, config):
        import boto
        import time
        from boto.cloudfront import CloudFrontConnection
        from boto.cloudfront.distribution import Distribution
        
        self.cdn_config = config

        class _cdn(object):
            conn = CloudFrontConnection(config['access'], config['secret'])
            dist = Distribution(connection=conn,  id=config['id'])
            domain_name=config['domain']
            key_pair_id = config['key_pair_id'] #from the AWS accounts page
            priv_key_file = config['key_file'] #your private keypair file
            expires =  config['expire'] 
              
            def sign_url(self, rel_key):
                
                resource = 'http://{}/{}'.format(self.domain_name, rel_key)
                
                return self.dist.create_signed_url(
                                              resource, 
                                              self.key_pair_id, 
                                              int(time.time()) + self.expires , 
                                              private_key_file= self.priv_key_file)
                    
        self.cdn = _cdn()
        
  
    def _rename(self, rel_path):
        '''Remove the .gz suffix that may have been added by a compression cache.
        In s3, compression is indicated by the content encoding.  '''
        import re
        rel_path =  re.sub('\.gz$','',rel_path)
        return rel_path
        
    def _prefix(self, rel_path):
        
        if self.prefix is not None:
            return self.prefix+"/"+rel_path
        else:
            return rel_path

    def path(self, rel_path, propagate=True, missing_ok = False, **kwargs):
        import re

        if self.cdn:
            rel_path = self._rename(rel_path)
            path = self._prefix(rel_path)
   
            return self.cdn.sign_url(path)
        else:

            if 'method' in kwargs:
                method = kwargs['method'].upper()
            else:
                method = 'GET'

            k = self._get_boto_key(rel_path, validate = not missing_ok)

            if not k:
                from ..dbexceptions import NotFoundError
                raise NotFoundError("Didn't find key for bucket='{}' prefix='{}', path='{}' ".format(self.bucket_name, self.prefix, rel_path))

            if kwargs.get('public_url', False):
                url =  k.generate_url(1, method=method)
                url, _ = url.split('?',2)

            else:
                if kwargs.get('expire_time', False):
                    time = int(kwargs['expire_time'])
                else:
                    time = 300

                url =  k.generate_url(time, method=method) # expires in 5 minutes

            if kwargs.get('use_cname', False):
                # For websites, assume there is a CNAME, so remove the AWS part
                url = re.sub(r'\.s3-website[^\.]+\.amazonaws.com','',url)
                url = re.sub(r'\.s3.amazonaws.com', '', url)

            return url

    def s3path(self, rel_path):
        """Return the path as an S3 schema"""
        import urlparse

        path = self.path(rel_path, public_url = True)

        parts = list(urlparse.urlparse(path))

        parts[0] = 's3'
        parts[1] = self.bucket_name

        return urlparse.urlunparse(parts)





    @property
    def cache_dir(self):
        return None
  
    @property
    def size(self):
        '''Return the size of all of the files referenced in the database'''
        raise NotImplementedError() 
     
    def add_record(self, rel_path, size):
        raise NotImplementedError() 

    def verify(self):
        raise NotImplementedError() 
        
    @property
    def repo_id(self):
        '''Return the ID for this repository'''

        return "s3:{}/{}".format(self.bucket_name, self.prefix)

 
    def get_stream(self, rel_path, cb=None, return_meta=False):
        """Return the object as a stream"""
        from boto.s3.key import Key
        from boto.exception import S3ResponseError 
     
        import StringIO
        from ..util.flo import MetadataFlo

        b = StringIO.StringIO()
        try:
            k = self._get_boto_key(rel_path)
            if not k:
                return None
        
            k.get_contents_to_file(b, cb=cb, num_cb=100)
           
            b.seek(0)
            
            if return_meta:
                d = k.metadata
                d['size'] = k.size
                d['etag'] = k.etag  
            else:
                d = {}
                
            return MetadataFlo(b,d)
            
        except S3ResponseError as e:
            if e.status == 404:
                return None
            else:
                raise e
        
    def get(self, rel_path, cb=None):
        '''For S3, get requires an upstream, where the downloaded file can be stored
        '''
        
        raise NotImplemented("Can't get() from an S3, since it has no place to put a file. Wrap with an FsCache. ")
    
    def _get_boto_key(self, rel_path, validate = True):
        from boto.s3.key import Key

        rel_path = self._rename(rel_path)
        path = self._prefix(rel_path)

        k = self.bucket.get_key(path, validate = validate)
        
        return k        
  
    def put(self, source, rel_path,  metadata=None):  

        sink = self.put_stream(rel_path, metadata = metadata)
        
        copy_file_or_flo(source, sink)
        
        sink.close()

        return rel_path
    
    def put_key(self, source, rel_path):
        '''Copy a file to the repository
        
        Args:
            source: Absolute path to the source file, or a file-like object
            rel_path: path relative to the root of the repository
        
        '''
        k = self._get_boto_key(rel_path)

        try:
            k.set_contents_from_file(source)
        except AttributeError:
            if os.path.getsize(source) > 4.8*1024*1024*1024:
                # Need to do multi-part uploads here
                k.set_contents_from_filename(source)
            else:
                k.set_contents_from_filename(source)
      
    def put_stream(self, rel_path,  metadata=None, cb=None):
        '''Return a Flo object that can be written to to send data to S3.
        This will result in a multi-part upload, possibly with each part
        being sent in its own thread '''

        import Queue
        import time
        import threading

        md5 = metadata.get('md5',None) if metadata else None

        # Horrible, but doing it anyway because I can.
        acl = ('public-read' if metadata.get('public', False) else metadata.get('acl', 'public-read')) if metadata else 'public-read'

        path = self._prefix(self._rename(rel_path))

        class ThreadUploader(threading.Thread):
            """Thread class for uploading a part to S3"""
            def __init__(self, n, queue):
                threading.Thread.__init__(self)
                self.n = n
                self.queue = queue
            
            def run(self):
              
                while True:
                    mp, part_number, buf = self.queue.get()
                    if mp is None: # Signal to die
                        global_logger.debug("put_stream: Thread {} exiting".format(self.n))
                        self.queue.task_done()
                        return
                    global_logger.debug("put_stream: Thread {}: processing part: {}".format(self.n, part_number))
                    t1 = time.time()
                    try:
                        mp.upload_part_from_file(buf,part_number  )
                    finally:
                        self.queue.task_done()
                        t2 = time.time()
                        global_logger.debug("put_stream: Thread {}, part {}. time = {} rate =  {} b/s"
                                     .format(self.n, part_number, round(t2-t1,3), round((float(buf.tell())/(t2-t1)), 2)))
                    
                        

        if metadata is None:
            metadata = {}
        
        if md5:
            metadata['md5'] = md5 # Multipart uploads don't use md5 for etag

        # Some libraries, including apparently, boto, have troubles properly calcing the hash when
        # metadata values are null, empty, 0, etc.  https://forums.aws.amazon.com/thread.jspa?threadID=117580
        for k,v in metadata.items():
            if not v:
                del metadata[k]

        this = self
        
        buffer_size = 50*1024*1024 # Min part size is 5MB
        num_threads = 4
        thread_upload_queue = Queue.Queue(maxsize=100)

        for i in range(num_threads):
            t = ThreadUploader(i, thread_upload_queue)
            t.setDaemon(True)
            t.start()

        class flo:
            '''Object that is returned to the caller, for the caller to issue
            write() or writeline() calls on '''
       
            def __init__(self, rel_path):
                import io

                self.mp = this.bucket.initiate_multipart_upload(path, metadata=metadata)
                self.part_number = 1
                self.buffer = io.BytesIO()
                self.total_size = 0

                self.rel_path = rel_path

     
            def _send_buffer(self):
                '''Schedules a buffer to be sent in a thread by queuing it'''
                global_logger.debug("_send_buffer: sending part {} to thread pool size: {}, total_size = {}"
                             .format(self.part_number, self.buffer.tell(), self.total_size))
                self.buffer.seek(0)
                thread_upload_queue.put( (self.mp, self.part_number, self.buffer) )


            def write(self, d):
                import io


                self.buffer.write(d) # Load the requested data into a buffer
                self.total_size += len(d)
                # After the buffer is large enough, send it, then create a new buffer. 
                if self.buffer.tell() > buffer_size:
                    self._send_buffer() 
                    
                    self.part_number += 1
                    self.buffer = io.BytesIO()

            def writelines(self, lines):
                raise NotImplemented()
            
            def close(self):
               
                if self.buffer.tell() > 0:
                    self._send_buffer()

                thread_upload_queue.join()  # Wait for all of th upload to complete
         
                for i in range(num_threads):
                    thread_upload_queue.put( (None,None,None) ) # Tell all of the threads to die
 
                thread_upload_queue.join()  # Wait for all of the threads to exit

                # Multi-part uploads throw a 400 Bad Request if they are completed without writing any data.
                if self.total_size > 0:
                    self.mp.complete_upload()

                    this.bucket.set_acl(acl, path)

            def __enter__(self):
                return self

            def __exit__(self, type_, value, traceback):
                if type_:
                    return False

                self.close()

        return flo(rel_path)
     
            
    def find(self,query):
        '''Passes the query to the upstream, if it exists'''

        raise NotImplemented()
    
    def remove(self,rel_path, propagate = False):
        '''Delete the file from the cache, and from the upstream'''

        key = self._get_boto_key(rel_path)
        if key:
            key.delete()   

    def clean(self):
        '''Delete Everything'''

        for e in self.bucket.list(self.prefix):
            path = e.name.replace(self.prefix,'',1).strip('/')
            if path.startswith('_'):
                continue

            self.remove(path)

        
    def list(self, path=None,with_metadata=False, include_partitions=False):
        '''Get a list of all of bundle files in the cache. Does not return partition files'''
        import json
        
        sub_path = self.prefix+'/'+path.strip('/') if path else self.prefix

        l = {}

        for e in self.bucket.list(sub_path):

            path = e.name.replace(self.prefix,'',1).strip('/')
            if path.startswith('_') or path.startswith('meta'):
                continue

            if not include_partitions and path.count('/') > 1:
                continue # partition files

            if with_metadata:
                d = self.metadata(path)
                if d and 'identity' in d:
                    d['identity'] = json.loads(d['identity'])
            else:
                d = {}

            d['caches'] = [self.repo_id]

            if path:
                l[path] = d


        return l




    def has(self, rel_path, md5=None, propagate=True):

        k = self._get_boto_key(rel_path)
        
        if not k:
            return False
        
        if not md5:
            
            return  k.exists()
        else:
            remote_md5 = k.get_metadata('md5')
            
            r =  k.exists() and  str(md5) == str(remote_md5) 
            #print "=== ",r

            return r
    
    def metadata(self, rel_path):

        k = self._get_boto_key(rel_path)
        
        if not k or not k.exists():
            return None
        
        d = k.metadata
        d['size'] = k.size
        d['etag'] = k.etag       


        return d


    
    def __repr__(self):
        return "S3Cache: bucket={} prefix={} access=...{} ".format(self.bucket, self.prefix, self.access_key[-5:], self.upstream)
       
