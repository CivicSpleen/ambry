from . import CacheInterface, RemoteInterface, RemoteMarker, new_cache
from ..client.rest import RemoteLibrary

# Setup a default logger. The logger is re-assigned by the
# bundle when the bundle instantiates the logger. 
import logging #@UnusedImport
import logging.handlers #@UnusedImport
from ..util import  get_logger

logger = get_logger(__name__)
#logger.setLevel(logging.DEBUG) 

class RestReadCache(RemoteInterface):
    '''A cache that looks up the cache key with the remote API and returns
    a URL to download '''

    def __init__(self,  url, **kwargs):
        self.url = url

        self.rl = RemoteLibrary(self.url)

    def path(self, rel_path, **kwargs): 

        info = self.rl.info(rel_path)

        if not info:
            return None

        return info['urls']['db']

    def get(self, did, pid=None):
        raise NotImplementedError("Use get_stream instead")

    def get_stream(self, rel_path, cb=None):

        return self.rl.get_stream(rel_path)

    def has(self, rel_path, md5=None, use_upstream=True):
        return bool(self.path(rel_path))


    def remove(self,rel_path, propagate = False): 
        return False

    
    def metadata(self,rel_path):
        raise  NotImplementedError()
    
    def find(self,query):
        raise NotImplementedError()
    
    def list(self, path=None,with_metadata=False):
        return  self.rl.list()
        
        
    def get_upstream(self, type_):
        ''''''
         
        return None
       
    def __repr__(self):
        return "RestReadCache: url={}".format(self.url)
       
class Save(object):


    def put_bundle(self, bundle):

        metadata = bundle.identity.to_meta(file=bundle.database.path )

        return self.put(bundle.database.path,
                         bundle.identity.cache_key,
                         metadata = metadata)

    def put_partition(self, partition):

        metadata = partition.identity.to_meta(file=partition.database.path )

        return self.put(partition.database.path,
                         partition.identity.cache_key,
                         metadata = metadata)



    def put(self, source, rel_path, metadata=None):

        from ambry.bundle import DbBundle
        from ambry.util import md5_for_file
        import json
        from ..dbexceptions import ConfigurationError

        if not self.upstream:
            raise ConfigurationError("Can't put to a RestRemote without an s3 upstream")

        if not metadata:
            raise ConfigurationError("Must have metadata")

        if set(['id','identity','name','md5','fqname']) != set(metadata.keys()):
            raise ConfigurationError("Metadata is missing keys: {}".format(metadata.keys()))

        # Store the bundle into the S3 cache.

        if not self.last_upstream().has(rel_path, md5=metadata['md5'], use_upstream=True):
            logger.debug("PUTTING %s to upstream, %s", str(rel_path),self.upstream)
            r =  self.upstream.put(source, rel_path, metadata=metadata)
        else:
            logger.debug("Upstream has path %s, returning path", str(rel_path))
            r = self.upstream.path(rel_path)


        self.api.put(metadata)

        return r

    def put_stream(self,rel_path, metadata=None):
        from io import IOBase
        import requests

        if set(['id','identity','name','md5']) not in set(metadata.keys()):
            raise ValueError("Must have complete metadata to use put_stream(): 'id','identity','name','md5' ")

        class flo(IOBase):

            def __init__(self, api, url, upstream,  rel_path):

                self._api = api
                self._url = url
                self._upstream = upstream
                self._rel_path = rel_path
                self._sink = self.upstream.put_stream(rel_path, metadata=metadata)

            def write(self, str_):
                self._sink.write(str_)

            def close(self):
                if not self._sink.closed:
                    self._sink.close()
                    self.api.put(self._url, metadata)


        return flo(self.api, self.url, self.upstream,  rel_path)




class RemoteUpstreamCache(CacheInterface):
    '''Cache that writes to one upstream on writes, and reads from another.

    On reads, if the read_upstream has the object, return it. If the write upstream has it, copy
    it to the read upstream and return it.

    On writes, write only to the write upstream.

    '''


    def __init__(self, upstream=None,  remote_upstream=None,**kwargs):
        '''
        '''

        raise NotImplementedError("Not finished")

        self.upstream = upstream
        self.remote_upstream = remote_upstream

    def path(self, rel_path, **kwargs):

        path = self.upstream.path(rel_path,**kwargs)

        if path:
            return path

        else:
            return self.remote_upstream.path(rel_path,**kwargs)

    def get(self, rel_path, cb=None):

        # Return from the local upstream, if it exists
        if self.upstream.has(rel_path):
            return self.upstream.get(rel_path)

        if not self.remote_upstream.has(rel_path):
            return False

        # Copy from the remote upstream to the local upstream.
        read_stream = self.remote_upstream.get_stream(rel_path, cb=cb)

        self.upstream.put(read_stream, metadata=read_stream.metadata)

        assert self.upstream.has(rel_path)

        return self.upstream.get(rel_path)


    def get_stream(self, rel_path, cb=None):

        # Return from the local upstream, if it exists
        if self.upstream.has(rel_path):
            return self.upstream.get_stream(rel_path, cb=cb)

        if not self.remote_upstream.has(rel_path):
            return False

        # Copy from the remote upstream to the local upstream.
        read_stream = self.remote_upstream.get_stream(rel_path, cb=cb)

        self.upstream.put(read_stream, metadata=read_stream.metadata)

        assert self.upstream.has(rel_path)

        return self.upstream.get_stream(rel_path, cb=cb)


    def has(self, rel_path, md5=None, use_upstream=True):

        if self.upstream.has(rel_path):
            return True
        elif self.remote_upstream.has(rel_path):
            return True
        else:
            return False


    def put(self, source, rel_path, metadata=None):
        '''Put directly to the remote'''



    def put_stream(self,rel_path, metadata=None): raise NotImplementedError()

    def remove(self,rel_path, propagate = False): raise NotImplementedError()

    def find(self,query): raise NotImplementedError()

    def list(self, path=None, with_metadata=False): raise NotImplementedError()

    def last_upstream(self):  raise NotImplementedError(type(self))

    def get_upstream(self, type_):
        '''Return self, or an upstream, that has the given class type.
        This is typically used to find upstream s that implement the RemoteInterface
        '''

        if isinstance(self, type_):
            return self
        elif self.upstream and isinstance(self.upstream, type_):
            return self.upstream
        elif self.remote_upstream and isinstance(self.remote_upstream, type_):
            return self.upstream
        elif self.upstream:
            return self.upstream.get_upstream(type_)
        else:
            return None
