from . import  RemoteInterface, Cache, new_cache
from ..client.rest import RemoteLibrary, RestApi

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

    base_priority = 50  # Priority for this class of cache.

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

    def has(self, rel_path, md5=None, propagate=True):
        return bool(self.path(rel_path))


    def remove(self,rel_path, propagate = False): 
        return False

    
    def metadata(self,rel_path):
        raise  NotImplementedError()
    
    def find(self,query):
        raise NotImplementedError()
    
    def list(self, path=None, with_metadata=False):
        l =   self.rl.list()

        print l

        return l
        
        
    def get_upstream(self, type_):
        ''''''
         
        return None
       
    def __repr__(self):
        return "RestReadCache: url={}".format(self.url)

    @property
    def repo_id(self):
        return self.url

    def set_priority(self, i):
        self._priority = self.base_priority + i

    @property
    def priority(self):
        return self._priority


class HttpCache( Cache):


    def __init__(self,  url, **kwargs):


        self.api =  RestApi(url)

    def _rename(self, rel_path):
        '''Remove the .gz suffix that may have been added by a compression cache.
        In s3, compression is indicated by the content encoding.  '''
        import re
        rel_path =  re.sub('\.gz$','',rel_path)
        return rel_path

    @property
    def repo_id(self):
        return self.api.url('')

    def path(self, rel_path, **kwargs):

        return self.url(self._rename(rel_path))

    def get(self, rel_path, cb=None):
        raise NotImplementedError()


    def get_stream(self, rel_path, cb=None):

        stream =  self.api.get_stream(self._rename(rel_path))


        if not stream.meta:
            stream.meta = self.metadata(rel_path)

        return stream


    def has(self, rel_path, md5=None, propagate=True):
        from ..client.exceptions import NotFound

        try:
            self.api.head(self._rename(rel_path))
            return True
        except NotFound:
            return False


    def put(self, source, rel_path, metadata=None): raise NotImplementedError()

    def put_stream(self,rel_path, metadata=None): raise NotImplementedError()

    def put_metadata(self,rel_path, metadata): raise NotImplementedError()

    def metadata(self,rel_path):
        from ..client.exceptions import NotFound
        rel_path = self._rename(rel_path)

        import json, os

        if rel_path.startswith('meta'):
            return {}

        try:
            strm = self.get_stream(os.path.join('meta',rel_path))
        except NotFound:
            return {}

        if strm:
            try:
                s = strm.read()
                if not s:
                    return {}
                return json.loads(s)
            except ValueError as e:
                raise ValueError("Failed to decode json for key '{}',  {}. {}".format(rel_path, self.path(os.path.join('meta',rel_path)), strm))
        else:
            return {}

    def remove(self,rel_path, propagate = False): raise NotImplementedError()

    def find(self,query): raise NotImplementedError()

    def clean(self): raise NotImplementedError()

    def list(self, path=None,with_metadata=True, include_partitions=True):

        l = self.api.get('meta/_list.json')

        if not l:
            from ..client.exceptions import NotFound
            raise NotFound("Did not find _list.json file at {}".format(self.path('_list.json')))

        return l.json()



    def attach(self,upstream): raise NotImplementedError()
    def detach(self): raise NotImplementedError()

    def set_priority(self, i):
        self._priority = self.base_priority + i

    @property
    def priority(self):
        return self._priority


    def last_upstream(self): raise NotImplementedError()

    def __repr__(self):
        return "HttpCache: url={}".format(self.api.url(''))


