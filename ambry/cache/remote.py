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
