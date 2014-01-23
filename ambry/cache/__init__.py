

import os
from ambry.dbexceptions import ConfigurationError

def new_cache(config):
        """Return a new :class:`FsCache` built on the configured cache directory
        """   

        if 'size' in config:
            from filesystem import FsLimitedCache
            fsclass = FsLimitedCache
        elif 'url' in config:
            from remote import RestReadCache
            fsclass = RestReadCache
        elif 'account' in config:
            
            if config['account']['service'] == 's3':
                from s3 import S3Cache
                fsclass = S3Cache
            elif config['account']['service'] == 'gcs':
                from gcs import GcsCache
                fsclass = GcsCache
            else:
                raise ConfigurationError("Unknown account service: {}".format(config['account']['service']))
                                                                 
        elif 'dir' in config:
            from filesystem import FsCache
            fsclass = FsCache
        else:
            
            raise ConfigurationError("Can't determine cache type: {} ".format(config))

        if 'options' in config and 'compress' in config['options'] :
            config['options'] = [ i for i in config['options'] if i !=  'compress']
            from filesystem import FsCompressionCache
            return FsCompressionCache(upstream=config)
        else:
            return  fsclass(**dict(config))
            
       

class CacheInterface(object):

    config = None
    upstream = None

    def path(self, rel_path, **kwargs): raise NotImplementedError()

    def get(self, rel_path, cb=None): raise NotImplementedError()
    
    def get_stream(self, rel_path, cb=None):  raise NotImplementedError(type(self))
    
    def last_upstream(self):  raise NotImplementedError(type(self))
    
    def has(self, rel_path, md5=None, use_upstream=True):  raise NotImplementedError()
    
    def put(self, source, rel_path, metadata=None): raise NotImplementedError()
    
    def put_stream(self,rel_path, metadata=None): raise NotImplementedError()
    
    def remove(self,rel_path, propagate = False): raise NotImplementedError()
    
    def find(self,query): raise NotImplementedError()
    
    def list(self, path=None, with_metadata=False): raise NotImplementedError()
   
    def get_upstream(self, type_):
        '''Return self, or an upstream, that has the given class type.
        This is typically used to find upstream s that impoement the RemoteInterface
        ''' 

        if isinstance(self, type_):
            return self
        elif self.upstream and isinstance(self.upstream, type_):
            return self.upstream
        elif self.upstream:
            return self.upstream.get_upstream(type_)
        else:
            return None


class RemoteMarker(object):
    pass
   
class RemoteInterface(CacheInterface, RemoteMarker):
    
    bucket_name = None #@UnusedVariable
    prefix = None #@UnusedVariable
    access_key = None #@UnusedVariable
    secret_key = None #@UnusedVariable
    
    @property
    def connection_info(self):  raise NotImplementedError()
    
    def find(self, query): raise NotImplementedError()
    
    def get_ref(self, id_): raise NotImplementedError()
    
    def last_upstream(self):
        us = self
        
        while us.upstream:   
            us = us.upstream
            
        return us    
       
class Cache(CacheInterface):
    
    upstream = None
    readonly = False
    usreadonly = False   
    
    def __init__(self,  upstream=None,**kwargs):   
        self.upstream = upstream
   
        self.args = kwargs
   
        self.readonly = False
        self.usreadonly = False   
    
        if upstream:
            if isinstance(upstream, Cache):
                self.upstream = upstream
            else:
                self.upstream = new_cache(upstream)

    def last_upstream(self):
        us = self
        
        while us.upstream:   
            us = us.upstream
            
        return us

    @property
    def repo_id(self):
        raise NotImplementedError()
    
    
    def path(self, rel_path, **kwargs):
        if self.upstream:
            return self.upstream.path(rel_path, **kwargs)
        
        return None
    
    def get(self, rel_path, cb=None):
        if self.upstream:
            return self.upstream.get(rel_path, cb)
        
        return None

    def get_stream(self, rel_path, cb=None):

        if self.upstream:
            return self.upstream.get_stream(rel_path, cb)
        
        return None

    def has(self, rel_path, md5=None, use_upstream=True):
        if self.upstream:
            return self.upstream.has(rel_path, md5=md5, use_upstream=use_upstream)
        
        return None

    def put(self, source, rel_path, metadata=None):
        if self.upstream:
            return self.upstream.put(self, source, rel_path, metadata=metadata)
        
        return None

    def put_stream(self,rel_path, metadata=None):
        if self.upstream:
            return self.upstream.put_stream(self,rel_path, metadata=metadata)
        
        return None

    def put_metadata(self,rel_path, metadata):
        import json
        
        if rel_path.startswith('meta'):
            return
        
        if metadata:
            strm = self.put_stream(os.path.join('meta',rel_path))
            json.dump(metadata, strm)
            strm.close()
    
    def metadata(self,rel_path):
        import json

        if rel_path.startswith('meta'):
            return None

        strm = self.get_stream(os.path.join('meta',rel_path))
        
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
        
    def remove(self,rel_path, propagate = False):
        if self.upstream:
            return self.upstream.remove(self,rel_path, propagate = propagate)
        
        return None

    def find(self,query):

        if self.upstream:
            return self.upstream.find(query)    
        return None

    def clean(self):

        if self.upstream:
            return self.upstream.clean()
        
        return None
        
        
    def list(self, path=None,with_metadata=False):
        if self.upstream:
            return self.upstream.list(path, with_metadata=with_metadata)
        
        return None


    def __repr__(self):
        return "{}".format(type(self))


