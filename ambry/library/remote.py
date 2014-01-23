


class RemoteLibrary(object):

    def __init__(self, cache):
        pass

    def get(self,bp_id, force = False, cb=None):
        pass


    @property
    def cache(self):
        '''Return the remote cache, usually s3, where files can be uploaded'''


    def find(self, query_command):
        from ..cache import RemoteMarker
        import socket

        try:
            api = self.remote.get_upstream(RemoteMarker).api
        except AttributeError: # No api
            try:
                api = self.remote.api
            except AttributeError: # No api
                return False

        try:
            r = api.find(query_command)
        except socket.error:
            self.logger.error("Connection to remote failed")
            return False

        if not r:
            return False

        return r


    def remote_list(self, datasets=None, with_meta = True, key='vid'):

        from ..identity import LocationRef, Identity

        try:
            for k,v in self.remote.list(with_metadata=with_meta).items():

                if v and v['identity']['id'] != 'a0' and v['identity']['vid'] != Config.ROOT_CONFIG_NAME_V:
                    dsid = Identity.from_dict(v['identity'])
                    ck = getattr(dsid, key)
                    dsid.locations.set(LocationRef.LOCATION.REMOTE)
                    datasets[ck] = dsid
        except socket.error:
            pass


    def put(self, bundle, force=False):
        '''Install a bundle or partition file into the library.

        :param bundle: the file object to install
        :rtype: a `Partition`  or `Bundle` object

        '''
        from ..bundle import Bundle
        from ..partition import PartitionInterface

        if not isinstance(bundle, (PartitionInterface, Bundle)):
            raise ValueError("Can only install a Partition or Bundle object")


        bundle.identity.name # throw exception if not right type.

        dst, cache_key, url = self.put_file(bundle.identity, bundle.database.path, force=force)

        return dst, cache_key, url

    def remove(self, bundle):
        '''Remove a bundle from the library, and delete the configuration for
        it from the library database'''

        self.database.remove_bundle(bundle)

        self.cache.remove(bundle.identity.cache_key, propagate = True)

    def clean(self, add_config_root=True):
        self.database.clean(add_config_root=add_config_root)

    def purge(self):
        """Remove all records from the library database, then delete all
        files from the cache"""
        self.clean()
        self.cache.clean()


