


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


def remote_rebuild(self):
    '''Rebuild the library from the contents of the remote'''

    self.logger.info("Rebuild library from: {}".format(self.upstream))

    #self.database.drop()
    #self.database.create()

    # This should almost always be an object store, like S3. Well, it better be,
    # inst that is the only cache that has the include_partitions parameter.
    rlu = self.upstream.last_upstream()

    remote_partitions = rlu.list(include_partitions=True)

    for rel_path in self.upstream.list():


        path = self.load(rel_path)

        if not path or not os.path.exists(path):
            self.logger.error("ERROR: Failed to get load for relpath: '{}' ( '{}' )".format(rel_path, path))
            continue

        bundle = DbBundle(path)
        identity = bundle.identity

        self.database.add_file(path, self.cache.repo_id, identity.vid, 'pulled')
        self.logger.info('Installing: {} '.format(bundle.identity.name))
        try:
            self.database.install_bundle_file(identity, path)
        except Exception as e:
            self.logger.error("Failed: {}".format(e))
            continue

        for p in bundle.partitions:

            # This is the slow way to do it:
            # if self.remote.last_upstream().has(p.identity.cache_key):
            if p.identity.cache_key in remote_partitions:
                self.database.add_remote_file(p.identity)
                self.logger.info('            {} '.format(p.identity.name))
            else:
                self.logger.info('            {} Ignored; not in remote'.format(p.identity.name))


