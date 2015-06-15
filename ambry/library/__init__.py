"""A Library is a local collection of bundles. It holds a database for the configuration
of the bundles that have been installed into it.
"""

# Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt


def new_library(config=None):

    from ckcache import new_cache
    from ..orm import Database
    from .filesystem import LibraryFilesystem
    from boto.exception import S3ResponseError  # the ckcache lib should return its own exception

    if config is None:
        from ..run import get_runconfig
        config = get_runconfig()

    remotes = {}

    for name, remote in config.library().get('remotes', {}).items():

        try:
            remotes[name] = new_cache(remote)
        except S3ResponseError as e:
            from ..util import get_logger

            logger = get_logger(__name__)
            logger.error("Failed to init cache {} : {}; {} ".format(name, str(remote.bucket), e))

    # A bit random. There just should be some priority
    for i, remote in enumerate(remotes.values()):
        remote.set_priority(i)

    lfs = LibraryFilesystem(config)
    db = Database(config.library()['database'])
    warehouse = None

    l = Library(database=db,
                filesystem = lfs,
                warehouse = warehouse,
                remotes=remotes)

    return l


class Library(object):

    def __init__(self,database,filesystem, warehouse, remotes ):
        from ..util import get_logger

        self._db = database
        self._db.open()
        self._fs = filesystem
        self._warehouse = warehouse
        self._remotes = remotes

    def commit(self):
        self._db.commit()

    def new_bundle(self,**kwargs):
        """Create a new bundle, with the same arguments as creating a new dataset"""
        from ..bundle import Bundle

        ds = self._db.new_dataset(**kwargs)
        self._db.commit()

        return self.bundle(ds.vid)

    def bundle(self, ref):
        """Return a bundle build on a dataset, with the given vid or id reference"""

        from ..bundle import Bundle
        from fs.opener import fsopendir

        ds = self._db.dataset(ref)

        source_dir = ds.config.library.source_dir.dir

        if not source_dir:
            source_dir = self._fs.source(ds.name)
            ds.config.library.source_dir.dir = source_dir
            self.commit()

        return Bundle(ds, self, source_fs=fsopendir(source_dir))

    ##
    ## Storing
    ##

    def put_bundle(self, bundle, source=None, install_partitions=True, file_state='installed', commit=True,
                   force=False):
        """Install the records for the dataset, tables, columns and possibly
        partitions. Does not install file references """

        if not force and self.files.query.ref(bundle.identity.vid).type(Files.TYPE.BUNDLE).one_maybe:
            return self.cache.path(bundle.identity.cache_key), False

        self.database.install_bundle(bundle)

        if source is None:
            source = self.cache.repo_id

        installed = self.files.install_bundle_file(bundle, source, commit=commit, state=file_state)

        if install_partitions:
            for p in bundle.partitions.all:
                self.put_partition(p, source, commit=commit, file_state=file_state)

        # Copy the file in if we don't have it already
        if not self.cache.has(bundle.identity.cache_key):
            self.cache.put(bundle.database.path, bundle.identity.cache_key)

        if self._doc_cache:
            self.search.index_dataset(bundle, force=True)

            for partition in bundle.partitions:
                self.search.index_partition(partition, force=True)

            self.search.commit()

        self.mark_updated(vid=bundle.identity.vid)
        self.mark_updated(key="bundle_index")
        self.mark_updated(key="library_info")

        return self.cache.path(bundle.identity.cache_key), installed

    def put_partition(self, partition, source=None, file_state='installed', commit=True):
        """Install the record and file reference for the partition """

        if source is None:
            source = self.cache.repo_id

        installed = self.files.install_partition_file(partition, source, commit=commit, state=file_state)

        # Ref partitions use the file of an earlier version, so there is no file to install
        if not self.cache.has(partition.identity.cache_key) and os.path.exists(partition.database.path):
            self.cache.put(partition.database.path, partition.identity.cache_key)

        return self.cache.path(partition.identity.cache_key), installed

    def remove(self, bundle):
        '''Remove a bundle from the library, and delete the configuration for
        it from the library database'''

        self.database.remove_bundle(bundle)

        self.mark_updated(vid=bundle.identity.vid)

        self.cache.remove(bundle.identity.cache_key, propagate=True)

