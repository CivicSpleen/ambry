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
    from search import Search
    if config is None:
        from ..run import get_runconfig
        config = get_runconfig()

    remotes = {}

    for name, remote in config.library().get('remotes', {}).items():

        try:
            remotes[name] = new_cache(remote, config.filesystem('root'))
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
                remotes=remotes
                )

    return l


class Library(object):

    def __init__(self,database,filesystem, warehouse, remotes ):
        from ..util import get_logger

        self._db = database
        self._db.open()
        self._fs = filesystem
        self._warehouse = warehouse
        self._remotes = remotes
        self._search = None

        self.logger = get_logger(__name__)

    @property
    def database(self):
        return self._db

    @property
    def remotes(self):
        return self._remotes

    def remote(self,name):
        return self._remotes[name]

    def commit(self):
        self._db.commit()

    def datasets(self):
        """Return all datasets"""

        return self._db.datasets()


    def new_bundle(self,**kwargs):
        """Create a new bundle, with the same arguments as creating a new dataset"""
        from ..bundle import Bundle

        ds = self._db.new_dataset(**kwargs)
        self._db.commit()

        return self.bundle(ds.vid)

    def new_from_bundle_config(self, config):
        """
        Create a new bundle, or link to an existing one, based on the identity in config data.


        :param config: A Dict form of a bundle.yaml file
        :return:
        """

        from ..identity import Identity
        from ..bundle import Bundle

        identity = Identity.from_dict(config['identity'])

        ds  = self._db.dataset(identity.vid)

        if not ds:
            ds = self._db.dataset(identity.name)

        if not ds:
            ds = self._db.new_dataset(**identity.dict)

        return Bundle(ds, self)


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

    def partition(self, ref):
        from ambry.identity import ObjectNumber

        on = ObjectNumber.parse(ref)
        ds_on = on.as_dataset

        ds = self._db.dataset(ds_on) # Could do it in on SQL query, but this is easier.

        return ds.partition(ref)

    ##
    ## Storing
    ##

    def install_to_remote(self,b):
        import tempfile
        from ambry.orm.database import Database
        from ambry.util import copy_file_or_flo

        try:
            td = tempfile.mkdtemp()

            db = Database('sqlite:////{}/{}.db'.format(td, b.identity.vid))

            db.open()

            ds = db.copy_dataset(b.dataset)

            remote = self.remote(b.metadata.about.access)

            remote.put(db.path, b.identity.cache_key + ".db")

            for p in b.partitions:
                with remote.put_stream(p.datafile().munged_path) as f:
                    copy_file_or_flo(p.datafile().open('rb'), f)

        finally:
            from shutil import rmtree

            rmtree(td)

    def stream_partition(self, ref):
        """Yield rows of a partition"""
        from ambry.bundle.partitions import PartitionProxy

        p_orm = self.partition(ref)

        b = self.bundle(p_orm.d_vid)

        p = PartitionProxy(b, p_orm)

        remote = self.remote(p.dataset.config.metadata.about.access)

        with remote.get_stream(p.datafile().munged_path) as f:

            reader = p.datafile().reader(f)

            header = reader.next()

            for a,b in  zip(header, (c.name for c in p.table.columns)):
                if a != b:
                    raise Exception("Partition header {} is different from column name {}".format(a,b))

            for row in reader:
                yield row

    def remove(self, bundle):
        '''Remove a bundle from the library, and delete the configuration for
        it from the library database'''

        raise NotImplementedError

        self.database.remove_bundle(bundle)

        self.mark_updated(vid=bundle.identity.vid)

        self.cache.remove(bundle.identity.cache_key, propagate=True)

    @property
    def search(self):
        from search import Search
        if not self._search:
            self._search = Search(self)

        return self._search

