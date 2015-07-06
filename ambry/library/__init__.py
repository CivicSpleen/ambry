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

    l = Library(config=config,
                database=db,
                filesystem=lfs,
                warehouse=warehouse,
                remotes=remotes
                )

    return l


class Library(object):

    def __init__(self,config, database,filesystem, warehouse, remotes ):
        from ..util import get_logger

        self._config = config
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
    def download_cache(self):
        from fs.osfs import OSFS
        return OSFS(self._fs.downloads())

    @property
    def remotes(self):
        return self._remotes

    def remote(self,name_or_bundle):
        from ..dbexceptions import ConfigurationError

        fails = []

        try:
            return self._remotes[name_or_bundle]
        except KeyError:
            pass

        try:
            return self._remotes[name_or_bundle.metadata.about.access]
        except AttributeError:
            pass
        except KeyError:
            fails.append(name_or_bundle.metadata.about.access)

        try:
            return self._remotes[name_or_bundle.metadata.about.remote]
        except AttributeError:
            pass
        except KeyError:
            fails.append(name_or_bundle.metadata.about.access)

        raise ConfigurationError('Failed to find remote for key values: {}'.format(fails))



    def commit(self):
        self._db.commit()

    @property
    def root(self):
        """Return the root dataset"""
        return self._db.root_dataset

    @property
    def datasets(self):
        """Return all datasets"""
        return self._db.datasets()

    def new_bundle(self,assignment_class = None, **kwargs):
        """
        Create a new bundle, with the same arguments as creating a new dataset

        :param assignment_class: String. assignment class to use for fetching a number, if one
        is not specified in kwargs
        :param kwargs:
        :return:
        """

        from ..bundle import Bundle
        from time import time

        if not ('id' in kwargs and bool(kwargs['id'])) or assignment_class is not None:
            kwargs['id'] = self.number(assignment_class)

        ds = self._db.new_dataset(**kwargs)
        self._db.commit()

        b =  self.bundle(ds.vid)
        b.state = Bundle.STATES.NEW

        b.set_last_access(Bundle.STATES.NEW)

        b.set_file_system(source_url=self._fs.source(ds.name),
                          build_url=self._fs.build(ds.name))

        self._db.commit()
        return b

    def new_from_bundle_config(self, config):
        """
        Create a new bundle, or link to an existing one, based on the identity in config data.

        :param config: A Dict form of a bundle.yaml file
        :return:
        """

        from ..identity import Identity
        from ..bundle import Bundle

        identity = Identity.from_dict(config['identity'])

        ds = self._db.dataset(identity.vid)

        if not ds:
            ds = self._db.dataset(identity.name)

        if not ds:
            ds = self._db.new_dataset(**identity.dict)

        b =  Bundle(ds, self)

        b.state = Bundle.STATES.NEW
        b.set_last_access(Bundle.STATES.NEW)
        b.set_file_system(source_url=self._fs.source(ds.name),
                          build_url = self._fs.build(ds.name) )

        return b


    def bundle(self, ref):
        """Return a bundle build on a dataset, with the given vid or id reference"""

        from ..bundle import Bundle
        from fs.opener import fsopendir
        from ..orm.dataset import Dataset
        from ..orm.exc import NotFoundError

        if isinstance(ref, Dataset ):
            ds = ref
        else:
            ds = self._db.dataset(ref)

        if not ds:
            raise NotFoundError("Failed to find dataset for ref: {}".format(ref))

        return Bundle(ds, self)

    @property
    def bundles(self):
        """Return all datasets"""

        for ds in self.datasets:
            yield self.bundle(ds)


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

            remote = self.remote(b)

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

        remote = self.remote(b)

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

        bundle.remove()
        self.database.remove_dataset(bundle.dataset)


    def number(self, assignment_class=None, namespace = 'd'):
        """
        Return a new number.

        :param assignment_class: Determines the length of the number. Possible values are 'authority' (3 characters) ,
            'registered' (5) , 'unregistered' (7)  and 'self' (9). Self assigned numbers are random and acquired locally,
            while the other assignment classes use the number server defined in the configuration. If None,
            then look in the number server configuration for one of the class keys, starting
            with the longest class and working to the shortest.
        :param namespace: The namespace character, the first character in the number. Can be one of 'd', 'x' or 'b'
        :return:
        """
        from requests.exceptions import HTTPError
        from ..identity import NumberServer, DatasetNumber
        from ..dbexceptions import ConfigurationError

        if assignment_class=='self':
            # When 'self' is explicit, don't look for number server config
            return str(DatasetNumber())

        elif assignment_class is None:

            try:
                nsconfig = self._config.service('numbers')

            except ConfigurationError:
                # A missing configuration is equivalent to 'self'
                self.logger.error("No number server configuration; returning self assigned number")
                return str(DatasetNumber())

            for assignment_class in ('self', 'unregistered', 'registered', 'authority'):
                if assignment_class+'-key' in nsconfig:
                    break

            # For the case where the number configuratoin references a self-assigned key
            if assignment_class == 'self':
                return str(DatasetNumber())

        else:
            try:
                nsconfig = self._config.service('numbers')

            except ConfigurationError:
                raise ConfigurationError("No number server configuration")

            if assignment_class + '-key' not in nsconfig:
                raise ConfigurationError('Assignment class {} not number server config'.format(assignment_class))

        try:

            key = nsconfig[assignment_class + '-key']
            config = {
                'key': key,
                'host': nsconfig['host'],
                'port': nsconfig.get('port',80)
            }

            ns = NumberServer(**config)

            n  = str(ns.next())
            self.logger.info("Got number from number server: {}".format(n))

        except HTTPError as e:
            self.logger.error("Failed to get number from number server for key: {}".format(key, e.message))
            self.logger.error("Using self-generated number. "
                 "There is no problem with this, but they are longer than centrally generated numbers.")
            n = str(DatasetNumber())

        return n

    def edit_history(self):
        """Return config record information about the most recent bundle accesses and operations"""
        from ..orm import Config

        return (self._db.session.query(Config)
                .filter(Config.type =='buildstate').filter(Config.group =='access').filter(Config.key =='last')
                .order_by(Config.modified.desc())).all()




    @property
    def search(self):
        from search import Search
        if not self._search:
            self._search = Search(self)

        return self._search

