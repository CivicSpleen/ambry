"""A Library is a local collection of bundles. It holds a database for the configuration
of the bundles that have been installed into it.
"""

# Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt
import logging

from ambry.util import get_logger

logger = get_logger(__name__, level=logging.INFO, propagate=False)


def new_library(config=None):


    from ..orm import Database
    from .filesystem import LibraryFilesystem
    from boto.exception import S3ResponseError  # the ckcache lib should return its own exception

    if config is None:
        from ..run import get_runconfig
        config = get_runconfig()

    lfs = LibraryFilesystem(config)
    db = Database(config.library()['database'])
    warehouse = None

    l = Library(config=config,
                database=db,
                filesystem=lfs,
                warehouse=warehouse)

    return l


class Library(object):

    def __init__(self,config, database,filesystem, warehouse ):
        from ..util import get_logger

        self._config = config
        self._db = database
        self._db.open()
        self._fs = filesystem
        self._warehouse = warehouse

        self._search = None

        self.logger = get_logger(__name__)

    def resolve_object_number(self,ref):
        """Resolve a variety of object numebrs to a dataset number"""
        from ..identity import ObjectNumber

        on = ObjectNumber.parse(ref)
        ds_on = on.as_dataset

        return ds_on

    @property
    def database(self):
        return self._db

    @property
    def filesystem(self):
        return self._fs

    @property
    def config(self):
        return self._config

    @property
    def download_cache(self):
        from fs.osfs import OSFS
        return OSFS(self._fs.downloads())

    @property
    def remotes(self):
        return self.filesystem.remotes

    def remote(self, name_or_bundle):
        return self.filesystem.remote(self.resolve_remote(name_or_bundle))

    def resolve_remote(self, name_or_bundle):
        from ..dbexceptions import ConfigurationError

        fails = []

        remote_names = self.filesystem.remotes.keys()

        try:
            if name_or_bundle in remote_names:
                return name_or_bundle
        except KeyError:
            fails.append(name_or_bundle)

        try:
            if name_or_bundle.metadata.about.remote in remote_names:
                return name_or_bundle.metadata.about.remote
        except AttributeError:
            pass
        except KeyError:
            fails.append(name_or_bundle.metadata.about.access)

        try:
            if name_or_bundle.metadata.about.access in remote_names:
                return name_or_bundle.metadata.about.access
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
        return self._db.datasets

    def dataset(self, ref, load_all = False):
        """Return all datasets"""
        return self.database.dataset(ref, load_all = load_all)

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
        from ..orm.dataset import Dataset
        from ..orm.exc import NotFoundError

        if isinstance(ref, Dataset ):
            ds = ref
        else:
            ref = self.resolve_object_number(ref)
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
        from ambry.orm import Partition
        from ambry.orm.exc import NotFoundError
        from ambry.identity import ObjectNumber, NotObjectNumberError
        from sqlalchemy import or_

        try:
            on = ObjectNumber.parse(ref)
            ds_on = on.as_dataset

            ds = self._db.dataset(ds_on) # Could do it in on SQL query, but this is easier.

            p =  ds.partition(ref)

        except NotObjectNumberError:
            q = (self.database.session.query(Partition)
                 .filter(or_(Partition.name == str(ref), Partition.vname == str(ref)))
                 .order_by(Partition.vid.desc()))

            p = q.first()

        if not p:
            raise NotFoundError("No partition for ref: '{}'".format(ref))


        b = self.bundle(p.d_vid)
        return b.wrap_partition(p)

    ##
    ## Storing
    ##

    def create_bundle_file(self, b):
        import os
        from ambry.orm.database import Database
        import tempfile
        import os

        fh, path = tempfile.mkstemp()
        os.fdopen(fh).close()

        db = Database('sqlite:///{}.db'.format(path))
        db.open()

        b.commit()
        ds = db.copy_dataset(b.dataset)

        ds.commit()

        db.close()

        return db.path

    def duplicate(self, b):
        """Duplicate a bundle, with a higher version number"""
        from ..bundle import Bundle
        from ..orm.exc import NotFoundError, ConflictError
        from sqlalchemy.orm import object_session, make_transient, lazyload
        from ..orm import File

        on = b.identity.on
        on.revision = on.revision + 1

        try:
            extant = self.bundle(str(on))

            if extant:
                raise ConflictError("Already have a bundle with vid: {}".format(str(on)))
        except NotFoundError:
            pass

        d = b.dataset.dict
        d['revision'] = on.revision
        d['vid'] = str(on)
        del d['name']
        del d['vname']
        del d['version']
        del d['fqname']
        del d['cache_key']

        ds = self.database.new_dataset(**d)

        nb = self.bundle(ds.vid)
        nb.state = Bundle.STATES.NEW

        nb.set_last_access(Bundle.STATES.NEW)

        nb.set_file_system(source_url=self._fs.source(ds.name),
                          build_url=self._fs.build(ds.name))

        session = object_session(nb.dataset)

        for f in session.query(File).filter(File.d_vid == ds.vid).options(lazyload('*')).all():

            d =  f.row

            del d['id']
            del d['d_vid']
            del d['dataset']

            nf = File(**d)

            ds.files.append(nf)

        ds.commit()

        return nb

    def checkin(self,b):
        """
        Copy a bundle to a new Sqlite file, then store the file on the remote.

        :param b: The bundle
        :return:
        """

        from ambry.bundle import Bundle
        import os
        from ambry.orm.database import Database
        remote_name = self.resolve_remote(b)

        remote = self.remote(remote_name)

        db_path = self.create_bundle_file(b)

        db = Database('sqlite:///{}'.format(db_path))
        ds = db.dataset(b.dataset.vid)

        self.logger.info('Checking in bundle {}'.format(ds.identity.vname))

        # Set the location for the bundle file
        for p in ds.partitions:
            p.location = 'remote'

        ds.config.build.state.current = Bundle.STATES.INSTALLED
        ds.commit()
        db.commit()
        db.close()

        db_ck = b.identity.cache_key + ".db"

        with open(db_path) as f:
            remote.setcontents(db_ck, f)

        os.remove(db_path)

        def prt(a,b):
            print a, b

        for p in b.partitions:
            # Turn off the compression, which really turns off decompression on read. This is important because
            # we want to copy the compressed data to the remote.
            with p.datafile.open('rb', compress = False) as fin:
                self.logger.info('Checking in {}'.format(p.identity.vname))
                remote.setcontents(p.datafile.munged_path, fin)

        b.dataset.commit()

        return remote_name, db_ck

    def sync_remote(self, remote_name):
        from fs.opener import fsopendir
        import os
        from ambry.orm.database import Database
        from ambry.bundle import Bundle

        remote = self.remote(remote_name)

        temp = fsopendir("temp://ambry-import", create_dir = True)
        #temp = fsopendir("/tmp/ambry-import", create_dir=True)

        for fn in remote.walkfiles(wildcard='*.db'):
            temp.makedir(os.path.dirname(fn), recursive = True, allow_recreate=True)
            with remote.open(fn, 'rb') as f:
                temp.setcontents(fn, f)

            try:
                db = Database('sqlite:///{}'.format(temp.getsyspath(fn)))
                db.open()

                ds = list(db.datasets)[0]

                extant = self.dataset(ds.vid)
                if not extant:
                    self.database.copy_dataset(ds)

                b = self.bundle(ds.vid)
                b.state = Bundle.STATES.INSTALLED

                self.logger.info("Synced {}".format(ds.vname))
            except Exception as e:
                self.logger.error("Failed to sync {}, {}: {}".format(fn, temp.getsyspath(fn), e))

        self.database.commit()

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

    def install_packages(self, module_name, pip_name):

        from ambry.dbexceptions import ConfigurationError
        from ..util.packages import install
        import sys
        import imp
        import os

        python_dir = self._fs.python()

        if not python_dir:
            raise ConfigurationError("Can't install python requirements without a configuration item for filesystems.python")

        if not os.path.exists(python_dir):
            os.makedirs(python_dir)

        sys.path.append(python_dir)

        try:
            imp.find_module(module_name)
            return # self.log("Required package already installed: {}->{}".format(module_name, pip_name))
        except ImportError:
            self.logger.info("Installing required package: {}->{}".format(module_name, pip_name))
            install(python_dir, module_name, pip_name)


