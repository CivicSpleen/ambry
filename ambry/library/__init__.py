"""A Library is a local collection of bundles. It holds a database for the configuration
of the bundles that have been installed into it.
"""

# Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt
import imp
import logging
import os
import sys
import tempfile
import traceback

import json

from fs.osfs import OSFS
from fs.opener import fsopendir

from sqlalchemy import or_

from six import text_type

from requests.exceptions import HTTPError

from ambry.bundle import Bundle
from ambry.dbexceptions import ConfigurationError
from ambry.identity import Identity, ObjectNumber, NotObjectNumberError, NumberServer, DatasetNumber

from ambry.library.search import Search
from ambry.orm import Partition, File, Config, Table, Database, Dataset, Account
from ambry.orm.exc import NotFoundError, ConflictError
from ambry.run import get_runconfig
from ambry.util import get_logger

from .filesystem import LibraryFilesystem

logger = get_logger(__name__, level=logging.INFO, propagate=False)

global_library = None


def new_library(config=None):

    if config is None:
        config = get_runconfig()

    l = Library(config)

    global global_library

    global_library = l

    return l


class Library(object):

    def __init__(self,  config=None, search=None, echo=None, read_only=False):
        from sqlalchemy.exc import OperationalError
        from ambry.orm.exc import DatabaseMissingError

        if config:
            self._config = config
        else:
            self._config = get_runconfig()

        self.logger = logger

        self.read_only = read_only  # allow optimizations that assume we aren't building bundles.

        self._fs = LibraryFilesystem(config)

        self._db = Database(self._fs.database_dsn, echo=echo)

        self._account_password = self.config.accounts.password

        self._warehouse = None  # Will be populated in the warehouse property.

        try:
            self._db.open()
        except OperationalError as e:
            raise DatabaseMissingError("Failed to open database '{}': {} ".format(self._db.dsn, e))

        self.processes = None  # Number of multiprocessing proccors. Default to all of them

        if search:
            self._search = Search(self, search)
        else:
            self._search = None

    def sync_config(self):
        """Sync the file config into the library proxy data in the root dataset """
        from ambry.library.config import LibraryConfigSyncProxy
        lcsp = LibraryConfigSyncProxy(self)
        lcsp.sync()

    def init_debug(self):
        """Initialize debugging features, such as a handler for USR2 to print a trace"""
        import signal

        def debug_trace(sig, frame):
            """Interrupt running process, and provide a python prompt for interactive
            debugging."""

            self.log('Trace signal received')
            self.log(''.join(traceback.format_stack(frame)))

        signal.signal(signal.SIGUSR2, debug_trace)  # Register handler

    def resolve_object_number(self, ref):
        """Resolve a variety of object numebrs to a dataset number"""

        if not isinstance(ref, ObjectNumber):
            on = ObjectNumber.parse(ref)
        else:
            on = ref

        ds_on = on.as_dataset

        return ds_on

    def drop(self):
        return self.database.drop()

    def clean(self):
        return self.database.clean()

    def close(self):
        return self.database.close()

    def create(self):
        from config import LibraryConfigSyncProxy
        self.database.create()

        lcsp = LibraryConfigSyncProxy(self)
        lcsp.sync()

    @property
    def database(self):
        return self._db

    @property
    def filesystem(self):
        return self._fs

    @property
    def warehouse(self):
        if not self._warehouse:
            from ambry.library.warehouse import Warehouse
            self._warehouse = Warehouse(self)
        return self._warehouse

    @property
    def config(self):
        return self._config

    @property
    def download_cache(self):
        return OSFS(self._fs.downloads())

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

    def dataset(self, ref, load_all=False, exception=True):
        """Return all datasets"""
        return self.database.dataset(ref, load_all=load_all, exception=exception)

    def new_bundle(self, assignment_class=None, **kwargs):
        """
        Create a new bundle, with the same arguments as creating a new dataset

        :param assignment_class: String. assignment class to use for fetching a number, if one
        is not specified in kwargs
        :param kwargs:
        :return:
        """

        if not ('id' in kwargs and bool(kwargs['id'])) or assignment_class is not None:
            kwargs['id'] = self.number(assignment_class)

        ds = self._db.new_dataset(**kwargs)
        self._db.commit()

        b = self.bundle(ds.vid)
        b.state = Bundle.STATES.NEW

        b.set_last_access(Bundle.STATES.NEW)

        b.set_file_system(source_url=self._fs.source(b.identity.source_path),
                          build_url=self._fs.build(b.identity.source_path))

        bs_meta = b.build_source_files.file(File.BSFILE.META)
        bs_meta.objects_to_record()
        bs_meta.record_to_objects()

        self._db.commit()
        return b

    def new_from_bundle_config(self, config):
        """
        Create a new bundle, or link to an existing one, based on the identity in config data.

        :param config: A Dict form of a bundle.yaml file
        :return:
        """
        identity = Identity.from_dict(config['identity'])

        ds = self._db.dataset(identity.vid, exception=False)

        if not ds:
            ds = self._db.dataset(identity.name, exception=False)
        if not ds:
            ds = self._db.new_dataset(**identity.dict)

        b = Bundle(ds, self)
        b.commit()
        b.state = Bundle.STATES.NEW
        b.set_last_access(Bundle.STATES.NEW)

        # b.set_file_system(source_url=self._fs.source(ds.name),
        #                   build_url=self._fs.build(ds.name))

        return b

    def bundle(self, ref, capture_exceptions=False):
        """Return a bundle build on a dataset, with the given vid or id reference"""
        from ..orm.exc import NotFoundError

        if isinstance(ref, Dataset):
            ds = ref
        else:
            try:
                ds = self._db.dataset(ref)
            except NotFoundError:
                ds = None

        if not ds:
            try:
                p = self.partition(ref)
                ds = p._bundle.dataset
            except NotFoundError:
                ds = None

        if not ds:
            raise NotFoundError('Failed to find dataset for ref: {}'.format(ref))

        b = Bundle(ds, self)
        b.capture_exceptions = capture_exceptions

        return b

    @property
    def bundles(self):
        """ Returns all datasets in the library as bundles. """

        for ds in self.datasets:
            yield self.bundle(ds.vid)

    def partition(self, ref):
        """ Finds partition by ref and converts to bundle partition.

        Args:
            ref (str): id, vid (versioned id), name or vname (versioned name) (FIXME: try all)

        Raises:
            NotFoundError: if partition with given ref not found.

        Returns:
            FIXME:
        """

        try:
            on = ObjectNumber.parse(ref)
            ds_on = on.as_dataset

            ds = self._db.dataset(ds_on)  # Could do it in on SQL query, but this is easier.

            p = ds.partition(ref)

        except NotObjectNumberError:
            q = (self.database.session.query(Partition)
                 .filter(or_(Partition.name == str(ref), Partition.vname == str(ref)))
                 .order_by(Partition.vid.desc()))

            p = q.first()

        if not p:
            raise NotFoundError("No partition for ref: '{}'".format(ref))

        b = self.bundle(p.d_vid)
        return b.wrap_partition(p)

    def table(self, ref):
        """ Finds table by ref and returns it.

        Args:
            ref (str): id, vid (versioned id) or name of the table

        Raises:
            NotFoundError: if table with given ref not found.

        Returns:
            orm.Table

        """

        try:
            obj_number = ObjectNumber.parse(ref)
            ds_obj_number = obj_number.as_dataset

            dataset = self._db.dataset(ds_obj_number)  # Could do it in on SQL query, but this is easier.
            table = dataset.table(ref)

        except NotObjectNumberError:
            q = self.database.session.query(Table)\
                .filter(Table.name == str(ref))\
                .order_by(Table.vid.desc())

            table = q.first()

        if not table:
            raise NotFoundError("No table for ref: '{}'".format(ref))
        return table

    def remove(self, bundle):
        """ Removes a bundle from the library and deletes the configuration for
        it from the library database."""

        bundle.remove()
        self.database.remove_dataset(bundle.dataset)

    #
    # Storing
    #

    def create_bundle_file(self, b):

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
        """Duplicate a bundle, with a higher version number.

        This only copies the files, under the theory that the bundle can be rebuilt from them.
        """

        on = b.identity.on
        on.revision = on.revision + 1

        try:
            extant = self.bundle(str(on))

            if extant:
                raise ConflictError('Already have a bundle with vid: {}'.format(str(on)))
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
        nb.set_file_system(source_url=b.source_fs.getsyspath('/'))
        nb.state = Bundle.STATES.NEW

        nb.commit()

        for f in b.dataset.files:
            assert f.major_type == f.MAJOR_TYPE.BUILDSOURCE
            nb.dataset.files.append(nb.dataset.bsfile(f.minor_type).update(f))

        # Load the metadata in to records, then back out again. The objects_to_record process will set the
        # new identity object numbers in the metadata file
        nb.build_source_files.file(File.BSFILE.META).record_to_objects()
        nb.build_source_files.file(File.BSFILE.META).objects_to_record()

        ds.commit()

        return nb

    def checkin(self, b):
        """
        Copy a bundle to a new Sqlite file, then store the file on the remote.

        :param b: The bundle
        :return:
        """

        from ambry.bundle.process import call_interval

        remote_name = self.resolve_remote(b)

        remote = self.remote(remote_name)

        db_path = self.create_bundle_file(b)

        db = Database('sqlite:///{}'.format(db_path))
        ds = db.dataset(b.dataset.vid)

        with b.progress.start('checkin', 0, message='Check in bundle') as ps:

            ps.add(message='Checking in bundle {} to {}'.format(ds.identity.vname, remote))

            # Set the location for the bundle file
            for p in ds.partitions:
                p.location = 'remote'

            # Fixme; this should be on ds, not b
            # b.buildstate.state.current = Bundle.STATES.INSTALLED
            ds.commit()
            db.commit()
            db.close()

            db_ck = b.identity.cache_key + '.db'

            ps.add(message='Upload bundle file', item_type='bytes', item_count=0)
            total = [0]

            @call_interval(5)
            def upload_cb(n):
                total[0] += n
                ps.update(message='Upload bundle file', item_count=total[0])

            with open(db_path) as f:
                remote.makedir(os.path.dirname(db_ck), recursive=True, allow_recreate=True)
                self.logger.info('Send bundle file {} '.format(db_path))
                e = remote.setcontents_async(db_ck, f, progress_callback=upload_cb)
                e.wait()

            ps.update(state='done')

            os.remove(db_path)

            for p in b.partitions:

                ps.add(message='Upload partition', item_type='bytes', item_count=0, p_vid=p.vid)

                with p.datafile.open(mode='rb') as fin:

                    total = [0]

                    @call_interval(5)
                    def progress(bytes):
                        total[0] += bytes
                        ps.update(message='Upload partition'.format(p.identity.vname), item_count=total[0])

                    remote.makedir(os.path.dirname(p.datafile.path), recursive=True, allow_recreate=True)
                    event = remote.setcontents_async(p.datafile.path, fin, progress_callback=progress)
                    event.wait()

                    ps.update(state='done')

            ps.add(message='Setting metadata')
            ident = json.dumps(b.identity.dict)
            remote.setcontents(os.path.join('_meta', 'vid', b.identity.vid), ident)
            remote.setcontents(os.path.join('_meta', 'id', b.identity.id), ident)
            remote.setcontents(os.path.join('_meta', 'vname', text_type(b.identity.vname)), ident)
            remote.setcontents(os.path.join('_meta', 'name', text_type(b.identity.name)), ident)
            ps.update(state='done')

            b.dataset.commit()

            return remote_name, db_ck

    #
    # Remotes
    #

    def sync_remote(self, remote_name, bundle_name, list_only=False):
        remote = self.remote(remote_name)

        temp = fsopendir('temp://ambry-import', create_dir=True)

        entries = []

        for fn in remote.walkfiles(wildcard='*.db'):

            this_name = fn.strip('/').replace('/', '.').replace('.db', '')

            if bundle_name and this_name != bundle_name:
                continue

            if list_only:
                entries.append(this_name)
                self.logger.info(this_name)
                continue
            else:
                self.logger.info('Sync {}'.format(this_name))

            temp.makedir(os.path.dirname(fn), recursive=True, allow_recreate=True)
            with remote.open(fn, 'rb') as f:
                temp.setcontents(fn, f)

            try:
                db = Database('sqlite:///{}'.format(temp.getsyspath(fn)))
                db.open()

                ds = list(db.datasets)[0]

                try:
                    self.dataset(ds.vid)
                except NotFoundError:
                    self.database.copy_dataset(ds)

                b = self.bundle(ds.vid)
                b.state = Bundle.STATES.INSTALLED
                b.commit()

                self.search.index_bundle(b)

                entries.append(this_name)

            except Exception as e:
                self.logger.error('Failed to sync {} from {}, {}: {}'
                                  .format(fn, remote_name, temp.getsyspath(fn), e))

            # If we synced a requested bundle, no need to check more
            if bundle_name and this_name == bundle_name:
                break

        self.database.commit()
        return entries

    @property
    def remotes(self):
        """Return the names and URLs of the remotes"""
        root = self.database.root_dataset
        rc = root.config.library.remotes

        return dict(rc.items())

    def remote(self, name_or_bundle):

        from fs.opener import fsopendir

        r = self.remotes[self.resolve_remote(name_or_bundle)]

        # TODO: Hack the pyfilesystem fs.opener file to get credentials from a keychain
        # https://github.com/boto/boto/issues/2836
        if r.startswith('s3'):
            return self.filesystem.s3(r, self.account_acessor)
        else:
            return fsopendir(r, create_dir=True)

        return self.filesystem.remote(self.resolve_remote(name_or_bundle))

    def resolve_remote(self, name_or_bundle):
        """Determine the remote name for a name that is either a remote name, or the name
        of a bundle, which references a remote"""
        fails = []

        remote_names = self.remotes.keys()

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

    #
    # Accounts
    #

    @property
    def password(self):
        """The password for decrypting the account secrets"""
        return self._account_password

    @password.setter
    def password(self, v):
        self._account_password = v

    def account(self, account_id):
        """
        Return accounts references for the given account id.
        :param account_id:
        :param accounts_password: The password for decrypting the secret
        :return:
        """

        act = self.database.session.query(Account).filter(Account.account_id == account_id).one()
        act.password = self._account_password
        return act

    @property
    def account_acessor(self):

        def _accessor(account_id):
            return self.account(account_id).dict

        return _accessor

    @property
    def accounts(self):
        """
        Return an account reference
        :param account_id:
        :param accounts_password: The password for decrypting the secret
        :return:
        """
        d = {}

        if not self._account_password:
            from ambry.dbexceptions import ConfigurationError
            raise ConfigurationError(
                "Can't access accounts without setting an account password"
                " either in the accounts.password config, or in the AMBRY_ACCOUNT_PASSWORD"
                " env var.")

        for act in self.database.session.query(Account).all():
            act.password = self._account_password
            e = act.dict
            a_id = e['account_id']
            del e['account_id']
            d[a_id] = e

        return d

    @property
    def services(self):
        return self.database.root_dataset.config.library['services']

    def number(self, assignment_class=None, namespace='d'):
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
        if assignment_class == 'self':
            # When 'self' is explicit, don't look for number server config
            return str(DatasetNumber())

        elif assignment_class is None:

            try:
                nsconfig = self.services['numbers']

            except ConfigurationError:
                # A missing configuration is equivalent to 'self'
                self.logger.error('No number server configuration; returning self assigned number')
                return str(DatasetNumber())

            for assignment_class in ('self', 'unregistered', 'registered', 'authority'):
                if assignment_class+'-key' in nsconfig:
                    break

            # For the case where the number configuratoin references a self-assigned key
            if assignment_class == 'self':
                return str(DatasetNumber())

        else:
            try:
                nsconfig = self.services['numbers']

            except ConfigurationError:
                raise ConfigurationError('No number server configuration')

            if assignment_class + '-key' not in nsconfig:
                raise ConfigurationError(
                    'Assignment class {} not number server config'.format(assignment_class))

        try:

            key = nsconfig[assignment_class + '-key']
            config = {
                'key': key,
                'host': nsconfig['host'],
                'port': nsconfig.get('port', 80)
            }

            ns = NumberServer(**config)

            n = str(next(ns))
            self.logger.info('Got number from number server: {}'.format(n))

        except HTTPError as e:
            self.logger.error('Failed to get number from number server for key: {}'.format(key, e.message))
            self.logger.error('Using self-generated number. There is no problem with this, '
                              'but they are longer than centrally generated numbers.')
            n = str(DatasetNumber())

        return n

    def edit_history(self):
        """Return config record information about the most recent bundle accesses and operations"""

        ret = self._db.session\
            .query(Config)\
            .filter(Config.type == 'buildstate')\
            .filter(Config.group == 'access')\
            .filter(Config.key == 'last')\
            .order_by(Config.modified.desc())\
            .all()
        return ret

    @property
    def search(self):
        if not self._search:
            self._search = Search(self)

        return self._search

    def install_packages(self, module_name, pip_name, force=False):
        from ambry.util.packages import install

        python_dir = self._fs.python()

        if not python_dir:
            raise ConfigurationError(
                "Can't install python requirements without a configuration item for filesystems.python")

        if not os.path.exists(python_dir):
            os.makedirs(python_dir)

        sys.path.append(python_dir)

        if force:
            self.logger.info('Upgrading required package: {}->{}'.format(module_name, pip_name))
            install(python_dir, module_name, pip_name)
        else:
            try:
                imp.find_module(module_name)
                return  # self.log("Required package already installed: {}->{}".format(module_name, pip_name))
            except ImportError:
                self.logger.info('Installing required package: {}->{}'.format(module_name, pip_name))
                install(python_dir, module_name, pip_name)

    def import_bundles(self, dir, detach=False, force=False):
        """
        Import bundles from a directory

        :param dir:
        :return:
        """

        import yaml

        fs = fsopendir(dir)

        bundles = []

        for f in fs.walkfiles(wildcard='bundle.yaml'):

            self.logger.info("Visiting {}".format(f))
            config = yaml.load(fs.getcontents(f))

            if not config:
                self.logger.error("Failed to get a valid bundle configuration from '{}'".format(f))

            bid = config['identity']['id']

            try:
                b = self.bundle(bid)

            except NotFoundError:
                b = None

            if not b:
                b = self.new_from_bundle_config(config)
                self.logger.info('{} Loading New'.format(b.identity.fqname))
            else:
                self.logger.info('{} Loading Existing'.format(b.identity.fqname))

            source_url = os.path.dirname(fs.getsyspath(f))
            b.set_file_system(source_url=source_url)
            self.logger.info('{} Loading from {}'.format(b.identity.fqname, source_url))
            b.sync_in()

            if detach:
                self.logger.info("{} Detaching".format(b.identity.fqname))
                b.set_file_system(source_url=None)

            if force:
                self.logger.info("{} Sync out".format(b.identity.fqname))
                # FIXME. It won't actually sync out until re-starting the bundle.
                # The source_file_system is probably cached
                b = self.bundle(bid)
                b.sync_out()

            bundles.append(b)

        return bundles

    def process_pool(self, limited_run=False):
        """Return a pool for multiprocess operations, sized either to the number of CPUS, or a configured value"""

        from multiprocessing import cpu_count
        from ambry.bundle.concurrent import Pool, init_library

        if self.processes:
            cpus = self.processes
        else:
            cpus = cpu_count()

        self.logger.info('Starting MP pool with {} processors'.format(cpus))
        return Pool(self, processes=cpus, initializer=init_library,
                    maxtasksperchild=1,
                    initargs=[self.database.dsn, self._account_password, limited_run])
