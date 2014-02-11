"""A Library is a local collection of bundles. It holds a database for the configuration
of the bundles that have been installed into it.
"""

# Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

import os.path

from ambry.util import temp_file_name
from ambry.dbexceptions import ConfigurationError, NotFoundError, DependencyError
from ambry.bundle import DbBundle

# Setup a default logger. The logger is re-assigned by the
# bundle when the bundle instantiates the logger.
import logging
import logging.handlers
from ambry.orm import Dataset, Config
from ..identity import LocationRef, Identity

libraries = {}


def _new_library(config):
    import copy
    from ..cache import new_cache, RemoteMarker
    from database import LibraryDb
    from sqlalchemy.exc import OperationalError

    cache = new_cache(config['filesystem'])

    database = LibraryDb(**dict(config['database']))

    try:
        database.create()
    except OperationalError as e:
        from ..dbexceptions import DatabaseError
        raise DatabaseError('Failed to create {} : {}'.format(database.dsn, e.message))

    upstream = new_cache(config['upstream']) if 'upstream' in config else None

    remotes = config['remotes'] if 'remotes' in config else None

    if upstream and (not isinstance(upstream, RemoteMarker)
                     and not isinstance(upstream.last_upstream(), RemoteMarker)):
        raise ConfigurationError("Library upstream must have a RemoteMarker interface: {}".format(config))

    sourcerepo = config.get('sourcerepo', None)
    try:
        source_dir = sourcerepo.dir if sourcerepo else None
        source_repos = sourcerepo.list if sourcerepo else None
    except ConfigurationError:
        source_dir = None
        source_repos = None


    #print "### Upstream", config['upstream']

    l = Library(cache=cache,
                database=database,
                name = config['_name'] if '_name' in config else 'NONE',
                upstream=upstream,
                remotes=remotes,
                sync = config.get('sync', None),
                require_upload=config.get('require_upload', None),
                source_dir = source_dir,
                source_repos = source_repos,
                host=config.get('host', None),
                port=config.get('port', None),
                )

    return l


def new_library(config, reset=False):
    """Return a new :class:`~ambry.library.Library`, constructed from a configuration

    :param config: a :class:`~ambry.run.RunConfig` object
    :rtype:  :class:`~ambry.library.Library`

    If ``config`` is None, the function will constuct a new :class:`~ambry.run.RunConfig` with a default
    constructor.

    """

    global libraries

    if reset:
        libraries = {}

    name = config['_name']

    if name is None:
        name = 'default'

    if name not in libraries:
        libraries[name] = _new_library(config)

    l = libraries[name]
    l.clear_dependencies()

    return l

def clear_libraries():

    global libraries

    libraries = {}



import collections


class Library(object):
    '''

    '''


    def __init__(self, cache, database,
                 name=None,
                 upstream=None,  remotes=None,
                 source_dir = None, source_repos = None,
                 sync=False, require_upload=False,
                 host=None, port=None):
        '''Libraries are constructed on the root cache name for the library.
        If the cache does not exist, it will be created.

        Args:

        cache: a path name to a directory where bundle files will be stored
        database:
        remote: URL of a remote library, for fallback for get and put.
        sync: If true, put to remote synchronously. Defaults to False.

        '''

        self.name = name
        self.cache = cache
        self.source_dir = source_dir
        self.source_repos = source_repos
        self._database = database
        self._upstream = upstream

        self.sync = sync
        self.bundle = None # Set externally in bundle.library()
        self.host = host
        self.port = port
        self.dep_cb = None# Callback for dependency resolution
        self.require_upload = require_upload
        self._dependencies = None
        self._remotes = remotes

        if not self.cache:
            raise ConfigurationError("Must specify library.cache for the library in bundles.yaml")

        self.logger = logging.getLogger(__name__)
        #self.logger.setLevel(logging.DEBUG)

        self.needs_update = False


    def clone(self):

        return self.__class__(self.cache, self.database.clone(), self._upstream, self.sync, self.require_upload,
                              self.host, self.port)


    @property
    def upstream(self):
        if self._upstream:
            return self._upstream # When it is a URL to a REST interface.
        else:
            return None

    @property
    def database(self):
        '''Return ambry.database.Database object'''
        return self._database

    def load(self, rel_path, decl_md5=None):
        '''Load a bundle from the remote to the local cache and install it'''
        from ..util.flo import copy_file_or_flo
        from ..dbexceptions import ConflictError

        if not self.upstream.has(rel_path):
            raise ConfigurationError('Remote {} does not have cache key  {}'.format(self.upstream, rel_path))

        if not self.cache.has(rel_path):
            source = self.upstream.get_stream(rel_path)

            sink = self.cache.put_stream(rel_path, metadata=source.meta)

            try:
                copy_file_or_flo(source, sink)
            except:
                self.cache.remove(rel_path, propagate=True)
                raise

            source.close()
            sink.close()

        file_md5 = self.cache.md5(rel_path)

        if file_md5 != decl_md5:
            raise ConflictError('MD5 Mismatch for {} : file={} != declared={} '.format(rel_path, file_md5, decl_md5))

        abs_path = self.cache.path(rel_path)
        b = DbBundle(abs_path)

        if b.identity.cache_key != rel_path:
            raise ConflictError("Identity of downloaded bundle doesn't match request payload")

        self.put(b)

        return b

    def config(self, bp_id):

        from ..cache import RemoteMarker

        d, p = self.get_ref(bp_id)

        try:
            api = self.upstream.get_upstream(RemoteMarker)
        except AttributeError: # No api
            api = self.upstream

        if self.cache.has(d.cache_key):
            b = self.get(d.vid)
            config = b.db_config.dict
        else:
            return None

    ##
    ## Storing
    ##

    def put_bundle(self, bundle, force=False):

        dst, cache_key, url = self.put(bundle, force=force)

        for p in bundle.partitions:
            self.put(p, force=force)

        return dst, cache_key, url


    def put(self, bundle, force=False):
        '''Install a single bundle or partition file into the library.

        :param bundle: the file object to install
        :rtype: a `Partition`  or `Bundle` object

        '''

        from ..bundle import Bundle
        from ..partition import PartitionInterface

        if not isinstance(bundle, (PartitionInterface, Bundle)):
            raise ValueError("Can only install a Partition or Bundle object")

        dst, cache_key, url = self._put_file(bundle.identity, bundle.database.path, force=force)

        return dst, cache_key, url



    def _put_file(self, identity, file_path, state='new', force=False):
        '''Store a dataset or partition file, without having to open the file
        to determine what it is, by using  seperate identity'''
        from ..identity import Identity
        from .files import Files

        if isinstance(identity, dict):
            identity = Identity.from_dict(identity)

        dst = None
        if not self.cache.has(identity.cache_key) or force:
            dst = self.cache.put(file_path, identity.cache_key)
        else:
            dst = self.cache.path(identity.cache_key)

        if not os.path.exists(dst):
            raise Exception("cache {}.put() didn't return an existent path. got: {}".format(type(self.cache), dst))

        if self.upstream and self.sync:
            self.upstream.put(identity, file_path)

        f = self.files.new_file(path=dst,
                                group=self.cache.repo_id,
                                ref=identity.vid,
                                state=state)

        if identity.is_bundle:
            self.database.install_bundle_file(identity, file_path)
            f.type_ = Files.TYPE.BUNDLE
        else:
            f.type_ = Files.TYPE.PARTITION

        self.files.merge(f)

        return dst, identity.cache_key, self.cache.last_upstream().path(identity.cache_key)


    def remove(self, bundle):
        '''Remove a bundle from the library, and delete the configuration for
        it from the library database'''

        self.database.remove_bundle(bundle)

        self.cache.remove(bundle.identity.cache_key, propagate=True)


    ##
    ## Retreiving
    ##


    def list(self, datasets=None, with_meta=True, locations=None, key='fqname'):
        '''Lists all of the datasets in the partition, optionally with
        metadata. Does not include partitions. This returns a dictionary
        in  a form that is similar to the remote and source lists. '''
        import socket

        if datasets is None:
            datasets = {}

        self.database.list(datasets=datasets, locations=locations)

        return datasets

    def path(self, rel_path):
        """Return the cache path for a cache key"""

        return self.cache.path(rel_path)

    def _attach_rrc(self, url, gets, cb):
        '''Attaches a remote library, acessible through a REST interface
        to the lowest level of the Library's filesystem cache, allowing
        files to be retrieved from the remote library by making a get() request
        to the local cache '''

        from ..cache.remote import RestReadCache

        # Attach and detach the appropriate RestReadCache

        orig_last_upstream = self.cache.last_upstream()
        orig_last_upstream.upstream = RestReadCache(url)

        # Gets from the remote are slow, but after that, gets from the
        # file system cache are fast, so we can afford to repeat the gets later.

        for ident in gets:
            try:
                self.cache.get(ident.cache_key, cb=cb)
            except:
                orig_last_upstream.upstream = None
                self.logger.info("Deleting incomplete download: {}".format(self.cache.path(ident.cache_key)))
                self.cache.remove(ident.cache_key, propagate = True)
                raise


        orig_last_upstream.upstream = None

    def get(self, ref, force=False, cb=None):
        '''Get a bundle, given an id string or a name '''
        from sqlite3 import DatabaseError
        from sqlalchemy.orm.exc import NoResultFound
        from ..dbexceptions import NotFoundError
        from .files import  Files
        from ..orm import Dataset

        # Get a reference to the dataset, partition and relative path
        # from the local database.

        dataset = self.resolve(ref)

        if not dataset:
            return None



        if Dataset.LOCATION.REMOTE in dataset.locations.codes:

            f  = self.files.query.type(Dataset.LOCATION.REMOTE).ref(dataset.vid).one

            gets = [dataset]

            if dataset.partition:
                gets.append(dataset.partition)

            # Since it was remote, attach the appropriate remote cache to our cache stack then
            # when we read from the top level, we'l get it from the remote.

            # NOTE! The partition and bundle are actually fetched from the remote in _attach_rrc!
            # All of the subsequent cache gets in this function just read from the local cache


            self._attach_rrc(f.source_url, gets, cb=cb)

        # First, get the bundle and instantiate it. If what was requested
        # was just the bundle, return it, otherwise, return it. If it was
        # a partition, get the partition from the bundle.

        abs_path = self.cache.get(dataset.cache_key, cb=cb)

        if not abs_path or not os.path.exists(abs_path):
            return False

        try:
            bundle = DbBundle(abs_path)
        except DatabaseError:
            self.logger.error("Failed to load databundle at path {}".format(abs_path))
            raise

        # Do we have it in the database? If not install it.
        # It should be installed if it was retrieved remotely,
        # but may not be installed if there is a local copy in the cache.

        try:
            d = self.database.get(bundle.identity.vid)
        except NotFoundError:
            d = None

        if not d:
            self.sync_library_dataset(bundle)

        bundle.library = self

        if dataset.partition:

            # Ensure the partition is in the cache
            if not self.cache.has(dataset.partition.cache_key):
                f = self.files.query.type(Dataset.LOCATION.REMOTE).ref(dataset.vid).one

                # Since it was remote, attach the appropriate remote cache to our cache stack then
                # when we read from the top level, we'l get it from the remote.
                self._attach_rrc(f.source_url, [dataset.partition], cb=cb)

            else:
                url = None

            abs_path = self.cache.get(dataset.partition.cache_key, cb=cb)

            if not abs_path or not os.path.exists(abs_path):

                raise NotFoundError('Failed to get partition {} from cache '.format(dataset.partition.fqname))

            try:
                partition = bundle.partitions.get(dataset.partition.vid)
            except KeyboardInterrupt:
                raise

            self.sync_library_partition(partition.identity)

            if not partition:
                from ..dbexceptions import NotFoundError

                raise NotFoundError('Failed to get partition {} from bundle '.format(dataset.partition.fqname))

            # Attach the partition into the bundle, and return both.
            bundle.partition = partition

        return bundle

    ##
    ## Finding
    ##

    def find(self, query_command):

        return self.database.find(query_command)


    @property
    def resolver(self):

        return self.database.resolver

    @property
    def remote_resolver(self):

        from .query import RemoteResolver
        #return RemoteResolver(local_resolver=self.database.resolver, remote_urls=self._remotes)
        return RemoteResolver(local_resolver=None, remote_urls=self._remotes)


    def resolve(self, ref, location = [Dataset.LOCATION.LIBRARY,Dataset.LOCATION.REMOTE], use_remote = False):
        from .query import RemoteResolver

        if isinstance(ref, Identity):
            ref = ref.vid

        if use_remote:
            resolver = RemoteResolver(local_resolver=self.resolver, remote_urls=self._remotes)
        else:
            resolver = self.resolver

        ip, ident = resolver.resolve_ref_one(ref, location)

        try:
            if ident:
                ident.bundle_path = self.source.source_path(ident=ident)
        except ConfigurationError:
            pass  # Warehouse libraries don't have source directories.

        return ident


    ##
    ## Dependencies
    ##

    def dep(self, name):
        """"Bundle version of get(), which uses a key in the
        bundles configuration group 'dependencies' to resolve to a name"""
        from ..dbexceptions import DependencyError

        object_ref = self.dependencies.get(name, False)

        if not object_ref:
            raise DependencyError("No dependency named '{}'".format(name))

        o = self.get(object_ref)

        if not o:
            raise DependencyError("Failed to get dependency, key={}, id={}".format(name, object_ref))

        if self.dep_cb:
            self.dep_cb(self, name, object_ref, o)

        return o

    @property
    def dependencies(self):

        if not self._dependencies:
            self._dependencies = self._get_dependencies()

        return self._dependencies

    def clear_dependencies(self):
        self._dependencies = None

    def _get_dependencies(self):
        from ..orm import Dataset

        if not self.bundle:
            raise ConfigurationError("Can't use the dep() method for a library that is not attached to a bundle");

        group = self.bundle.config.group('build')

        errors = 0

        try:
            deps = group.get('dependencies')
        except AttributeError:
            deps = None

        if not deps:
            return {}

        out = {}
        for k, v in deps.items():

            try:
                ident = self.resolve(v)
                if not ident:
                    self.bundle.error("Failed to resolve {} ".format(v))
                    errors += 1
                    continue
                if ident.partition:
                    out[k] = ident.partition
                else:
                    out[k] = ident
            except Exception as e:
                self.bundle.error(
                    "Failed to parse dependency name '{}' for '{}': {}".format(v, self.bundle.identity.name, e.message))
                errors += 1

        if errors > 0:
            raise DependencyError("Failed to find one or more dependencies")

        return out

    def check_dependencies(self, throw=True):
        from ..util import Progressor

        errors = {}
        for k, v in self.dependencies.items():
            self.logger.info('Download and check dependency: {}'.format(v))
            b = self.get(v, cb=Progressor().progress)

            if not b:
                if throw:
                    raise NotFoundError("Dependency check failed for key={}, id={}. Failed to get bundle or partition".format(k, v))
                else:
                    errors[k] = v


    @property
    def files(self):
        from .files import Files

        return Files(self.database)

    @property
    def source(self):
        '''Return a SourceTree object, based on the source_repo configuration'''
        from ..source import SourceTree

        if not self.source_dir:
            raise ConfigurationError("Don't have a source_dir")

        return SourceTree(self.source_dir, self.source_repos, self, self.logger)


    @property
    def new_files(self):
        '''Generator that returns files that should be pushed to the remote
        library'''

        new_files = self.files.query.installed.state('new').all

        for nf in new_files:
            yield nf

    def push(self, ref=None, cb=None):
        """Push any files marked 'new' to the upstream

        Args:
            file_: If set, push a single file, obtailed from new_files. If not, push all files.

        """
        import time

        what = None
        start = None
        end = None
        if not self.upstream:
            raise Exception("Can't push() without defining a upstream. ")

        if ref is not None:

            ip, dsid = self.resolver.resolve_ref_one(ref)

            if not dsid:
                raise Exception("Didn't get id from database for ref: {}".format(ref))

            if dsid.partition:
                identity = dsid.partition
            else:
                identity = dsid

            file_ = self.files.query.installed.ref(identity.vid).one

            md = identity.to_meta(file=file_.path)

            if False and self.upstream.has(identity.cache_key):
                if cb: cb('Has', md, 0)
                what = 'has'
                file_.state = 'pushed'

            else:
                start = time.clock()
                if cb: cb('Pushing', md, start)

                self.upstream.put(file_.path, identity.cache_key, metadata=md)
                end = time.clock()
                dt = end - start
                file_.state = 'pushed'
                if cb: cb('Pushed', md, dt)
                what = 'pushed'

            if identity.is_bundle:
                self.sync_upstream_dataset(identity, md)

            self.database.session.merge(file_)
            self.database.commit()

        else:
            for file_ in self.new_files:
                self.push(file_.ref, cb=cb)

        return what, start, end, md['size'] if 'size' in md else None


    #
    # Maintainence
    #

    def clean(self, add_config_root=True):
        self.database.clean(add_config_root=add_config_root)


    def purge(self):
        """Remove all records from the library database, then delete all
        files from the cache"""
        self.clean()
        self.cache.clean()

    #
    # Backup and restore
    #

    def run_dumper_thread(self):
        '''Run a thread that will check the database and call the callback when the database should be
        backed up after a change. '''
        from util import DumperThread

        dt = DumperThread(self.clone())
        dt.start()

        return dt

    def backup(self):
        '''Backup the database to the remote, but only if the database needs to be backed up. '''

        if not self.database.needs_dump():
            return False

        backup_file = temp_file_name() + ".db"

        self.database.dump(backup_file)

        path = self.upstream.put(backup_file, '_/library.db')

        os.remove(backup_file)

        return path

    def can_restore(self):

        backup_file = self.cache.get('_/library.db')

        if backup_file:
            return True
        else:
            return False

    def restore(self, backup_file=None):
        '''Restore the database from the remote'''

        if not backup_file:
            # This requires that the cache have and upstream that is also the remote
            backup_file = self.cache.get('_/library.db')

        self.database.restore(backup_file)

        # HACK, fix the dataset root
        try:
            self.database._clean_config_root()
        except:
            print "ERROR for path: {}, {}".format(self.database.dbname, self.database.dsn)
            raise

        os.remove(backup_file)

        return backup_file


    def sync_library(self):
        '''Rebuild the database from the bundles that are already installed
        in the repository cache'''

        from ambry.bundle import DbBundle
        from .files import Files
        from database import ROOT_CONFIG_NAME_V

        assert Files.TYPE.BUNDLE == Dataset.LOCATION.LIBRARY
        assert Files.TYPE.PARTITION == Dataset.LOCATION.PARTITION

        (self.database.session.query(Dataset)
            .filter(Dataset.vid != ROOT_CONFIG_NAME_V)
            .filter(Dataset.location == Dataset.LOCATION.LIBRARY).delete())

        self.files.query.type(Files.TYPE.BUNDLE).delete()
        self.files.query.type(Files.TYPE.PARTITION).delete()

        bundles = []

        self.logger.info("Rebuilding from dir {}".format(self.cache.cache_dir))

        for r, d, f in os.walk(self.cache.cache_dir): #@UnusedVariable

            if '/meta/' in r:
                continue

            for file_ in f:

                if file_.endswith(".db"):
                    path_ = os.path.join(r, file_)
                    try:
                        b = DbBundle(path_)
                        # This is a fragile hack -- there should be a flag in the database
                        # that diferentiates a partition from a bundle.
                        f = os.path.splitext(file_)[0]

                        if b.db_config.get_value('info', 'type') == 'bundle':
                            self.logger.info("Queing: {} from {}".format(b.identity.vname, file_))
                            bundles.append(b)

                    except Exception as e:
                        raise
                        pass
                        self.logger.error('Failed to process {}, {} : {} '.format(file_, path_, e))

        for bundle in bundles:
            self.logger.info('Installing: {} '.format(bundle.identity.vname))

            try:
                self.sync_library_dataset(bundle)
            except Exception as e:
                self.logger.error('Failed to install bundle {}'.format(bundle.identity.vname))
                continue

            for p in bundle.partitions:
                if self.cache.has(p.identity.cache_key, use_upstream=False):
                    self.logger.info('            {} '.format(p.identity.vname))
                    self.sync_library_partition(p.identity)

        self.database.commit()
        return bundles

    def sync_library_dataset(self, bundle):

        from files import Files

        assert Files.TYPE.BUNDLE == Dataset.LOCATION.LIBRARY

        ident  = bundle.identity

        self.database.install_bundle(bundle)

        self.files.new_file(
            merge=True,
            path=bundle.database.path,
            group=self.cache.repo_id,
            ref=ident.vid,
            state='rebuilt',
            type_= Files.TYPE.BUNDLE,
            data=None,
            source_url=None)

    def sync_library_partition(self, ident):
        from files import Files

        self.files.new_file(
            merge=True,
            path=ident.fqname,
            group=None,
            ref=ident.vid,
            state='rebuilt',
            type_= Files.TYPE.PARTITION,
            data=ident.urls,
            source_url=None)

    def sync_upstream(self):
        import json
        from ..identity import Identity

        self.database.session.query(Dataset).filter(Dataset.location == Dataset.LOCATION.UPSTREAM).delete()
        self.files.query.type(Dataset.LOCATION.UPSTREAM).delete()


        if not self.upstream:
            return

        self.logger.info("Upstream sync: {}".format(self.upstream))

        for e in self.upstream.list():
            md =  self.upstream.metadata(e)
            ident = Identity.from_dict(json.loads(md['identity']))

            self.sync_upstream_dataset(ident, md)

            self.logger.info("Upstream sync: {}".format(ident.fqname))



    def sync_upstream_dataset(self, ident, metadata):

        self.database.install_dataset_identity(ident, location=Dataset.LOCATION.UPSTREAM)

        self.files.new_file(
            merge=True,
            path=ident.fqname,
            group='upstream',
            ref=ident.vid,
            state='synced',
            type_=Dataset.LOCATION.UPSTREAM,
            data=metadata,
            source_url=None)


    def sync_remotes(self):

        from ambry.client.rest import RemoteLibrary

        self.database.session.query(Dataset).filter(Dataset.location == Dataset.LOCATION.REMOTE).delete()
        self.files.query.type(Dataset.LOCATION.REMOTE).delete()

        if not self.remotes:
            return

        for url in self.remotes:
            self.logger.info("Remote sync: {}".format(url))
            rl = RemoteLibrary(url)
            for ident in rl.list().values():
                self.sync_remote_dataset(url, ident)

                self.logger.info("Remote {} sync: {}".format(url, ident.fqname))


    def sync_remote_dataset(self, url, ident):

        self.database.install_dataset_identity(ident, location=Dataset.LOCATION.REMOTE)

        self.files.new_file(
            merge=True,
            path=ident.fqname,
            group=url,
            ref=ident.vid,
            state='synced',
            type_=Dataset.LOCATION.REMOTE,
            data=ident.urls,
            source_url=url)


    @property
    def remotes(self):
        return self._remotes

    @property
    def info(self):
        return """
------ Library {name} ------
Database: {database}
Cache:    {cache}
Remote:   {remote}
        """.format(name=self.name, database=self.database.dsn,
                   cache=self.cache, remote=self.upstream if self.upstream else '')

    @property
    def dict(self):
        return dict(name=str(self.name),
                    database=str(self.database.dsn),
                    cache=str(self.cache),
                    remote=str(self.upstream) if self.upstream else None)

