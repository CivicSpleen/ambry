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
from ..util import memoize
import weakref
from files import Files
import collections
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

    source_dir = config.get('source', None)

    hostport = config.get('host', None)

    if hostport:
        if ':' in hostport:
            host, port = hostport.split(':')
        else:
            host = hostport
            port = 80
    else:
        host = None
        port = 80


    l = Library(cache=cache,
                database=database,
                name = config['_name'] if '_name' in config else 'NONE',
                upstream=upstream,
                remotes=remotes,
                sync = config.get('sync', None),
                require_upload=config.get('require_upload', None),
                source_dir = source_dir,
                host = host,
                port = port,
                urlhost=config.get('urlhost', None)

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



class Library(object):
    '''

    '''


    def __init__(self, cache, database,
                 name=None,
                 upstream=None,  remotes=None,
                 source_dir = None,
                 sync=False, require_upload=False,
                 host=None, port=None, urlhost = None):
        '''Libraries are constructed on the root cache name for the library.
        If the cache does not exist, it will be created.

        Args:

        cache: a path name to a directory where bundle files will be stored
        database:
        remote: URL of a remote library, for fallback for get and put.
        sync: If true, put to remote synchronously. Defaults to False.

        '''

        assert database is not None

        self.name = name
        self.cache = cache
        self.source_dir = source_dir

        self._database = database
        self._upstream = upstream

        self.sync = sync
        self.bundle = None # Set externally in bundle.library()
        self.host = host
        self.port = port
        self.urlhost = urlhost if urlhost else ( '{}:{}'.format(self.host,self.port) if self.port else self.host)
        self.dep_cb = None# Callback for dependency resolution
        self.require_upload = require_upload
        self._dependencies = None
        self._remotes = remotes

        if not self.cache:
            raise ConfigurationError("Must specify library.cache for the library in bundles.yaml")

        self.logger = logging.getLogger(__name__)
        #self.logger.setLevel(logging.DEBUG)

        self.needs_update = False

        self.bundles = weakref.WeakValueDictionary()


    def clone(self):

        return self.__class__(self.cache, self.database.clone(), self._upstream, self.sync, self.require_upload,
                              self.host, self.port)

    def _create_bundle(self, path):
        from ..bundle.bundle import DbBundle

        if path in self.bundles:
            return self.bundles[path]

        bundle  = DbBundle(path)

        self.bundles[path] = bundle

        return bundle


    def close(self):

        for path, bundle in self.bundles.items():
            bundle.close()

        self.database.close()


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
        b = self._create_bundle( abs_path)

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

    def put_bundle(self, bundle, force=False, logger=None, install_partitions=True):

        self.database.install_bundle(bundle)
        dst, cache_key, url = self._put_file(bundle.identity, bundle.database.path,
                                             force=force)

        if install_partitions:
            for p in bundle.partitions:
                self.put_partition(bundle, p, force=force)

        return dst, cache_key, url

    def put_partition(self, bundle, partition, force=False):
        self.database.install_partition(bundle, partition.identity.id_)
        dst, cache_key, url = self._put_file(partition.identity, partition.database.path,
                                             force=force)

        return dst, cache_key, url


    def _put_file(self, identity, file_path, state='new', force=False):
        '''Store a dataset or partition file, without having to open the file
        to determine what it is, by using  seperate identity'''
        from ..identity import Identity
        from .files import Files
        from ..util import md5_for_file


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

        self.files.query.path(dst).group(self.cache.repo_id).delete()

        f = self.files.new_file(path=dst,
                                group=self.cache.repo_id,
                                ref=identity.vid,
                                state=state)

        if identity.is_bundle:
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
        '''Attaches a remote library, accessible through a REST interface
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



    def get(self, ref, force=False, cb=None, use_remote=False):
        '''Get a bundle, given an id string or a name '''
        from sqlite3 import DatabaseError
        from sqlalchemy.exc import OperationalError, IntegrityError
        from sqlalchemy.orm.exc import NoResultFound
        from ..dbexceptions import NotFoundError
        from .files import  Files
        from ..orm import Dataset

        # Get a reference to the dataset, partition and relative path
        # from the local database.

        dataset = self.resolve(ref, use_remote=use_remote)

        if not dataset:
            return None

        if (Dataset.LOCATION.REMOTE in dataset.locations.codes and
            Dataset.LOCATION.LIBRARY not in dataset.locations.codes and
            use_remote):

            f = self.files.query.type(Dataset.LOCATION.REMOTE).ref(dataset.vid).one

            # Since it was remote, attach the appropriate remote cache to our cache stack then
            # when we read from the top level, we'll get it from the remote.

            # NOTE! The partition and bundle are actually fetched from the remote in _attach_rrc!
            # All of the subsequent cache gets in this function just read from the local cache

            self._attach_rrc(f.source_url, [dataset], cb=cb)

        # First, get the bundle and instantiate it. If what was requested
        # was just the bundle, return it, otherwise, return it. If it was
        # a partition, get the partition from the bundle.

        abs_path = self.cache.get(dataset.cache_key, cb=cb)

        if not abs_path or not os.path.exists(abs_path):
            return False

        try:
            bundle = self._create_bundle( abs_path)
        except DatabaseError as e:
            self.logger.error("Failed to load databundle at path {}: {}".format(abs_path,e))
            raise
        except AttributeError as e:
            self.logger.error("Failed to load databundle at path {}: {}".format(abs_path,e))
            raise # DatabaseError
        except OperationalError as e:
            self.logger.error("Failed to load databundle at path {}: {}".format(abs_path,e))
            raise DatabaseError


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
            if not self.cache.has(dataset.partition.cache_key) and use_remote:
                try:
                    f = self.files.query.type(Dataset.LOCATION.REMOTE).ref(dataset.vid).one
                except:
                    bundle.close()
                    raise

                # Since it was remote, attach the appropriate remote cache to our cache stack then
                # when we read from the top level, we'll get it from the remote.
                self._attach_rrc(f.source_url, [dataset.partition], cb=cb)

            else:
                url = None


            try:
                partition = bundle.partitions.get(dataset.partition.vid)
            except KeyboardInterrupt:
                bundle.close()
                raise

            if not partition:
                from ..dbexceptions import NotFoundError
                bundle.close()
                raise NotFoundError('Failed to get partition {} from bundle at {} '
                                    .format(dataset.partition.fqname, abs_path))

            abs_path = self.cache.get(partition.identity.cache_key, cb=cb)

            if not abs_path or not os.path.exists(abs_path):
                bundle.close()
                raise NotFoundError('Failed to get partition {} from cache '.format(partition.identity.fqname))

            try:
                self.sync_library_partition(bundle, partition.identity)
            except IntegrityError as e:
                self.database.session.rollback()
                self.logger.error("Partition is already in Library.: {} ".format(e.message))

            # Attach the partition into the bundle, and return both.
            bundle.partition = partition

        bundle.close()

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

        if use_remote: # Do we need the remote resolver with remote sync?
            raise Exception("Don't be using the remote for resolution")
            resolver = RemoteResolver(local_resolver=self.resolver, remote_urls=self._remotes)
        else:
            resolver = self.resolver

        ip, ident = resolver.resolve_ref_one(ref, location)

        try:
            if ident and self.source:
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

        from sqlalchemy.orm.exc import NoResultFound

        object_ref = self.dependencies.get(name, False)

        if not object_ref:
            raise DependencyError("No dependency named '{}'".format(name))

        try:
            o = self.get(object_ref)
        except NoResultFound:
            o = None

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
                ident = self.resolve(v, use_remote=True)
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
    @memoize
    def files(self):

        return Files(self.database)

    @property
    def source(self):
        '''Return a SourceTree object, based on the source_repo configuration'''
        from ..source import SourceTree

        if not self.source_dir:
            return None

        return SourceTree(self.source_dir, self, self.logger)


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
                self.sync_upstream_dataset(file_.path, identity, md)

            self.database.session.merge(file_)
            self.database.commit()

            return what, start, end, md['size'] if 'size' in md else None

        else:
            for file_ in self.new_files:
                self.push(file_.ref, cb=cb)




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
    # Synchronize
    #

    def sync_library(self, clean=False):
        '''Rebuild the database from the bundles that are already installed
        in the repository cache'''


        from .files import Files
        from database import ROOT_CONFIG_NAME_V

        assert Files.TYPE.BUNDLE == Dataset.LOCATION.LIBRARY
        assert Files.TYPE.PARTITION == Dataset.LOCATION.PARTITION

        if clean:
            (self.database.session.query(Dataset)
                .filter(Dataset.vid != ROOT_CONFIG_NAME_V)
                .filter(Dataset.location == Dataset.LOCATION.LIBRARY).delete())

            self.files.query.type(Files.TYPE.BUNDLE).delete()
            self.files.query.type(Files.TYPE.PARTITION).delete()

        bundles = []

        self.logger.info("Rebuilding from dir {}".format(self.cache.cache_dir))

        for r, d, f in os.walk(self.cache.cache_dir, topdown=True): #@UnusedVariable

            # Exclude all of the directories which have the same basename as a database file. These
            # hold only partitions.
            d[:] = [dr for dr in d if dr + ".db" not in f]

            if '/meta/' in r:
                continue

            for file_ in f:

                if file_.endswith(".db"):
                    path_ = os.path.join(r, file_)

                    extant_bundle = self.files.query.type(Files.TYPE.BUNDLE).path(path_).one_maybe
                    extant_partition = self.files.query.type(Files.TYPE.PARTITION).path(path_).one_maybe
                    if (extant_bundle or extant_partition):
                        continue

                    try:

                        b = self._create_bundle( path_)
                        # This is a fragile hack -- there should be a flag in the database
                        # that differentiates a partition from a bundle.
                        f = os.path.splitext(file_)[0]

                        if b.get_value('info', 'type') == 'bundle':
                            self.logger.info("Queing: {} from {}".format(b.identity.vname, file_))
                            bundles.append(b)
                    except NotFoundError:
                        # Probably a partition, not a bundle.
                        pass
                    except Exception as e:
                        raise
                        self.logger.error('Failed to process {}, {} : {} '.format(file_, path_, e))

        bundles = sorted(bundles, key = lambda b: b.partitions.count)

        for bundle in bundles:
            self.logger.info('Installing: {} '.format(bundle.identity.vname))

            try:
                self.sync_library_dataset(bundle, install_partitions=False)
            except Exception as e:
                self.logger.error('Failed to install bundle {}: {}'.format(bundle.identity.vname, e.message))
                continue

            for p in bundle.partitions:
                if self.cache.has(p.identity.cache_key, use_upstream=False):
                    self.logger.info('            {} '.format(p.identity.vname))
                    self.sync_library_partition(bundle, p.identity, commit = False)

            self.database.commit()
        return bundles

    def sync_library_dataset(self, bundle, install_partitions=True):

        from files import Files

        assert Files.TYPE.BUNDLE == Dataset.LOCATION.LIBRARY

        ident  = bundle.identity

        self.database.install_bundle(bundle, install_partitions = install_partitions)

        self.files.new_file(
            commit=False,
            merge=True,
            path=bundle.database.path,
            group=self.cache.repo_id,
            ref=ident.vid,
            state='rebuilt',
            type_= Files.TYPE.BUNDLE,
            data=None,
            source_url=None)

    def sync_library_partition(self, bundle, ident, install_tables=True,
                               install_partition=True, commit = True):
        from files import Files

        if install_partition:
            self.database.install_partition(bundle, ident.id_,
                                            install_bundle=False, install_tables=install_tables)

        self.files.new_file(
            commit = commit,
            merge=True,
            path=ident.fqname,
            group=None,
            ref=ident.vid,
            state='rebuilt',
            type_= Files.TYPE.PARTITION,
            data=ident.urls,
            source_url=None)


    def sync_upstream(self, clean=False):
        import json
        from ..identity import Identity

        if clean:
            self.database.session.query(Dataset).filter(Dataset.location == Dataset.LOCATION.UPSTREAM).delete()
            self.files.query.type(Dataset.LOCATION.UPSTREAM).delete()

        if not self.upstream:
            return

        self.logger.info("Upstream sync: {}".format(self.upstream))

        for e in self.upstream.list():

            if self.files.query.type(Dataset.LOCATION.UPSTREAM).path(e).one_maybe:
                #self.logger.info("Upstream found: {}".format(e))
                continue

            md =  self.upstream.metadata(e)
            print '!!!', md
            ident = Identity.from_dict(json.loads(md['identity']))
            self.sync_upstream_dataset(e, ident, md)

            self.logger.info("Upstream sync: {}".format(ident.fqname))

    def sync_upstream_dataset(self, path, ident, metadata):

        self.database.install_dataset_identity(ident, location=Dataset.LOCATION.UPSTREAM)

        f = self.files.new_file(
            merge=True,
            path=path,
            group='upstream',
            ref=ident.vid,
            state='synced',
            type_=Dataset.LOCATION.UPSTREAM,
            data=metadata,
            source_url=None)

        self.download_upstream(f)


    def sync_remotes(self, clean=False):

        from ambry.client.rest import RemoteLibrary

        if clean:
            self.database.session.query(Dataset).filter(Dataset.location == Dataset.LOCATION.REMOTE).delete()
            self.files.query.type(Dataset.LOCATION.REMOTE).delete()

        if not self.remotes:
            return

        for url in self.remotes:

            self.logger.info("Remote sync: {}".format(url))
            rl = RemoteLibrary(url)
            for ident in rl.list().values():

                if self.files.query.type(Dataset.LOCATION.REMOTE).ref(ident.vid).one_maybe:
                    continue

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



    def download_upstream(self, f):
        '''Download all of the upstream bundles and load them as library bundles. '''
        from .files import Files
        import json

        #print self.upstream
        #print f.path, self.upstream.path(f.path)

        try:
            d = json.loads(f.data.get('identity'))
        except:
            return

        identity = Identity.from_dict(d)

        dst = None
        if not self.cache.has(identity.cache_key):
            s = self.upstream.get_stream(f.path)
            dst = self.cache.put(s, identity.cache_key)
            self.logger.info('Downloaded: {}'.format(dst))
        else:
            dst = self.cache.path(identity.cache_key)

        extant = self.files.query.type(Files.TYPE.BUNDLE).ref(f.ref).one_maybe
        if not extant:
            try:
                b = self._create_bundle( dst)
            except:
                self.logger.error("Failed to open bundle: {} ".format(dst))
                return

            self.sync_library_dataset(b)
            self.logger.info('Synchronized to library: {}'.format(dst))

    @property
    def remotes(self):
        return self._remotes

    @property
    def info(self):
        return """
------ Library {name} ------
Database: {database}
Cache:    {cache}
Remotes:  {remotes}
        """.format(name=self.name, database=self.database.dsn,
                   cache=self.cache, remotes=', '.join(self.remotes) if self.remotes else '')

    @property
    def dict(self):
        return dict(name=str(self.name),
                    database=str(self.database.dsn),
                    cache=str(self.cache),
                    remote=str(self.upstream) if self.upstream else None)


class AnalysisLibrary(Library):
    '''A Library that redefines some of the methods to make them easier to use from ipython'''

    def __init__(self, library):
        self.l = library

    def list(self,  fields=None):
        '''List all of the datasets available in this library

        :param fields: If set, is a list of fields to be displayed. Available fields are:
            * deps. Number of dependencies in the bundle
            * locations. A string that indicates where the bundle exists. ``S`` is source, ``L`` is the local library
                and ``R`` is the remote library.
            * vid. The id number, with a version suffix
            * status. The build status string for the bundle.
            * sname. The bundle's simple name
            * vname. The simple name, with the semantic version number
            * fqname. The bundle's fully qualified name
            * source_path. If the bundle exists as source, the path to the source directory.

        '''
        from  ..identity import IdentitySet

        l = self.l.list()


        return IdentitySet(sorted(l.values(), key=lambda ident: ident.vname), fields=fields)

    def about(self, ref):
        '''Lookup a bundle or partition reference and display information about it.

        This command will fetch the bundle from the remote, or instantiate it from source, so for bundles with
        very large schemas ( Like the US Census or ACS ) it may take a while to execute.
        '''

        ident = None


    def find(self, command_string = None, source=None, name=True, fields=None):
        from ..identity import IdentitySet
        from ..library.query import QueryCommand

        idents = []

        if not command_string:
            if source:
                command_string = "identity.source = {}".format(source)
            elif name:
                command_string = "identity.name like {}".format(name)


        qc = QueryCommand.parse(command_string)

        vids = set()
        for entry in self.l.find(qc):
            vids.add(entry['identity']['vid'])

        for vid in vids:
            ident = self.l.resolve(vid, None)
            idents.append(ident)

        return IdentitySet(idents, fields=fields)

    def get(self, ref, force=False, cb=None):
        return self.l.get( ref=ref, force=force, cb=cb)

    @property
    def info(self):
        return self.l.info

    def _repr_html_(self):
        return self.info.replace('\n','<br/>')

