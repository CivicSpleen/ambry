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

from ambry.orm import Dataset, Config
from ..identity import LocationRef, Identity
from ..util import memoize, get_logger
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

    if 'upstream' in config:
        raise DeprecationWarning("Upstream no longer allowed in configuration")

    root = config['root']

    remotes =  [ new_cache(remote) for remote in config.get('remotes' ,[])]

    for i,remote in enumerate(remotes):
        remote.set_priority(i)


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
                remotes=remotes,
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
                 name=None, remotes=None,
                 source_dir = None,
                 require_upload=False,
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
        self.bundle = None # Set externally in bundle.library()
        self.host = host
        self.port = port
        self.urlhost = urlhost if urlhost else ( '{}:{}'.format(self.host,self.port) if self.port else self.host)
        self.dep_cb = None# Callback for dependency resolution
        self.require_upload = require_upload
        self._dependencies = None
        self._remotes = remotes

        self._all_vids = None

        if not self.cache:
            raise ConfigurationError("Must specify library.cache for the library in bundles.yaml")

        self.logger = get_logger(__name__)
        self.logger.setLevel(logging.DEBUG)

        self.needs_update = False

        self.bundles = weakref.WeakValueDictionary()


    def clone(self):

        return self.__class__(self.cache, self.database.clone(),  self.require_upload,
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
    def database(self):
        '''Return ambry.database.Database object'''
        return self._database

    ##
    ## Storing
    ##

    def put_bundle(self, bundle, logger=None, install_partitions=True, commit = True):
        """Install the records for the dataset, tables, columns and possibly partitions. Does not
        install file references """
        from ..dbexceptions import ConflictError

        try:
            self.database.install_bundle(bundle, install_partitions=install_partitions, commit = commit)
            installed = True
        except ConflictError:
            installed = False

        self.files.install_bundle_file(bundle, self.cache, commit=commit)

        ident = bundle.identity
        return self.cache.path(ident.cache_key), installed


    def put_partition(self, bundle, partition, commit = True):
        """Install the record and file reference for the partition """

        self.database.install_partition(bundle, partition, commit = commit)

        installed = self.files.install_partition_file(partition, self.cache, commit = commit)

        return self.cache.path(partition.identity.cache_key), installed


    def remove(self, bundle):
        '''Remove a bundle from the library, and delete the configuration for
        it from the library database'''

        self.database.remove_bundle(bundle)

        self.cache.remove(bundle.identity.cache_key, propagate=True)


    ##
    ## Retreiving
    ##


    def list(self, datasets=None, with_partitions = False):
        '''Lists all of the datasets in the partition, optionally with
        metadata. Does not include partitions. This returns a dictionary
        in  a form that is similar to the remote and source lists. '''
        import socket

        if datasets is None:
            datasets = {}

        self.database.list(datasets=datasets, with_partitions = with_partitions)

        return datasets

    def path(self, rel_path):
        """Return the cache path for a cache key"""

        return self.cache.path(rel_path)

    @property
    def remote_stack(self):
        """Return a MultiCache that has all of the remotes as upstreams"""

        from ..cache.multi import MultiCache

        return MultiCache(self.remotes)


    def _get_bundle_by_cache_key(self, cache_key, cb=None):
        from ..cache.multi import AltReadCache
        from sqlite3 import DatabaseError
        from sqlalchemy.exc import OperationalError, IntegrityError

        # The AltReadCache will get from the remote stack if the path is not found in the cache.
        arc = AltReadCache(self.cache, self.remote_stack)
        abs_path = arc.get(cache_key, cb=cb)

        if not abs_path or not os.path.exists(abs_path):
            return False

        try:
            bundle = self._create_bundle(abs_path)
        except DatabaseError as e:
            self.logger.error("Failed to load databundle at path {}: {}".format(abs_path, e))
            raise
        except AttributeError as e:
            self.logger.error("Failed to load databundle at path {}: {}".format(abs_path, e))
            raise  # DatabaseError
        except OperationalError as e:
            self.logger.error("Failed to load databundle at path {}: {}".format(abs_path, e))
            raise DatabaseError

        bundle.library = self

        return bundle


    def get(self, ref, force=False, cb=None):
        '''Get a bundle, given an id string or a name '''
        from sqlite3 import DatabaseError
        from sqlalchemy.exc import OperationalError, IntegrityError
        from sqlalchemy.orm.exc import NoResultFound
        from ..dbexceptions import NotFoundError
        from .files import  Files
        from ..orm import Dataset
        from ..cache.multi import AltReadCache

        # Get a reference to the dataset, partition and relative path
        # from the local database.

        dataset = self.resolve(ref)

        if not dataset:
            return False

        bundle = self._get_bundle_by_cache_key(dataset.cache_key)

        try:
            if dataset.partition:

                partition = bundle.partitions.get(dataset.partition.vid)

                if not partition:
                    from ..dbexceptions import NotFoundError
                    raise NotFoundError('Failed to get partition {} from bundle at {} '
                                        .format(dataset.partition.fqname, dataset.cache_key))

                arc = AltReadCache(self.cache, self.remote_stack)
                abs_path = arc.get(partition.identity.cache_key, cb=cb)

                if not abs_path or not os.path.exists(abs_path):
                    raise NotFoundError('Failed to get partition {} from cache '.format(partition.identity.fqname))

                try:
                    self.database.install_partition_by_id(bundle, dataset.partition.id_,
                                                    install_bundle=False, install_tables=False)
                except IntegrityError as e:
                    self.database.session.rollback()
                    self.logger.error("Partition is already in Library.: {} ".format(e.message))

                # Attach the partition into the bundle, and return both.
                bundle.partition = partition

        finally:
            if bundle:
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

    def resolve(self, ref, location = None):

        if isinstance(ref, Identity):
            ref = ref.vid

        resolver = self.resolver

        ip, ident = resolver.resolve_ref_one(ref, location)

        try:
            if ident and self.source:
                ident.bundle_path = self.source.source_path(ident=ident)
        except ConfigurationError:
            pass  # Warehouse libraries don't have source directories.

        return ident

    def locate(self, ref):
        """Return list of files for a reference, indicating where a file for a partition or dataset is located"""

        if isinstance(ref, Identity):
            ident = ref
        else:
            ident = self.resolve(ref)

        if not ident:
            return None, None

        if self.cache.has(ident.cache_key):
            return ident, self.cache


        for remote in sorted(self.remotes, key = lambda x: x.priority):
            if remote.has(ident.cache_key):
                return ident, remote


        return ident, None

    def locate_one(self,ref):
        """Like locate, but return only the highest priority result, or None if non exists"""

        f = self.locate(ref)

        try:
            return f.pop(0)
        except (AttributeError,IndexError):
            return None


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

        errors = 0

        deps = self.bundle.metadata.dependencies

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

    def push(self, ref=None, cb=None, upstream = None):
        """Push any files marked 'new' to the upstream

        Args:
            file_: If set, push a single file, obtailed from new_files. If not, push all files.

        """
        import time

        what = None
        start = None
        end = None

        if not upstream:
            upstream = self.remotes[0]

        if not upstream:
            raise Exception("Can't push() without defining a upstream. ")

        if ref is not None:

            ip, dsid = self.resolver.resolve_ref_one(ref)

            if not dsid:
                raise Exception("Didn't get id from database for ref: {}".format(ref))

            if dsid.partition:
                identity = dsid.partition
            else:
                identity = dsid

            try:
                file_ = self.files.query.installed.ref(identity.vid).one
            except:
                print 'Failed for ', identity.vid
                raise

            md = identity.to_meta(file=file_.path)

            if upstream.has(identity.cache_key):
                if cb: cb('Has', md, 0)
                what = 'has'
                file_.state = 'pushed'

            else:
                start = time.clock()
                if cb: cb('Pushing', md, start)

                upstream.put(file_.path, identity.cache_key, metadata=md)
                end = time.clock()
                dt = end - start
                file_.state = 'pushed'
                if cb: cb('Pushed', md, dt)
                what = 'pushed'



            self.database.session.merge(file_)
            self.database.commit()

            return what, start, end, md['size'] if 'size' in md else None

        else:

            for file_ in self.new_files:
                self.push(file_.ref, cb=cb, upstream=upstream)

            try:
                upstream.store_list()
            except AttributeError:
                pass

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

    def sync_library(self, clean = False):
        '''Rebuild the database from the bundles that are already installed
        in the repository cache'''

        from ..orm import Dataset
        from .files import Files
        from database import ROOT_CONFIG_NAME_V
        from ..dbexceptions import ConflictError

        assert Files.TYPE.BUNDLE == Dataset.LOCATION.LIBRARY
        assert Files.TYPE.PARTITION == Dataset.LOCATION.PARTITION

        if clean:
            self.files.query.type(Dataset.LOCATION.REMOTE).delete()

        bundles = []

        self.logger.info("Rebuilding from dir {}".format(self.cache.cache_dir))

        for r, d, f in os.walk(self.cache.cache_dir, topdown=True):

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

                    b = None
                    try:
                        b = self._create_bundle( path_)

                        # The path check above is wrong sometime when there are symlinks
                        if self.files.query.type(Files.TYPE.BUNDLE).ref(b.identity.vid).one_maybe and self.get(bundle.identity.vid):
                            continue

                        if b.identity.is_bundle:
                            self.logger.info("Queing: {} from {}".format(b.identity.vname, file_))
                            bundles.append(b)
                    except NotFoundError:
                        # Probably a partition, not a bundle.
                        pass
                    except Exception as e:
                        self.logger.error('Failed to process {}, {} : {} '.format(file_, path_, e))
                        raise
                    finally:
                        if b:
                            b.close()

        bundles = sorted(bundles, key = lambda b: b.partitions.count)

        for bundle in bundles:

            self.logger.info('Installing: {} '.format(bundle.identity.vname))

            try:

                try:
                    self.database.install_bundle(bundle, install_partitions=False, commit=True)
                    installed = True
                except ConflictError:
                    installed = False

                self.files.install_bundle_file(bundle, self.cache, commit=True)

                for p in bundle.partitions:
                    self.logger.info('            {} '.format(p.identity.vname))

                    if installed:
                        self.database.install_partition(bundle, p, commit='collect')

                    self.files.install_partition_file(p, self.cache, commit='collect')


                self.files.insert_collection()

                if installed:
                    self.database.insert_partition_collection()

                self.database.commit()
                self.database.close()
                bundle.close()

            except Exception as e:
                self.logger.error('Failed to install bundle {}: {}'.format(bundle.identity.vname, e.message))
                raise
                continue

        return bundles

    def sync_remotes(self, remotes=None, clean = False):
        from ..orm import Dataset
        from sqlalchemy.exc import IntegrityError

        if clean:
            self.files.query.type(Dataset.LOCATION.REMOTE).delete()

        if not remotes:
            remotes = self.remotes

        if not remotes:
            return



        for remote in remotes:

            all_keys = [ f.path for f  in self.files.query.type(Dataset.LOCATION.REMOTE).group(remote.repo_id).all ]

            for cache_key in remote.list().keys():

                if cache_key in all_keys:
                    continue

                if self.cache.has(cache_key):# This is just for reporting.
                    self.logger.info("Remote {} has: {}".format(remote.repo_id, cache_key))
                else:
                    self.logger.info("Remote {} sync: {}".format(remote.repo_id, cache_key))


                b = self._get_bundle_by_cache_key(cache_key)

                path, installed =  self.put_bundle(b, install_partitions=False, commit=True)

                try:
                    self.files.install_remote_bundle(b.identity, remote, {}, commit=True)
                except IntegrityError:
                    b.close()
                    continue

                for p in b.partitions:
                    if  installed:
                        self.database.install_partition(b, p, commit='collect')
                    if self.files.install_remote_partition(p.identity, remote, {}, commit = 'collect'):
                        self.logger.info("    + {}".format(p.identity.name))
                    else:
                        self.logger.info("    = {}".format(p.identity.name))

                try:
                    self.files.insert_collection()
                except IntegrityError:
                    b.close()
                    continue

                if installed:
                    self.database.insert_partition_collection()


                self.database.commit()
                self.database.close()
                b.close()


    def sync_source(self, clean=False):
        '''Rebuild the database from the bundles that are already installed
        in the repository cache'''
        from ..dbexceptions import ConflictError
        from sqlalchemy.exc import IntegrityError

        for ident in self.source._dir_list().values():
            try:
                bundle = self.source.bundle(ident.bundle_path)

                self.logger.info('Installing: {} '.format(bundle.identity.vname))
                try:
                    self.database.install_dataset_identity(bundle.identity)
                    self.database.commit()
                except ConflictError:
                    self.database.rollback()

                    pass

                try:
                    self.files.install_bundle_source(bundle, self.source, commit=True)
                    self.database.commit()
                except IntegrityError:
                    self.database.rollback()
                    pass

            except Exception as e:
                raise
                self.logger.error("Failed to sync: bundle_path={} : {} ".format(ident.bundle_path, e.message))

        self.database.commit()
        bundle.close()

    @property
    def remotes(self):
        from ..cache import new_cache

        if not self._remotes:
            return None

        return self._remotes

    @property
    def info(self):
        return """
------ Library {name} ------
Database: {database}
Cache:    {cache}
Remotes:  {remotes}
        """.format(name=self.name, database=self.database.dsn,
                   cache=self.cache, remotes='\n          '.join([ str(x) for x in self.remotes]) if self.remotes else '')

    @property
    def dict(self):
        return dict(name=str(self.name),
                    database=str(self.database.dsn),
                    cache=str(self.cache),
                    remotes=[ str(r) for r in self.remotes]) if self.remotes else []


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

        if len(qc.to_dict().items()) < 1:
            raise ValueError("Malformed find command")

        vids = set()
        for entry in self.l.find(qc):
            vids.add(entry['identity']['vid'])

        for vid in vids:
            ident = self.l.resolve(vid)
            idents.append(ident)

        return IdentitySet(idents, fields=fields)

    def get(self, ref, force=False, cb=None):
        return self.l.get( ref=ref, force=force, cb=cb)

    @property
    def info(self):
        return self.l.info

    def _repr_html_(self):
        return self.info.replace('\n','<br/>')

