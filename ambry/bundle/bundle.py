"""The Bundle object is the root object for a bundle, which includes acessors
for partitions, schema, and the filesystem

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import logging
import os
from ..filesystem import BundleFilesystem
from ..schema import Schema
from ..partitions import Partitions
from ..util import get_logger, clear_logger
from ..dbexceptions import ConfigurationError, ProcessError
from ..util import memoize


class Bundle(object):
    """Represents a bundle, including all configuration
    and top level operations. """

    _logger = None
    log_level = logging.INFO
    _database = None
    _schema = None
    _partitions = None
    _library = None
    _identity = None
    _repository = None
    # Needed in LibraryDbBundle to  disambiguate multiple datasets
    _dataset_id = None
    _library_name = 'default'

    def __init__(self, logger=None):
        """
        """

        # This bit of wackiness allows the var(self.run_args) code
        # to work when there have been no artgs parsed.
        class null_args(object):
            none = None
            multi = False
            test = False

        self.run_args = vars(null_args())


    def __del__(self):
        self.close()

    def close(self):

        self.partitions.close()

        if self._database:
            self.logger.debug("Closing bundle: {}".format(self.identity.sname))
            self._database.session.commit()
            self._database.close()

    @property
    def log_file(self):

        return self.path+".log"

    @property
    def logger(self):
        import sys
        if not self._logger:

            try:
                ident = self.identity
                template = "%(levelname)s " + ident.sname + " %(message)s"

                if self.run_args.multi > 1:
                    template = "%(levelname)s " + ident.sname + " %(process)s %(message)s"

            except:
                template = "%(message)s"

            if not os.path.isdir(os.path.dirname(self.log_file)):
                os.makedirs(os.path.dirname(self.log_file))

            self._logger = get_logger(__name__, template=template, stream= sys.stdout, file_name = self.log_file )

            self._logger.setLevel(self.log_level)

        return self._logger

    def clear_logger(self):
        # Force the logger to re-open, which will re-create the file that just got deleted
        self._logger = None
        clear_logger(__name__)

    @property
    def database(self):
        from ..dbexceptions import DatabaseMissingError

        if self._database is None:
            raise DatabaseMissingError('Database has not been set')

        return self._database

    @property
    def schema(self):
        if self._schema is None:
            self._schema = Schema(self)

        return self._schema

    @property
    def partitions(self):
        if self._partitions is None:
            self._partitions = Partitions(self)

        return self._partitions




    @property
    def repository(self):
        """Return a repository object """
        from ..repository import Repository  # @UnresolvedImport

        if not self._repository:
            repo_name = 'default'
            self._repository = Repository(self, repo_name)

        return self._repository

    def get_dataset(self):
        raise NotImplementedError()

    @property
    def dataset(self):
        """Return the dataset"""
        return self.get_dataset()


    def set_value(self, group, key, value):
        from ambry.orm import Config as SAConfig

        with self.session as s:
            return self.database.set_config_value(self.dataset.vid, group, key, value, session=s)



    def get_value(self, group, key, default=None):
        v = self.database.get_config_value(self.dataset.vid, group, key)

        if v is None and default is not None:
            return default
        else:
            return v

    def get_value_group(self, group):
        return self.database.get_config_value_group(self.dataset.vid, group)


    def _dep_cb(self, library, key, name, resolved_bundle):
        """A callback that is called when the library resolves a dependency.
        It stores the resolved dependency into the bundle database"""

        if resolved_bundle.partition:
            ident = resolved_bundle.partition.identity
        else:
            ident = resolved_bundle.identity

        if not self.database.is_empty():
            with self.session:
                self.db_config.set_value('rdep', key, ident.dict)

    @property
    def library(self):
        """Return the library set for the bundle, or
        local library from get_library() if one was not set. """

        from ..library import new_library

        if self._library:
            l = self._library
        else:
            l = new_library(self.config.library(self._library_name))

        l.logger = self.logger
        l.database.logger = self.logger
        l.bundle = self
        l.dep_cb = self._dep_cb

        return l

    @library.setter
    def library(self, value):
        self._library = value

    @property
    def library_name(self):
        return self._library_name

    @library_name.setter
    def library_name(self, value):
        self._library_name = value
        self._library = None

    @property
    def path(self):
        """Return the base path for the bundle, usually the path to the
        bundle database, but withouth the database extension."""
        raise NotImplementedError("Abstract")

    def sub_dir(self, *args):
        """Return a subdirectory relative to the bundle's database root path
        which based on the path of the database. For paths relative to the
        directory of a BuildBundle, use the Filesystem object. """
        return os.path.join(self.path, *args)

    def query(self, *args, **kwargs):
        """Convience function for self.database.connection.execute()"""
        return self.database.query(*args, **kwargs)

    def log(self, message, **kwargs):
        """Log the messsage"""
        self.logger.info(message)

    def error(self, message, **kwargs):
        """Log an error messsage"""
        self.logger.error(message)

    def warn(self, message, **kwargs):
        """Log an error messsage"""
        self.logger.warn(message)

    def fatal(self, message, **kwargs):
        """Log a fata messsage and exit"""
        import sys
        self.logger.fatal(message)
        sys.stderr.flush()
        if self.exit_on_fatal:
            sys.exit(1)
        else:
            from ..dbexceptions import FatalError
            raise FatalError(message)

    def _build_info(self):
        from collections import OrderedDict

        process = self.config.process
        return OrderedDict(
            created=process.get(
                'dbcreated',
                ''),
            prepared=process.get(
                'prepared',
                ''),
            built=process.get(
                'built',
                ''),
            build_time=str(
                round(
                    float(
                        process['buildtime']),
                    2)) +
            's' if process.get(
                'buildtime',
                False) else '')

    def _info(self, identity=None):
        """Return a nested, ordered dict  of information about the bundle. """
        from collections import OrderedDict
        d = OrderedDict()

        d['identity'] = identity if identity else self.identity._info()
        d['locations'] = str(self.identity.locations).strip()

        return d

    @property
    def info(self):
        """Display useful information about the bundle"""

        out = []

        key_lengths = set()
        d = self._info()
        for k, v in d.items():
            key_lengths.add(len(k))
            if isinstance(v, dict):
                for k, _ in v.items():
                    key_lengths.add(len(k))
        kw = max(key_lengths) + 4

        f1 = "{:<" + str(kw) + "s}: {}"
        f2 = "{:>" + str(kw) + "s}: {}"

        for k, v in d.items():

            if isinstance(v, dict):
                out.append("{}".format(k.title()))
                for k, v2 in v.items():
                    out.append(f2.format(k.title(), v2))
            else:
                out.append(f1.format(k.title(), v))

        return '\n'.join(out)

    def _repr_html_(self):
        out = []
        for k, v in self._info().items():

            if isinstance(v, dict):
                out.append(
                    "<tr><td><strong>{}</strong></td><td></td></tr>".format(k.title()))
                for k, v2 in v.items():
                    out.append(
                        '<tr><td align="right">{}</td><td>{}</td></tr>'.format(k.title(), v2))
            else:
                out.append(
                    '<tr><td align="left">{}</td><td>{}</td></tr>'.format(k.title(), v))

        return "<table>\n" + "\n".join(out) + "\n</table>"


class DbBundle(Bundle):

    def __init__(self, database_file, logger=None):
        """Initialize a db and all of its sub-components.

        If it does not exist, creates the db database and initializes the
        Dataset record and Config records from the db.yaml file. Through the
        config object, will trigger a re-load of the db.yaml file if it
        has changed.

        Order of operations is:
            Create db.db if it does not exist
        """
        from ..database.sqlite import SqliteBundleDatabase  # @UnresolvedImport

        super(DbBundle, self).__init__(logger=logger)

        self.database_file = database_file

        self._database = SqliteBundleDatabase(self, database_file)

        # Set in Library.get() and Library.find() when the user requests a
        # partition.
        self.partition = None

    @property
    def path(self):
        base, _ = os.path.splitext(self.database_file)
        return base

    def sub_path(self, *args):
        """For constructing paths to partitions"""
        return os.path.join(self.path, *args)

    @property
    def identity(self):
        """Return an identity object. """
        from ..identity import Identity, LocationRef

        if not self._identity:
            self._identity = self.get_dataset().identity
            self._identity.locations.set(LocationRef.LOCATION.LIBRARY)

        return self._identity

    def get_dataset(self):
        """Return the dataset
        """

        from sqlalchemy.exc import OperationalError
        from ..dbexceptions import NotFoundError

        from ambry.orm import Dataset

        try:
            ds = (self.database.session.query(Dataset).one())

            if not ds:
                raise NotFoundError(
                    "No dataset record found. Probably not a bundle: '{}'" .format(
                        self.path))

            return ds

        except OperationalError:
            raise NotFoundError(
                "No dataset record found. Probably not a bundle: '{}'" .format(
                    self.path))
        except Exception as e:
            from ..util import get_logger
            # self.logger can get caught in a recursion loop
            logger = get_logger(__name__)
            logger.error(
                "Failed to get dataset: {}; {}".format(
                    e.message,
                    self.database.dsn))
            raise

    def _info(self, identity=None):
        """Return a nested, ordered dict  of information about the bundle. """
        from collections import OrderedDict
        d = super(DbBundle, self)._info(identity)

        d['source'] = OrderedDict(
            db=self.database.path
        )

        d['source'].update(self._build_info())

        d['partitions'] = self.partitions.count

        return d


class LibraryDbBundle(Bundle):

    """A database bundle that is built in place from the data in a library """

    def __init__(self, database, dataset_id, logger=None):
        """Initialize a db and all of its sub-components.

        """

        super(LibraryDbBundle, self).__init__(logger=logger)

        self._dataset_id = dataset_id
        self._database = database

        # Set in Library.get() and Library.find() when the user requests a
        # partition. s
        self.partition = None

    def get_dataset(self):
        """Return the dataset
        """
        from sqlalchemy.orm.exc import NoResultFound
        from sqlalchemy.exc import OperationalError
        from ..dbexceptions import NotFoundError

        from ambry.orm import Dataset

        try:
            return (self.database.session.query(Dataset)
                    .filter(Dataset.location == Dataset.LOCATION.LIBRARY)
                    .filter(Dataset.vid == self._dataset_id).one())

        except NoResultFound:
            from ..dbexceptions import NotFoundError

            raise NotFoundError("Failed to find dataset for id {} in {} "
                                .format(self._dataset_id, self.database.dsn))

        except OperationalError:
            raise NotFoundError(
                "No dataset record found. Probably not a bundle: '{}'" .format(
                    self.path))

        except Exception as e:
            from ..util import get_logger
            # self.logger can get caught in a recursion loop
            logger = get_logger(__name__)
            logger.error(
                "Failed to get dataset: {}; {}".format(
                    e.message,
                    self.database.dsn))
            raise

    @property
    def path(self):
        raise NotImplemented()

    def sub_path(self, *args):
        """For constructing paths to partitions"""
        raise NotImplemented()

    @property
    def identity(self):
        """Return an identity object. """
        from ..identity import Identity, LocationRef

        if not self._identity:
            self._identity = self.get_dataset().identity
            self._identity.locations.set(LocationRef.LOCATION.LIBRARY)

        return self._identity

    def _info(self, identity=None):
        """Return a nested, ordered dict  of information about the bundle. """
        from collections import OrderedDict
        d = super(LibraryDbBundle, self)._info(identity)

        d['source'] = OrderedDict(
            db=self.database.path
        )

        d['source'].update(self._build_info())

        d['partitions'] = self.partitions.count

        return d


class BuildBundle(Bundle):

    """A bundle class for building bundle files. Uses the bundle.yaml file for
    identity configuration """

    META_COMPLETE_MARKER = '.meta_complete'
    SCHEMA_FILE = 'schema.csv'
    SCHEMA_REVISED_FILE = 'schema-revised.csv'
    SCHEMA_OLD_FILE = 'schema-old.csv'

    def __init__(self, bundle_dir=None):
        """
        """
        from ..database.sqlite import BundleLockContext
        import os

        super(BuildBundle, self).__init__()

        if bundle_dir is None:
            import inspect, os
            bundle_dir = os.path.abspath(
                os.path.dirname(
                    inspect.getfile(
                        self.__class__)))

        if bundle_dir is None or not os.path.isdir(bundle_dir):
            from ambry.dbexceptions import BundleError
            raise BundleError("BuildBundle must be constructed on a cache. " +
                              str(bundle_dir) + " is not a directory")

        self.bundle_dir = bundle_dir

        self._database = None

        self.filesystem = BundleFilesystem(self, self.bundle_dir)

        import base64
        self.logid = base64.urlsafe_b64encode(os.urandom(6))
        self.ptick_count = 0

        # Library for the bundle
        lib_dir = self.filesystem.path('lib')
        if os.path.exists(lib_dir):
            import sys
            sys.path.append(lib_dir)

        self._build_time = None
        self._update_time = None

        self.exit_on_fatal = True

        self._bundle_lock_context = BundleLockContext(self)


    def get_dataset(self):
        """Return the dataset
        """
        from sqlalchemy.exc import OperationalError
        from ..dbexceptions import NotFoundError

        from ambry.orm import Dataset

        try:
            return self.database.session.query(Dataset).one()

        except OperationalError:
            raise NotFoundError(
                "No dataset record found. Probably not a bundle: '{}'" .format(
                    self.path))
        except Exception as e:
            from ..util import get_logger
            # self.logger can get caught in a recursion loop
            logger = get_logger(__name__)
            logger.error(
                "Failed to get dataset: {}; {}".format(
                    e.message,
                    self.database.dsn))
            raise

    @property
    def build_dir(self):

        try:
            cache = self.filesystem.get_cache_by_name('build')
            return cache.cache_dir
        except ConfigurationError:
            return self.filesystem.path(self.filesystem.BUILD_DIR)

    @property
    def path(self):
        return os.path.join(self.build_dir, self.identity.path)

    @property
    def db_path(self):
        return self.path + '.db'

    def sub_path(self, *args):
        """For constructing paths to partitions"""

        return os.path.join(self.build_dir, self.identity.path, *args)

    @property
    def database(self):
        from ..database.sqlite import BuildBundleDb  # @UnresolvedImport

        if self._database is None:
            self._database = BuildBundleDb(self, self.db_path)

        return self._database

    @property
    def session(self):
        return self._bundle_lock_context

    @property
    def has_session(self):
        return self.database.has_session


    @property
    @memoize
    def metadata(self):
        from ambry.bundle.meta import Top
        return Top(path=self.bundle_dir)


    @property
    @memoize
    def config(self):
        from ..run import get_runconfig

        return  get_runconfig()

    @property
    def db_config(self):
        raise NotImplementedError()
        return BundleDbConfig(self, self.database)

    @property
    def identity(self):
        """Return an identity object. """
        from ..identity import Identity, Name, ObjectNumber, LocationRef


        if not self._identity:
            try:
                names = self.metadata.names.items()
                idents = self.metadata.identity.items()

            except KeyError as e:
                raise ConfigurationError("Bad bundle config in: {}: {} ".format(self.bundle_dir, e))
            except AttributeError:
                raise AttributeError(
                    "Failed to get required sections of config. " +
                    "\nconfig_source = {}\n".format(
                        self.config.source_ref))

            self._identity = Identity.from_dict(dict(names + idents))
            self._identity.locations.set(LocationRef.LOCATION.SOURCE)

        return self._identity

    def update_configuration(self, rewrite_database=True):
        from ..dbexceptions import DatabaseError
        # Re-writes the bundle.yaml file, with updates to the identity and partitions
        # sections.
        from ..dbexceptions import DatabaseMissingError

        md = self.metadata
        md.load_all()

        if len(md.errors) > 0:
            self.error("Metadata errors in {}".format(md._path))
            for k,v in md.errors.items():
                self.error("    {} = {}".format('.'.join([x for x in k if x]), v))
            raise Exception("Metadata errors")

        md.identity = self.identity.ident_dict
        md.names = self.identity.names_dict

        # Partitions is hanging around in some old bundles
        if 'partitions' in md._term_values:
            del md._term_values['partitions']

        # Ensure there is an entry for every revision, if only to nag the maintainer to fill it in.
        for i in range(1, md.identity.revision+1):
            md.versions[i]

            if i == md.identity.revision:
                md.versions[i].version = md.identity.version

        md.write_to_dir(write_all=True)


        # Reload some of the values from bundle.yaml into the database
        # configuration

        if rewrite_database:
            odep_set = False
            if self.database.exists():

                if self.config.build.get('dependencies'):
                    for k, v in self.config.build.get('dependencies').items():
                        self.set_value('odep', k, v)
                        odep_set = True


                self.database.rewrite_dataset()

    @property
    def sources(self):
        """Return a dictionary of sources from the build.sources configuration key"""

        if not self.config.group('build'):
            raise ConfigurationError(
                "Configuration does not have 'build' group")
        if not self.config.group('build').get('sources', None):
            raise ConfigurationError(
                "Configuration does not have 'build.sources' group")

        d = {}
        for k in self.config.build.sources.keys():
            d[k] = self.source(k) # Enforce structure requirements

    def source(self, name):
        """Return a source record with the given name, from the build.sources configuration
        value.

        This will return the whole record, which may have subcomponents. If the record is define with only a
        scalar string, it will be reutnred in a dict, mapped to the key 'url' """

        s = self.config.build.sources
        d =  s.get(name, None)

        if isinstance(d, basestring):
            return { 'url': d}
        elif isinstance(d, dict):

            if 'url' not in d:
                raise ValueError("Config value build.sources.{} must be either a string or a dict with a 'url' key "
                                .format(name))
            return d
        else:
            raise ValueError("Value assigned to config value build.sources.{} must be a string or dict. Got a {}"
                            .format(name, type(d)))


    def source_url(self, name):
        """Return a source url with the given name, from the build.sources configuration
        value.

        This will return only the url component of the named source"""

        d = self.source(name)

        return d['url']


    @property
    def dependencies(self):
        return self.config.build.get('dependencies')


    def clean(self, clean_meta=False):
        """Remove all files generated by the build process"""
        from ..util import rm_rf

        # Remove partitions

        rm_rf(self.sub_path())
        self.log("Removing {}".format(self.sub_path()))
        # Remove the database

        if self.database.exists():
            self.log("Removing {}".format(self.database.path))
            self.database.delete()

        # Remove the sqlite journal files, if they exists
        files = [
            self.database.path +
            "-wal",
            self.database.path +
            "-shm",
            self.database.path +
            "-journal"]
        for f in files:
            if os.path.exists(f):
                self.log("Removing {}".format(f))
                os.remove(f)

        if clean_meta:
            mf = self.filesystem.meta_path(self.META_COMPLETE_MARKER)
            if os.path.exists(mf):
                self.log("Removing {}".format(mf))
                os.remove(mf)

        ed = self.filesystem.path('extracts')
        if os.path.exists(ed):
            self.log("Removing {}".format(ed))
            rm_rf(ed)

        if os.path.exists(self.log_file):
            self.log("Removing {}".format(self.log_file))
            os.remove(self.log_file)

        # Should check for a shared download file -- specified
        # as part of the library; Don't delete that.
        # if not self.cache_downloads :
        #    self.rm_rf(self.filesystem.downloads_path())

        self.set_build_state( 'cleaned')

        self.close()

        self.clear_logger()

    def set_build_state(self, state):
        from datetime import datetime

        if state not in ('cleaned','meta'):
            self.set_value('process', 'state', state)
            self.set_value('process', 'last', datetime.now().isoformat())

        if self.library.source:
            self.library.source.set_bundle_state(self.identity, state)


    def progress(self, message):
        """print message to terminal, in place"""
        print 'PRG: ', message

    def ptick(self, message):
        """Writes a tick to the stdout, without a space or newline"""
        import sys
        sys.stdout.write(message)
        sys.stdout.flush()

        self.ptick_count += len(message)

        if self.ptick_count > 72:
            sys.stdout.write("\n")
            self.ptick_count = 0

    def init_log_rate(self, N=None, message='', print_rate=None):
        from ..util import init_log_rate as ilr

        return ilr(self.log, N=N, message=message, print_rate=print_rate)

    # Prepare is run before building, part of the devel process.
    def pre_meta(self):
        """Skips the meta stage if the :class:.`META_COMPLETE_MARKER` file already exists"""


        mf = self.filesystem.meta_path(self.META_COMPLETE_MARKER)

        if os.path.exists(mf) and not self.run_args.get('clean', None):
            self.log("Meta information already generated")
            #raise ProcessError("Bundle has already been prepared")
            return False


        return True

    def meta(self):
        return True

    def post_meta(self):
        """Create the :class:.`META_COMPLETE_MARKER` meta marker so we don't run the meta process again"""
        import datetime


        mf = self.filesystem.meta_path(self.META_COMPLETE_MARKER)
        with open(mf, 'w+') as f:
            f.write(str(datetime.datetime.now()))


        return True

    # Prepare is run before building, part of the devel process.

    def pre_prepare(self):

        self.log('---- Pre-Prepare ----')

        if self.config.build.get('requirements', False):
            from ..util.packages import install
            import sys
            import imp

            python_dir = self.config.python_dir()

            if not python_dir:
                raise ConfigurationError(
                    "Can't install python requirements without a configuration item for filesystems.python")

            if not os.path.exists(python_dir):
                os.makedirs(python_dir)

            sys.path.append(python_dir)

            self.log("Installing required packages in {}".format(python_dir))

            for k, v in self.config.build.requirements.items():

                try:
                    imp.find_module(k)
                    self.log(
                        "Required package already installed: {}->{}".format(k, v))
                except ImportError:
                    self.log(
                        "Installing required package: {}->{}".format(k, v))
                    install(python_dir, k, v)


        if self.is_prepared:
            self.log("Bundle has already been prepared")
            #raise ProcessError("Bundle has already been prepared")

            return False

        try:
            b = self.library.resolve(self.identity.id_)

            if b and b.on.revision >= self.identity.on.revision:
                self.fatal(
                    ("Can't build this version. Library {} has version {} of {}"
                     " which is less than or equal this version {}") .format(
                        self.library.database.dsn,
                        b.on.revision,
                        b.fqname,
                        self.identity.on.revision))
                return False

        except Exception as e:
            raise
            self.error(
                "Error in consulting library: {}\nException: {}".format(
                    self.library.info,
                    e.message))

        return True

    def _prepare_load_schema(self):

        sf = self.filesystem.path(
            self.config.build.get(
                'schema_file',
                'meta/' +
                self.SCHEMA_FILE))
        if os.path.exists(sf):
            with open(sf, 'rbU') as f:
                self.log("Loading schema from file: {}".format(sf))
                self.schema.clean()
                with self.session:
                    warnings, errors = self.schema.schema_from_file(f)

                for title, s, f in (("Errors", errors, self.error), ("Warnings", warnings, self.warn)):
                    if s:
                        self.log("----- Schema {} ".format(title))
                        for table_name, column_name, message in s:
                            f("{:20s} {}".format(
                                "{}.{}".format(table_name if table_name else '', column_name if column_name else ''),
                                message))

                if errors:
                    self.fatal("Schema load filed. Exiting")
        else:
            self.log("No schema file ('{}') not loading schema".format(sf))

    def prepare(self):
        from ..dbexceptions import NotFoundError


        # with self.session: # This will create the database if it doesn't
        # exist, but it will be empty
        if not self.database.exists():
            self.log("Creating bundle database")
            try:
                self.database.create()
            except:
                self.error("Failed to create database: {} ".format(self.database.dsn))
                raise
        else:
            self.log("Bundle database already exists")

        try:
            self.library.check_dependencies()
        except NotFoundError as e:
            self.fatal(e.message)

        if self.run_args and self.run_args.get('rebuild', False):
            with self.session:
                self.rebuild_schema()
        else:
            self._prepare_load_schema()


        return True

    def rebuild_schema(self):
        sf = self.filesystem.path(
            self.config.build.get(
                'schema_file',
                'meta/schema.csv'))
        with open(sf, 'rbU') as f:

            partitions = [p.identity for p in self.partitions.all]
            self.schema.clean()
            self.schema.schema_from_file(f)

            for p in partitions:
                self.partitions.new_db_partition(p)

    def _revise_schema(self):
        """Write the schema from the database back to a file. If the schema template exists, overwrite the
        main schema file. If it does not exist, use the revised file


        """

        self.update_configuration()

        sf_out = self.filesystem.path('meta', self.SCHEMA_REVISED_FILE)

        # Need to expire the unmanaged cache, or the regeneration of the schema may
        # use the cached schema object rather than the ones we just updated, if the schem objects
        # have alread been loaded.
        self.database.session.expire_all()

        with open(sf_out, 'w') as f:
            self.schema.as_csv(f)

    def post_prepare(self):
        """Set a marker in the database that it is already prepared. """
        from datetime import datetime

        with self.session:
            self.set_value('process', 'prepared', datetime.now().isoformat())

            self._revise_schema()
            
        self.set_build_state( 'prepared')

        self.update_configuration()

        return True

    @property
    def is_prepared(self):
        return (self.database.exists()
                and not self.run_args.get('rebuild', False)
                and self.get_value('process', 'prepared', False))

    def prepare_main(self):
        """This is the methods that is actually called in do_prepare; it dispatched to
        developer created prepare() methods"""
        return self.prepare()

    def do_prepare(self):
        """This method runs pre_, main and post_ prepare methods. """

        if self.pre_prepare():
            self.log("---- Preparing ----")
            if self.prepare_main():
                self.post_prepare()
                self.log("---- Done Preparing ----")
            else:
                self.log("---- Prepare exited with failure ----")
                r = False
        else:
            self.log("---- Skipping prepare ---- ")

        r = True

        return r

    # Build the final package
    def pre_build(self):
        from time import time
        import sys

        if not self.database.exists():
            raise ProcessError(
                "Database does not exist yet. Was the 'prepare' step run?")

        if self.is_built and not self.run_args.get('force', False):
            self.log(
                "Bundle is already built. Skipping  ( Use --clean  or --force to force build ) ")
            return False

        with self.session:
            if not self.get_value('process', 'prepared', False):
                raise ProcessError("Build called before prepare completed")

            self._build_time = time()

        python_dir = self.config.python_dir()

        if python_dir and python_dir not in sys.path:
            sys.path.append(python_dir)

        self.close()


        return True

    def build(self):
        return False

    def post_build(self):
        """After the build, update the configuration with the time required for the build,
        then save the schema back to the tables, if it was revised during the build.  """
        from datetime import datetime
        from time import time
        import shutil

        with self.session:
            self.set_value('process','built',datetime.now().isoformat())
            self.set_value( 'process','buildtime',time() -  self._build_time)
            self.update_configuration()

            self._revise_schema()

        # Some original import files don't have a schema, particularly
        # imported Shapefiles
        if os.path.exists(self.filesystem.path('meta', self.SCHEMA_FILE)):
            shutil.copy(
                self.filesystem.path('meta', self.SCHEMA_FILE),
                self.filesystem.path('meta', self.SCHEMA_OLD_FILE)
            )

            shutil.copy(
                self.filesystem.path('meta', self.SCHEMA_REVISED_FILE),
                self.filesystem.path('meta', self.SCHEMA_FILE)
            )

        self.post_build_write_stats()

        self.post_build_write_partitions()

        self.post_build_write_config()

        self.set_value('process', 'last', datetime.now().isoformat())
        self.set_build_state( 'built')

        return True

    def post_build_write_stats(self):
        from sqlalchemy.exc import OperationalError

        # Create stat entries for all of the partitions.
        for p in self.partitions:
            try:
                from ..partition.sqlite import SqlitePartition
                from ..partition.geo import GeoPartition
                self.log("Writting stats for: {}".format(p.identity.name))
                if isinstance(p, (SqlitePartition, GeoPartition)):
                    self.log("Writting stats for: {}".format(p.identity.name))
                    p.write_stats()
                else:
                    self.log(
                        "Skipping stats for non db partition: {}".format(
                            p.identity.name))

            except NotImplementedError:
                self.log(
                    "Can't write stats (unimplemented) for partition: {}".format(
                        p.identity.name))
            except ConfigurationError as e:
                self.error(e.message)
            except OperationalError as e:
                self.error(
                    "Failed to write stats for partition {}: {}".format(
                        p.identity.name,
                        e.message))
                raise
            p.close() # Or, will run out of files/connections and get operational error

    def post_build_write_partitions(self):
        """Write a list of partitions to the meta directory"""
        import yaml

        if self.database.exists():
            partitions = [p.identity.name.partital_dict for p in self.partitions]

        else:
            partitions = []


        fn = self.filesystem.path('meta','partitions.yaml')

        with open(fn,'w') as f:
            yaml.safe_dump(partitions, f,default_flow_style=False, indent=4, encoding='utf-8')

    def post_build_write_config(self):
        '''Write  the config into the database'''
        exclude_keys = ('names','identity')

        self.metadata.load_all()

        for row in self.metadata.rows:

            if row[0][0] in exclude_keys:
                continue

            k = '.'.join([str(x) for x in row[0] if x])

            self.set_value('config', k, row[1])

    @property
    def is_built(self):
        """Return True is the bundle has been built"""

        if not self.database.exists():
            return False

        v = self.get_value('process', 'built', False)

        return bool(v)

    def build_main(self):
        """This is the methods that is actually called in do_prepare; it dispatched to
        developer created prepare() methods"""
        self.set_build_state('building')
        return self.build()

    def do_build(self):

        if not self.is_prepared:
            self.do_prepare()

        if self.pre_build():
            self.log("---- Build ---")
            if self.build_main():
                self.post_build()
                self.log("---- Done Building ---")
                r = True
            else:
                self.log("---- Build exited with failure ---")
                r = False
        else:
            self.log("---- Skipping Build ---- ")
            r = False

        return r

    # Update is like build, but calls into an earlier version of the package.
    def pre_update(self):
        from time import time

        if not self.database.exists():
            raise ProcessError(
                "Database does not exist yet. Was the 'prepare' step run?")

        if not self.get_value('process', 'prepared'):
            raise ProcessError("Update called before prepare completed")

        self._update_time = time()

        return True

    def update(self):
        return False

    def post_update(self):
        from datetime import datetime
        from time import time
        with self.session:
            self.set_value('process','updated',datetime.now().isoformat())
            self.set_value('process','updatetime',time() - self._update_time)
            self.update_configuration()


        return True

    # Submit the package to the library

    def pre_install(self):

        with self.session:
            self.update_configuration()

        return True

    def install(self, library_name=None, delete=False, force=True):
        """Install the bundle and all partitions in the default library"""

        import ambry.library

        force = self.run_args.get('force', force)

        with self.session:

            #library_name = self.run_args.get('library', 'default') if library_name is None else 'default'
            #library_name = library_name if library_name else 'default'
            #library = ambry.library.new_library(self.config.library(library_name), reset=True)
            library = self.library

            self.log(
                "Install   {} to  library {}".format(
                    self.identity.name,
                    library.database.dsn))
            dest = library.put_bundle(
                self,
                force=force,
                install_partitions=False)
            self.log("Installed {}".format(dest[1]))

            skips = self.config.group('build').get('skipinstall', [])

            for partition in self.partitions:

                if not os.path.exists(partition.database.path):
                    self.log(
                        "{} File does not exist, skipping".format(
                            partition.database.path))
                    continue

                if partition.name in skips:
                    self.log('Skipping {}'.format(partition.name))
                else:
                    self.log("Install   {}".format(partition.name))
                    dest = library.put_partition(self, partition, force=force)
                    self.log("Installed {}".format(dest[1]))

                    if delete:
                        os.remove(partition.database.path)
                        self.log("Deleted {}".format(partition.database.path))

        return True

    def post_install(self):
        from datetime import datetime
        self.set_value('process','installed', datetime.now().isoformat())
        
        self.set_build_state( 'installed')

        return True

    # Submit the package to the repository

    def pre_submit(self):
        with self.session:
            self.update_configuration()
        return True

    # Submit the package to the repository
    def submit(self):

        self.repository.submit(
            root=self.run_args.get('name'),
            force=self.run_args.get('force'),
            repo=self.run_args.get('repo'))
        return True

    def post_submit(self):
        from datetime import datetime
        self.set_value('process','submitted', datetime.now().isoformat())

        return True

    # Submit the package to the repository

    def pre_extract(self):
        return True

    # Submit the package to the repository
    def extract(self):
        self.repository.extract(
            root=self.run_args.get('name'),
            force=self.run_args.get('force'))
        return True

    def post_extract(self):
        from datetime import datetime
        self.set_value('process','extracted',datetime.now().isoformat())

        return True

    def repopulate(self):
        """Pull bundle files from the library back into the working directory"""
        import shutil

        self.log('---- Repopulate ----')

        b = self.library.get(self.identity.name)

        self.log(
            'Copy bundle from {} to {} '.format(
                b.database.path,
                self.database.path))

        if not os.path.isdir(os.path.dirname(self.database.path)):
            os.makedirs(os.path.dirname(self.database.path))

        shutil.copy(b.database.path, self.database.path)

        # Restart with the new bundle database.
        newb = BuildBundle(self.bundle_dir)

        for newp in newb.partitions:
            self.log('Copy partition: {}'.format(newp.identity.name))

            b = self.library.get(newp.identity.vname)

            dir_ = os.path.dirname(newp.database.path)

            if not os.path.isdir(dir_):
                os.makedirs(dir_)

            shutil.copy(b.partition.database.path, newp.database.path)

    def set_args(self, args):

        from ..util import AttrDict

        self.run_args = AttrDict(vars(args))

    def run_mp(self, method, arg_sets):
        from ..run import mp_run
        from multiprocessing import Pool, cpu_count

        if len(arg_sets) == 0:
            return

        # Argsets should be tuples, but for one ag functions, the
        # caller may pass in a scalar, which we have to convert.
        if not isinstance(arg_sets[0], (list, tuple)):
            arg_sets = ((arg,) for arg in arg_sets)

        n = int(self.run_args.get('multi'))

        if n == 0:
            n = cpu_count()

        if n == 1:
            self.log(
                "Requested MP run, but for only 1 process; running in process instead")
            for args in arg_sets:
                method(*args)
        else:
            self.log("Multi processor run with {} processes".format(n))

            pool = Pool(n)

            pool.map(mp_run, [(self.bundle_dir, dict(self.run_args), method.__name__, args)
                              for args in arg_sets])

    def _info(self, identity=None):
        """Return a nested, ordered dict  of information about the bundle. """
        from collections import OrderedDict

        d = super(BuildBundle, self)._info(identity)

        d['source'] = OrderedDict(
            bundle=self.bundle_dir
        )

        deps = self.config.build.get('dependencies')
        d['build'] = OrderedDict(
            dependencies=deps if deps else ''
        )

        if self.is_built:
            d['build'].update(self._build_info())

        return d


def new_analysis_bundle(
        source,
        dataset,
        rc_path=None,
        subset=None,
        bspace=None,
        btime=None,
        variation=None,
        revision=1,
        ns_key=None):

    from ..run import get_runconfig
    from ..library import new_library

    rc = get_runconfig(path=rc_path)
    l = new_library(rc.library('default'))

    st = l.source

    repo_dir = rc.filesystem_path('analysis')

    if not repo_dir or not os.path.exists(repo_dir):
        raise ConfigurationError(
            "Must specify a valid directory for configuration item filesystem.analysis."
            "Got: '{}' ".format(repo_dir))

    ab_path = st.new_bundle(
        rc,
        repo_dir,
        type='analysis',
        source=source,
        dataset=dataset,
        subset=subset,
        bspace=bspace,
        btime=btime,
        variation=variation,
        revision=revision,
        throw=False,
        examples=False)

    return AnalysisBundle(ab_path)


class Registrar(object):

    def __init__(self, bundle):
        self.bundle = bundle

    def wrap_phase(self, orig_f, phase):

        def wrapped_phase_main(bundle):
            return orig_f(bundle)

        # This binds the object to the method.
        # http://stackoverflow.com/a/1015405
        setattr(
            self.bundle,
            phase + '_main',
            wrapped_phase_main.__get__(
                self.bundle,
                AnalysisBundle))

        return wrapped_phase_main

    def meta(self, orig_f):
        return self.wrap_phase(orig_f, 'meta')

    def prepare(self, orig_f):
        return self.wrap_phase(orig_f, 'prepare')

    def build(self, orig_f):
        return self.wrap_phase(orig_f, 'build')


class AnalysisBundle(BuildBundle):

    def __init__(self, bundle_path):
        """Initialize the analysis bundle by running the prepare phase and starting the pre-build
        portion of the build phase """
        from ..util import AttrDict
        super(AnalysisBundle, self).__init__(bundle_path)

        self.log_level = logging.CRITICAL

        if not self.config.get("build"):
            self.config.build = AttrDict(
                dependencies=AttrDict()
            )

        self.clean()
        self.prepare()

        self.pre_build()

    @property
    def register(self):
        return Registrar(self)

    def prepare(self):
        return super(AnalysisBundle, self).do_prepare()

    def prepare_main(self):
        """This is the methods that is actually called in do_prepare; it dispatched to
        developer created prepare() methods"""

        from ..dbexceptions import NotFoundError

        # with self.session: # This will create the database if it doesn't
        # exist, but it will be empty
        if not self.database.exists():
            self.log("Creating bundle database")
            self.database.create()

        return True

    def pre_build(self):
        from time import time
        import sys

        if not self.database.exists():
            raise ProcessError(
                "Database does not exist yet. Was the 'prepare' step run?")

        with self.session:
            if not self.get_value('process', 'prepared', False):
                raise ProcessError("Build called before prepare completed")

            self._build_time = time()

        python_dir = self.config.python_dir()

        if python_dir and python_dir not in sys.path:
            sys.path.append(python_dir)

        return True

    def post_build(self):
        self.config.rewrite()
        super(AnalysisBundle, self).post_build()
