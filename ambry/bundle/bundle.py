"""The Bundle object is the root object for a bundle, which includes accessors
for partitions, schema, and the filesystem.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import os
import sys
from time import time
import traceback
from decorator import decorator

from six import string_types, iteritems, u, b

from fs.errors import NoSysPathError

from geoid.civick import GVid
from geoid import NotASummaryName

from ambry.dbexceptions import PhaseError, BuildError, BundleError, FatalError
from ambry.orm import File
import ambry.etl
from ..util import get_logger, Constant

indent = '    '  # Indent for structured log output

BUILD_LOG_FILE = 'log/build_log{}.txt'

def _CaptureException(f, *args, **kwargs):
    """Decorator implementation for capturing exceptions."""
    from ambry.dbexceptions import LoggedException

    b = args[0]  # The 'self' argument

    try:
        return f(*args, **kwargs)
    except Exception as e:

        if b.capture_exceptions:
            b.exception(e)
            b.commit()
            raise LoggedException(e, b)
        else:
            raise

def CaptureException(f, *args, **kwargs):
    """Decorator to capture exceptions. and logging the error"""
    return decorator(_CaptureException, f)  # Preserves signature

class Bundle(object):

    STATES = Constant()
    STATES.NEW = 'new'
    STATES.SYNCED = 'sync_done'
    STATES.DOWNLOADED = 'downloaded'
    STATES.CLEANING = 'clean'
    STATES.CLEANED = 'clean_done'
    STATES.PREPARING = 'prepare'
    STATES.PREPARED = 'prepare_done'
    STATES.BUILDING = 'build'
    STATES.BUILT = 'build_done'
    STATES.FINALIZING = 'finalize'
    STATES.FINALIZED = 'finalize_done'
    STATES.INSTALLING = 'install'
    STATES.INSTALLED = 'install_done'
    STATES.META = 'meta'
    STATES.SCHEMA = 'schema'
    STATES.INGESTING = 'ingest'
    STATES.INGESTED = 'ingest_done'
    STATES.NOTINGESTABLE = 'not_ingestable'

    # Other things that can be part of the 'last action'
    STATES.INFO = 'info'

    # If the bundle is in test mode, only run 1000 rows, selected from the first 100,000
    TEST_ROWS = 1000  # Number of test rows to select from file.

    #  Default body content for pipelines
    default_pipelines = {

        'build': {
            'first': [],
            'map': [ambry.etl.MapSourceHeaders],
            'cast': [ambry.etl.CastColumns],
            'body': [],
            'last': [],
            'select_partition': [ambry.etl.SelectPartition],
            'write': [ambry.etl.WriteToPartition],
            'final': []
        },
    }

    def __init__(self, dataset, library, source_url=None, build_url=None):
        import logging
        import signal

        self._dataset = dataset
        self._library = library
        self._logger = None

        self.multi = None # Number of multiprocessing processes
        self.is_subprocess = False # Externally set in child processes.

        assert bool(library)

        self._log_level = logging.INFO

        self._errors = []
        self._warnings = []

        self._source_url = source_url
        self._build_url = build_url

        self._pipeline_editor = None  # A function that can be set to edit the pipeline, rather than overriding the method

        self._source_fs = None
        self._build_fs = None

        self._identity = None

        self._orig_alarm_handler = signal.SIG_DFL  # For start_progress_loggin

        self.stage = None # Set to the current ingest or build stage
        self.limited_run = False  # Set to true to trigger bundle-specific limits on # of rows processed

        self.capture_exceptions = False  # If set to true (in CLI), will catch and log exceptions internally.
        self.exit_on_fatal = True

        # Test class imported from the test.py file
        self.test_class = None;

        self.init()

    def init(self):
        """An overridable initialization method, called in the Bundle constructor"""
        pass

    def set_file_system(self, source_url=False, build_url=False):
        """Set the source file filesystem and/or build  file system"""

        assert isinstance(source_url, string_types) or source_url is None or source_url is False
        assert isinstance(build_url, string_types) or build_url is False

        if source_url:
            self._source_url = source_url
            self.dataset.config.library.source.url = self._source_url
        elif source_url is None:
            self._source_url = None
            self.dataset.config.library.source.url = self._source_url

        if build_url:
            self._build_url = build_url
            self.dataset.config.library.build.url = self._build_url

        self.dataset.commit()

    def cast_to_subclass(self):
        """
        Load the bundle file from the database to get the derived bundle class,
        then return a new bundle built on that class

        :return:
        """

        from sqlalchemy.exc import IntegrityError

        try:
            self.commit() # To ensure the rollback() doesn't clear out anything important
            bsf = self.build_source_files.file(File.BSFILE.BUILD)
        except:
            self.log("Error trying to create a bundle source file ... ")
            self.rollback()
            return self

        try:
            clz = bsf.import_bundle()

            b = clz(self._dataset, self._library, self._source_url, self._build_url)
            b.limited_run = self.limited_run
            b.capture_exceptions = self.capture_exceptions
            b.multi = self.multi
            return b

        except Exception as e:
            raise BundleError('Failed to load bundle code file, skipping : {}'.format(e))

    def import_lib(self):
        """Import the lib.py file from the bundle"""
        return self.build_source_files.file(File.BSFILE.LIB).import_lib()

    def load_requirements(self):
        """If there are python library requirements set, append the python dir
        to the path."""

        for module_name, pip_name in iteritems(self.metadata.requirements):
            self._library.install_packages(module_name, pip_name)

        python_dir = self._library.filesystem.python()
        sys.path.append(python_dir)

    def commit(self):
        return self.dataset.commit()

    @property
    def session(self):
        return self.dataset._database.session

    def rollback(self):
        return self.dataset.rollback()

    @property
    def dataset(self):
        from sqlalchemy import inspect

        if inspect(self._dataset).detached:
            vid = self._dataset.vid
            self._dataset = self._dataset._database.dataset(vid)

        return self._dataset

    @property
    def identity(self):
        if not self._identity:
            self._identity = self.dataset.identity

        return self._identity

    @property
    def library(self):
        return self._library

    def dep(self, source_name):
        """Return a bundle dependency from the sources list

        :param source_name: Source name. The URL field must be a bundle or partition reference
        :return:
        """
        from ambry.orm.exc import NotFoundError

        source = self.source(source_name)

        try:
            return self.library.partition(source.url)
        except NotFoundError:
            return self.library.bundle(source.url)

    @property
    def config(self):
        """Return the Cofig acessors. THe returned object has properties for acessing other
        groups of configuration values:

        - build: build state
        - metadata: raw acess to metadata values
        - sync: Synchronization times among the build source files, file records, and objects.

        """
        return self.dataset.config

    @property
    def metadata(self):
        """Return the Metadata acessor"""
        return self.dataset.config.metadata

    @property
    def documentation(self):
        """Return the documentation, from the documentation.md file, with template substitutions"""

        # Return the documentation as a scalar term, which has .text() and .html methods to do
        # metadata substitution using Jinja

        return self.metadata.scalar_term(self.build_source_files.documentation.record_content)

    @property
    def partitions(self):
        """Return the Schema acessor"""
        from .partitions import Partitions
        return Partitions(self)

    def partition(self, ref=None, **kwargs):
        """Return a partition in this bundle for a vid reference or name parts"""

        if not ref and not kwargs:
            return None

        if ref:

            for p in self.partitions:
                if p.vid == b(ref) or p.name == b(ref):
                    p._bundle = self
                    return p

            return None

        elif kwargs:
            from ..identity import PartitionNameQuery
            pnq = PartitionNameQuery(**kwargs)

            p = self.partitions._find_orm(pnq).one()
            if p:
                p._bundle = self
                return p

    def new_partition(self, table, **kwargs):
        """
        Add a partition to the bundle
        :param table:
        :param kwargs:
        :return:
        """

        return self.dataset.new_partition(table, **kwargs)

    def wrap_partition(self, p):

        # This used to return a proxy object, but those broke in the transition to python 3

        p._bundle = self

        return p

    def delete_partition(self, vid_or_p):

        try:
            vid = vid_or_p.vid
        except AttributeError:
            vid = vid_or_p

        vid = vid_or_p.vid

        p = self.partition(vid)

        self.session.delete(p._partition)

    def table(self, ref):
        """
        Return a table object for a name or id
        :param ref: Table name, vid or id
        :return:
        """
        return self.dataset.table(ref)

    @property
    def tables(self):
        """
        Return a iterator of tables in this bundle
        :return:
        """
        from ambry.orm import Table
        from sqlalchemy.orm import joinedload, noload

        return (self.dataset.session.query(Table)
                .filter(Table.d_vid == self.dataset.vid)
                .options(noload('column'))
                .all())

        #return self.dataset.tables

    def new_table(self, name, add_id=True, **kwargs):
        """
        Create a new table, if it does not exist, or update an existing table if it does
        :param name:  Table name
        :param add_id: If True, add an id field ( default is True )
        :param kwargs: Other options passed to table object
        :return:
        """

        return self.dataset.new_table(name=name, add_id=add_id, **kwargs)

    def source(self, name):
        source = self.dataset.source_file(name)
        source._bundle = self
        return source

    def source_pipe(self, source):
        """Create a source pipe for a source, giving it access to download files to the local cache"""
        from ambry.etl import DatafileSourcePipe
        if isinstance(source, string_types):
            source = self.source(source)

        source.dataset = self.dataset
        source._bundle = self

        return DatafileSourcePipe(self, source)

    @property
    def sources(self):
        """Iterate over downloadable sources"""
        def set_bundle(s):
            s._bundle = self
            return s

        return list(set_bundle(s) for s in self.dataset.sources )

    def _resolve_sources(self, sources, tables, stage=None, predicate=None):
        """
        Determine what sources to run from an input of sources and tables

        :param sources:  A collection of source objects, source names, or source vids
        :param tables: A collection of table names
        :param stage: If not None, select only sources from this stage
        :param predicate: If not none, a callable that selects a source to return when True
        :return:
        """

        assert sources is None or tables is None

        if not sources:
            if tables:
                sources = list(s for s in self.sources if s.dest_table_name in tables)
            else:
                sources = self.sources
        elif not isinstance(sources, (list, tuple)):
            sources = [sources]

        def objectify(source):
            if isinstance(source, basestring):
                source_name = source
                return self.source(source_name)
            else:
                return source

        sources = [objectify(s) for s in sources]

        if predicate:
            sources = [s for s in sources if predicate(s)]

        if stage:
            sources = [s for s in sources if s.stage == stage]


        return sources

    @property
    def refs(self):

        def set_bundle(s):
            s._bundle = self
            return s

        """Iterate over downloadable sources -- references and templates"""
        return list(set_bundle(s) for s in self.dataset.sources if not s.is_downloadable)

    @property
    def source_tables(self):
        return self.dataset.source_tables

    def source_table(self, ref):
        return self.dataset.source_table(ref)

    #
    # Files and Filesystems
    #

    @property
    def build_source_files(self):
        """Return acessors to the build files"""

        from .files import BuildSourceFileAccessor
        return BuildSourceFileAccessor(self, self.dataset, self.source_fs)

    @property
    def source_fs(self):
        from fs.opener import fsopendir
        from fs.errors import ResourceNotFoundError

        if not self._source_fs:

            source_url = self._source_url if self._source_url else self.dataset.config.library.source.url

            if not source_url:
                source_url = self.library.filesystem.source(self.identity.cache_key)

            try:

                self._source_fs = fsopendir(source_url)
            except ResourceNotFoundError:
                self.logger.warn("Failed to locate source dir {}; using default".format(source_url))
                source_url = self.library.filesystem.source(self.identity.cache_key)
                self._source_fs = fsopendir(source_url)

        return self._source_fs

    @property
    def build_fs(self):
        from fs.opener import fsopendir, opener

        if not self._build_fs:
            build_url = self._build_url if self._build_url else self.dataset.config.library.build.url

            if not build_url:
                build_url = self.library.filesystem.build(self.identity.cache_key)
                # raise ConfigurationError(
                #    'Must set build URL either in the constructor or the configuration')


            self._build_fs = fsopendir(build_url, create_dir=True)

        return self._build_fs

    @property
    def build_partition_fs(self):
        """Return a pyfilesystem subdirectory for the build directory for the bundle. This the sub-directory
        of the build FS that holds the compiled SQLite file and the partition data files"""

        base_path = os.path.dirname(self.identity.cache_key)

        if not self.build_fs.exists(base_path):
            self.build_fs.makedir(base_path, allow_recreate=True)

        return self.build_fs.opendir(base_path)

    @property
    def build_ingest_fs(self):
        """Return a pyfilesystem subdirectory for the ingested source files"""

        base_path = 'ingest'

        if not self.build_fs.exists(base_path):
            self.build_fs.makedir(base_path, recursive=True, allow_recreate=True)

        return self.build_fs.opendir(base_path)

    def phase_search_names(self, source, phase):
        """Search the bundle.yaml metadata file for pipeline configurations. Looks for:
        - <phase>-<source_table>
        - <phase>-<dest_table>
        - <phase>-<source_name>

        """
        search = []


        # Create a search list of names for getting a pipline from the metadata
        if source and source.source_table_name:
            search.append(phase + '-' + source.source_table_name)

        if source and source.dest_table_name:
            search.append(phase + '-' + source.dest_table_name)

        if source:
            search.append(phase + '-' + source.name)

        search.append(phase)

        return search

    #
    # Logging
    #

    @property
    def log_file(self):

        from os.path import dirname

        if self.multi:
            log_file = BUILD_LOG_FILE.format('_' + str(os.getpid()))
        else:
            log_file = BUILD_LOG_FILE.format('')

        if not self.build_fs.exists(log_file):
            self.build_fs.makedir(dirname(log_file), recursive=True, allow_recreate=True)
            self.build_fs.createfile(log_file)

        return log_file

    @property
    def logger(self):
        """The bundle logger."""
        import os


        if not self._logger:

            ident = self.identity
            if self.multi:
                template = '%(levelname)s %(process)d {} %(message)s'.format(ident.vid)
            else:
                template = '%(levelname)s {} %(message)s'.format(ident.vid)

            try:
                file_name = self.build_fs.getsyspath(self.log_file)
                self._logger = get_logger(__name__, template=template, stream=sys.stdout, file_name=file_name)
            except NoSysPathError:
                # file does not exists in the os - memory fs for example.
                self._logger = get_logger(__name__, template=template, stream=sys.stdout)

            self._logger.setLevel(self._log_level)

        return self._logger

    def log_to_file(self, message):
        """Write a log message only to the file"""

        with self.build_fs.open(self.log_file, 'a+') as f:
            f.write(unicode(message + '\n'))

    def log(self, message, **kwargs):
        """Log the messsage."""
        self.logger.info(message)

    def debug(self, message, **kwargs):
        """Log the messsage."""
        self.logger.debug(message)

    def error(self, message, set_error_state = False):
        """Log an error messsage.

        :param message:  Log message.

        """
        if set_error_state:
            if message not in self._errors:
                self._errors.append(message)

            self.set_error_state()

        self.logger.error(message)

    def exception(self, e):
        """Log an error messsage.

        :param e:  Exception to log.

        """
        if str(e) not in self._errors:
            self._errors.append(str(e))

        self.set_error_state()
        self.dataset.config.build.state.exception_type = str(e.__class__.__name__)
        self.dataset.config.build.state.exception = str(e)
        self.logger.exception(e)

    def warn(self, message):
        """Log an error messsage.

        :param message:  Log message.

        """
        if message not in self._warnings:
            self._warnings.append(message)

        self.logger.warn(message)

    def fatal(self, message):
        """Log a fatal messsage and exit.

        :param message:  Log message.

        """

        self.logger.fatal(message)
        sys.stderr.flush()
        if self.exit_on_fatal:
            sys.exit(1)
        else:
            raise FatalError(message)

    def progress_logging(self, f, interval=2):
        """Context manager to start and stop context logging.

        :param f: A function to call. Returns either a string, or a tuple (format_string, format_args)
        :param interval: Frequency to call the function, in seconds.
        :return:

        """

        bundle = self

        class _ProgressLogger(object):

            def __enter__(self):
                bundle.start_progress_logging(f, interval)

            def __exit__(self, exc_type, exc_val, exc_tb):

                bundle.stop_progress_logging()

                if exc_val:
                    return False
                else:
                    return True

        return _ProgressLogger()

    def start_progress_logging(self, f, interval=2):
        """
        Call the function ``f`` every ``interval`` seconds to produce a logging message to be passed
        to self.log().

        NOTE: This may cause problems with IO operations:

            When a signal arrives during an I/O operation, it is possible that the I/O operation raises an exception
            after the signal handler returns. This is dependent on the underlying Unix system's
            semantics regarding interrupted system calls.

        :param f: A function to call. Returns either a string, or a tuple (format_string, format_args)
        :param interval: Frequence to call the function, in seconds.
        :return:
        """

        import signal

        def handler(signum, frame):

            r = f()

            if isinstance(r, (tuple, list)):
                try:
                    self.log(r[0].format(*r[1]))
                except IndexError:
                    self.log(str(r) + ' (Bad log format)')  # Well, at least log something
            else:
                self.log(str(r) + ' ' + str(type(r)))

            # Or, use signal.itimer()? Maybe, but this way, the handler will stop if there is
            # an exception, rather than getting regular exceptions.
            signal.alarm(interval)

        old_handler = signal.signal(signal.SIGALRM, handler)

        if not self._orig_alarm_handler:  # Only want the handlers from other outside sources
            self._orig_alarm_handler = old_handler

        signal.alarm(interval)

    def stop_progress_logging(self):
        """
        Stop progress logging by removing the Alarm signal handler and canceling the alarm.
        :return:
        """

        import signal

        if self._orig_alarm_handler:
            signal.signal(signal.SIGALRM, self._orig_alarm_handler)
            self._orig_alarm_handler = None

            signal.alarm(0)  # Cancel any currently active alarm.

    def init_log_rate(self, N=None, message='', print_rate=None):
        from ..util import init_log_rate as ilr

        return ilr(self.log, N=N, message=message, print_rate=print_rate)

    #
    # Pipelines
    #

    def log_pipeline(self, pl):
        """Write a report of the pipeline out to a file """
        from datetime import datetime
        from ambry.etl.pipeline import CastColumns

        self.build_fs.makedir('pipeline', allow_recreate=True)

        try:
            ccp = pl[CastColumns]
            caster_code = ccp.pretty_code
        except Exception as e:
            caster_code = str(e)

        v = u("""
Pipeline     : {}
run time     : {}
phase        : {}
source name  : {}
source table : {}
dest table   : {}
========================================================
{}

Pipeline Headers
================
{}

Caster Code
===========
{}

""".format(pl.name, str(datetime.now()), pl.phase, pl.source_name, pl.source_table,
           pl.dest_table, unicode(pl), pl.headers_report(), caster_code))

        path = os.path.join('pipeline', pl.phase + '-' + pl.file_name + '.txt')

        self.build_fs.makedir(os.path.dirname(path), allow_recreate=True, recursive=True)

        self.build_fs.setcontents(path, v, encoding='utf8')

    def pipeline(self, source=None, phase='build'):
        """
        Construct the ETL pipeline for all phases. Segments that are not used for the current phase
        are filtered out later.

        :param source: A source object, or a source string name
        :return: an etl Pipeline
        """
        from ambry.etl.pipeline import Pipeline, PartitionWriter
        from ambry.dbexceptions import ConfigurationError

        if source:
            source = self.source(source) if isinstance(source, string_types) else source
        else:
            source = None

        pl = Pipeline(self, source=self.source_pipe(source) if source else None)

        # Get the default pipeline, from the config at the head of this file.
        try:
            phase_config = self.default_pipelines[phase]
        except KeyError:
            phase_config = None  # Ok for non-conventional pipe names

        if phase_config:
            pl.configure(phase_config)

        body = []

        # Find the pipe configuration, from the metadata
        pipe_config = None
        pipe_name = None
        if source and source.pipeline:
            pipe_name = source.pipeline
            try:
                pipe_config = self.metadata.pipelines[pipe_name]
            except KeyError:
                raise ConfigurationError("Pipeline '{}' declared in source '{}', but not found in metadata"
                                         .format(source.pipeline, source.name))
        else:
            for name in self.phase_search_names(source, phase):
                if name in self.metadata.pipelines:
                    pipe_config = self.metadata.pipelines[name]
                    pipe_name = name

                    break

        if pipe_name:
            pl.name = pipe_name
        else:
            pl.name = phase

        pl.phase = phase

        # The pipe_config can either be a list, in which case it is a list of pipe pipes for the
        # augment segment or it could be a dict, in which case each is a list of pipes
        # for the named segments.

        def apply_config(pl, pipe_config):

            if isinstance(pipe_config, (list, tuple)):
                # Just convert it to dict form for the next section

                # PartitionWriters are always moved to the 'store' section
                store, body = [], []

                for pipe in pipe_config:
                    store.append(pipe) if isinstance(pipe, PartitionWriter) else body.append(pipe)

                pipe_config = dict(body=body, store=store)

            if pipe_config:
                pl.configure(pipe_config)

        apply_config(pl, pipe_config)

        # One more time, for the configuration for 'all' phases
        if 'all' in self.metadata.pipelines:
            apply_config(pl, self.metadata.pipelines['all'])

        # Allows developer to over ride pipe configuration in code
        self.edit_pipeline(pl)

        try:

            pl.dest_table = source.dest_table_name
            pl.source_table = source.source_table.name
            pl.source_name = source.name
        except AttributeError:
            pl.dest_table = None

        return pl

    def set_edit_pipeline(self, f):
        """Set a function to edit the pipeline"""

        self._pipeline_editor = f

    def edit_pipeline(self, pipeline):
        """Called after the meta pipeline is constructed, to allow per-pipeline modification."""

        if self._pipeline_editor:
            self._pipeline_editor(pipeline)

        return pipeline

    def field_row(self, fields):
        """
        Return a list of values to match the fields values. This is used when listing bundles to
        produce a table of information about the bundle.

        :param fields: A list of names of data items.
        :return: A list of values, in the same order as the fields input

        The names in the fields llist can be:

        - state: The current build state
        - source_fs: The URL of the build source filesystem
        - about.*: Any of the metadata fields in the about section

        """

        row = self.dataset.row(fields)

        # Modify for special fields
        for i, f in enumerate(fields):
            if f == 'state':
                row[i] = self.state
            elif f == 'source_fs':
                row[i] = self.source_fs
            elif f.startswith('about'):  # all metadata in the about section, ie: about.title
                _, key = f.split('.')
                row[i] = self.metadata.about[key]
            elif f.startswith('state'):
                _, key = f.split('.')
                row[i] = self.dataset.config.build.state[key]
            elif f.startswith('count'):
                _, key = f.split('.')
                if key == 'sources':
                    row[i] = len(self.dataset.sources)
                elif key == 'tables':
                    row[i] = len(self.dataset.tables)

        return row

    #
    # States
    #

    def clear_states(self):
        """Delete  all of the build state information"""
        return self.dataset.config.build.clean()

    @property
    def state(self):
        """Return the current build state"""
        return self.dataset.config.build.state.current

    @property
    def error_state(self):
        """Set the error condition"""
        self.dataset.config.build.state.lasttime = time()
        return self.dataset.config.build.state.error

    @property
    def build_state(self):
        """"Returns the build state accessor, self.dataset.config.build.state"""

        return self.dataset.config.build.state

    @state.setter
    def state(self, state):
        """Set the current build state and record the time to maintain history"""

        assert state != 'build_bundle'

        self.dataset.config.build.state.current = state
        self.dataset.config.build.state[state] = time()
        self.dataset.config.build.state.lasttime = time()

        self.dataset.config.build.state.error = False
        self.dataset.config.build.state.exception = None
        self.dataset.config.build.state.exception_type = None

    def record_stage_state(self, phase, stage):
        """Record the completion times of phases and stages"""

        key = "{}-{}".format(phase, stage if stage else 1)

        self.dataset.config.build.state[key] = time()


    def set_error_state(self):
        self.dataset.config.build.state.error = time()
        self.state = self.state + ('_error' if not self.state.endswith('_error') else '')

    def set_last_access(self, tag):
        """Mark the time that this bundle was last accessed"""
        self.dataset.config.build.access.last = tag

    #########################
    # Build phases
    #########################

    #
    # Do All; Run the full process

    def run(self, sources=None, tables=None, force=False, schema=False):
        """Synonym for run_stages"""
        return self.run_stages(sources, tables, force=force)

    @CaptureException
    def run_stages(self, sources=None, tables=None, force=False):
        """Like run, but runs stages in order, rather than each phase in order. So, run() will ingest all sources,
        then build all sources, while run_stages() will ingest then build all order=1 sources, then ingest and
        build all order=2 sources.

        Actual operation is to group sources by order, then call run() on each group
        """

        from itertools import groupby
        from operator import attrgetter
        from ambry.bundle.events import TAG

        resolved_sources = self._resolve_sources(sources, tables)

        if force:  # Clean all of the old partitions

            for p in self.dataset.partitions:
                if p.type == p.TYPE.SEGMENT:
                    self.log("Removing old segment partition: {}".format(p.identity.name))
                else:
                    self.log("Removing build partition: {}".format(p.identity.name))
                self.wrap_partition(p).datafile.remove()
                self.session.delete(p)

            for s in resolved_sources:
                if s.state in (self.STATES.BUILDING, self.STATES.BUILT):
                    s.state = self.STATES.INGESTED

            self.commit()

        keyfunc = attrgetter('stage')
        self._run_events(TAG.BEFORE_RUN)

        for stage, stage_sources in groupby(sorted(resolved_sources, key=keyfunc), keyfunc):
            stage_sources = list(stage_sources)  # Stage_sources is an iterator, can only be traversed once

            self._run_events(TAG.BEFORE_STAGE, stage)
            if not self._run_stage(stage,sources=stage_sources,  force=force, finalize=False):
                self.error('Failed to run stage {}'.format(stage))
            self._run_events(TAG.AFTER_STAGE, stage)

        self.post_run()

        if not sources and not tables:
            self._run_events(TAG.AFTER_RUN)
        else:
            self.log("Sources or tables specified; skipping after run test")

        return True

    def _run_stage(self, stage=None, sources=None, tables=None,  force=False, finalize=True):

        if self.is_finalized and not force:
            self.error("Can't run; bundle is finalized")
            return False

        sources = self._resolve_sources(sources, tables)

        if not self.ingest(sources=sources, force=force):
            self.error('Run: failed to ingest')
            return False

        if not self.schema(sources=sources, stage=stage, force=force):
            self.error('Run: failed to build schema')
            return False

        if not self.build(sources=sources, stage=stage, force=force):
            self.error('Run: failed to build')
            return False

        if finalize:
            self.finalize()

        return True

    #
    # Syncing
    #

    def sync(self, force=None, defaults=False):
        """

        :param force: Force a sync in one direction, either ftr or rtf.
        :param defaults [False] If True and direction is rtf, write default source files
        :return:
        """

        if self.is_finalized:
            self.error("Can't sync; bundle is finalized")
            return False

        syncs = self.build_source_files.sync(force, defaults)

        self.state = self.STATES.SYNCED
        self.log("---- Synchronized ----")
        self.dataset.commit()

        self.library.search.index_bundle(self, force=True)

        return syncs

    def sync_in(self):
        """Synchronize from files to records, and records to objects"""
        from ambry.bundle.files import BuildSourceFile
        self.build_source_files.sync(BuildSourceFile.SYNC_DIR.FILE_TO_RECORD)
        self.build_source_files.record_to_objects()
        self.log("---- Sync In ----")
        self.library.search.index_bundle(self, force=True)
        # self.state = self.STATES.SYNCED

    def sync_objects_in(self):
        """Synchronize from records to objects"""
        self.build_source_files.record_to_objects()

    def sync_out(self):
        from ambry.bundle.files import BuildSourceFile
        """Synchronize from objects to records"""
        self.build_source_files.objects_to_record()
        self.build_source_files.sync(BuildSourceFile.SYNC_DIR.RECORD_TO_FILE)
        # self.state = self.STATES.SYNCED

    def sync_objects_out(self):
        """Synchronize from objects to records, and records to files"""
        self.log("---- Sync Out ----")
        self.build_source_files.objects_to_record()

    def sync_objects(self):
        self.build_source_files.record_to_objects()
        self.build_source_files.objects_to_record()

    def sync_code(self):
        """Sync in code files and the meta file, avoiding syncing the larger files"""
        from ambry.orm.file import File
        from ambry.bundle.files import BuildSourceFile

        self.log("---- Sync Code ----")
        for fc in [File.BSFILE.BUILD, File.BSFILE.META, File.BSFILE.LIB, File.BSFILE.TEST]:
            self.build_source_files.file(fc).sync(BuildSourceFile.SYNC_DIR.FILE_TO_RECORD)

    #
    # Clean
    #

    @property
    def is_clean(self):
        return self.state == self.STATES.CLEANED

    def clean(self, force=False):
        """Clean generated objects from the dataset, but only if there are File contents
         to regenerate them"""

        if self.is_finalized and not force:
            self.warn("Can't clean; bundle is finalized")
            return False

        self.log('---- Cleaning ----')
        self.state = self.STATES.CLEANING

        self.commit()

        self.clean_sources()
        self.clean_tables()
        self.clean_partitions()
        self.clean_build()
        self.clean_files()
        self.clean_ingested()
        self.clean_build_state()

        self.state = self.STATES.CLEANED

        self.commit()

        self.log('---- Done Cleaning ----')

        return True

    def clean_except_files(self):
        """Clean everything except the build source files"""

        if self.is_finalized:
            self.warn("Can't clean; bundle is finalized")
            return False

        self.log('---- Cleaning ----')
        self.state = self.STATES.CLEANING

        self.commit()

        self.clean_sources()
        self.clean_tables()
        self.clean_partitions()
        self.clean_build()
        self.clean_ingested()
        self.clean_build_state()

        self.state = self.STATES.CLEANED

        self.commit()

        self.log('---- Done Cleaning ----')

        return True

    def clean_sources(self):
        """Like clean, but also clears out files. """

        for src in self.dataset.sources:
            src.st_id = None
            src.t_id = None

        self.dataset.sources[:] = []
        self.dataset.source_tables[:] = []

    def clean_tables(self):
        """Like clean, but also clears out schema tables and the partitions that depend on them. . """

        self.dataset.delete_tables_partitions()

    def clean_partitions(self):
        """Delete partition records and any built partition files.  """
        import shutil
        from ambry.orm import ColumnStat

        # FIXME. There is a problem with the cascades for ColumnStats that prevents them from
        # being  deleted with the partitions. Probably, the are seen to be owed by the columns instead.
        self.session.query(ColumnStat).filter(ColumnStat.d_vid == self.dataset.vid).delete()

        self.dataset.delete_partitions()

        if self.build_partition_fs.exists:
            try:
                shutil.rmtree(self.build_partition_fs.getsyspath('/'))
            except NoSysPathError:
                pass # If there isn't a syspath, probably don't need to delete.

    def clean_build(self):
        """Delete the build directory and all ingested files """
        import shutil

        if self.build_fs.exists:
            try:
                shutil.rmtree(self.build_fs.getsyspath('/'))
            except NoSysPathError:
                pass # If there isn't a syspath, probably don't need to delete.

    def clean_files(self):
        """ Delete all build source files """

        self.dataset.files[:] = []
        self.commit()

    def clean_ingested(self):
        """"Clean ingested files"""
        for s in self.sources:
            df = s.datafile
            if df.exists:
                df.remove()
                s.state = s.STATES.NEW

        self.commit()

    def clean_all(self):
        """Like clean, but also clears out files. """

        self.clean()

    def clean_process_meta(self):
        """Remove all process and build metadata"""
        ds = self.dataset
        ds.config.build.clean()
        ds.config.process.clean()
        ds.commit()
        self.state = self.STATES.CLEANED

    def clean_source_files(self):
        """Remove the schema.csv and source_schema.csv files"""

        self.build_source_files.file(File.BSFILE.SOURCESCHEMA).remove()
        self.build_source_files.file(File.BSFILE.SCHEMA).remove()
        self.commit()

    def clean_build_files(self):
        """Remove all of the build files"""

        for bs in self.build_source_files:
            print '!!!!', bs.path

    def clean_build_state(self):

        self.dataset.config.build.clean()
        self.commit()

    #
    # Ingestion
    #

    @CaptureException
    def ingest(self, sources=None, tables=None, stage=None, force=False, update_tables = True):
        """Ingest a set of sources, specified as source objects, source names, or destination tables.
        If no stage is specified, execute the sources in groups by stage.

        Note, however, that when this is called from run_stage, all of the sources have the same stage, so they
        get grouped together. The result it that the stage in the inner loop is the same as the stage being
        run buy run_stage.
        """

        from itertools import groupby
        from ambry.bundle.events import TAG
        from fs.errors import ResourceNotFoundError
        import zlib

        self.state = self.STATES.INGESTING
        self.commit()

        # Clear out all ingested files that are malformed
        for s in self.sources:
            if s.is_downloadable:
                df = s.datafile
                try:
                    info = df.info
                except (ResourceNotFoundError, zlib.error, IOError):
                    s.datafile.remove()

        def not_final_or_delete(s):
            import zlib

            if force:
                return True

            try:
                return s.is_processable and not s.is_ingested and not s.is_built
            except (IOError, zlib.error):
                s.datafile.remove()
                return True

        key = lambda s: s.stage if s.stage else 1
        sources = sorted(self._resolve_sources(sources, tables, stage), key=key)

        for stage, g in groupby(sources, key):
            sources = [s for s in g if not_final_or_delete(s)]
            if len(sources):
                self.log("Ingesting {} sources in stage {}".format(len(sources), stage))
            else:
                self.log("No sources left to ingest for stage {}".format(stage))
                continue

            self._run_events(TAG.BEFORE_INGEST, stage)
            self._ingest_sources(sources, force=force)
            self._run_events(TAG.AFTER_INGEST, stage)
            self.record_stage_state(self.STATES.INGESTING, stage)

        self.state = self.STATES.INGESTED
        self.commit()

        return True

    def _ingest_sources(self, sources, force=False):
        """Ingest a set of sources, usually for one stage"""
        from concurrent import ingest_mp
        try:
            self.state = self.STATES.INGESTING

            downloadable_sources = [s for s in sources if force or
                                    (s.is_processable and not s.is_ingested and not s.is_built)]

            if self.multi:
                args = [(self.identity.vid, source.vid, force) for source in downloadable_sources]

                pool = self.library.process_pool
                try:
                    pool.map(ingest_mp, args)
                except KeyboardInterrupt:
                    self.log("Got keyboard interrrupt; terminating workers")
                    pool.terminate()
            else:
                for source in downloadable_sources:
                    self._ingest_source(source, force)

            return True

        except Exception as e:
            self.commit()
            raise

    def _ingest_source(self, source, force=None):
        """Ingest a single source"""
        from ambry_sources.sources import FixedSource, GeneratorSource
        from ambry_sources import get_source
        from ambry_sources.exceptions import MissingCredentials
        from ambry.orm.exc import NotFoundError

        def account_accessor(url, accounts=self.library.config.accounts):
            # return empty dict if credentials do not exist
            # to force ambry_sources to raise exception with required config.
            return accounts.get(url, {})

        if not source.is_partition and source.datafile.exists:
            if not source.datafile.is_finalized:
                source.datafile.remove()
            elif force:
                source.datafile.remove()
            else:
                self.log("Source {} already ingested, skipping".format(source.name))
                return

        if source.is_partition:
            # Check if the partition exists
            try:
                p = self.library.partition(source.ref)
            except NotFoundError:
                # Maybe it is an internal reference, in which case we can just delay
                # until the partition is built
                self.log("Not Ingesting {}: referenced partition '{}' does not exist".format(source.name, source.ref))
                return

        self.log('Ingesting: {} from {}'.format(source.spec.name, source.url or source.generator))
        source.state = source.STATES.INGESTING
        self.commit()

        s = None

        if source.reftype == 'partition':
            source.update_table()  # Generate the source tables.
            self.log('Ingested partition: {}'.format(source.datafile.path))
            source.state = source.STATES.INGESTED
            self.commit()

            return

        elif source.reftype == 'generator':
            import sys

            if hasattr(self, source.generator):
                gen_cls = getattr(self, source.generator)
            elif source.generator in sys.modules['ambry.build'].__dict__:
                gen_cls = sys.modules['ambry.build'].__dict__[source.generator]
            else:
                gen_cls = self.import_lib().__dict__[source.generator]

            spec = source.spec
            spec.start_line = 1
            spec.header_lines = [0]
            s = GeneratorSource(spec, gen_cls(self, source))

        elif source.is_downloadable:

            with self.progress_logging(lambda: ('Downloading {}', (source.url,)), 10):
                try:
                    s = get_source(
                        source.spec, self.library.download_cache,
                        clean=force, account_accessor=account_accessor)

                except MissingCredentials as exc:
                    formatted_cred = ['    {}: <your {}>'.format(x, x) for x in exc.required_credentials]
                    msg = \
                        'Missing credentials for {location}.\n' \
                        'Hint: Check accounts section of your ~/.ambry-accounts.yaml ' \
                        'for {location} credentials. If there is no such, use next template to ' \
                        'add credentials:\n' \
                        '{location}:\n' \
                        '{cred}'.format(location=exc.location, cred='\n'.join(formatted_cred))
                    raise Exception(msg)

            if isinstance(s, FixedSource):
                from ambry_sources.sources.spec import ColumnSpec

                s.spec.columns = [ColumnSpec(c.name, c.position, c.start, c.width)
                                  for c in source.source_table.columns]

        else:
            self.log('Not an ingestiable source: {}'.format(source.name))
            source.state = source.STATES.NOTINGESTABLE
            self.commit()
            return

        with self.progress_logging(lambda: ('Ingesting {}: {} {} of {}, rate: {}',
                                              (source.spec.name,) + source.datafile.report_progress()), 10):

            source.datafile.load_rows(s)

        source.update_table()  # Generate the source tables.
        source.update_spec()  # Update header_lines, start_line, etc.

        self.log('Ingested: {}'.format(source.datafile.path))
        source.state = source.STATES.INGESTED
        self.commit()

    def _ingest_update_tables(self, sources):
        # Do these updates, even if we skipped ingestion, so that the source tables will be generated if they
        # had been cleaned from the database, but the ingested files still exists.
        for i, source in enumerate(sources):
            source.update_table()  # Generate the source tables.
            source.update_spec()  # Update header_lines, start_line, etc.

        self.commit()

        return True

    #
    # Schema
    #

    @CaptureException
    def schema(self, sources =None, tables=None, stage=None, clean=False, force=False, use_pipeline=False):
        """
        Generate destination schemas.

        :param sources: If specified, build only destination tables for these soruces
        :param tables: If specified, build only these tables
        :param clean: Delete tables and partitions first
        :param force: Population tables even if the table isn't empty
        :param use_pipeline: If True, use the build pipeline to determine columns. If false, use the source schemas.

        :return: True on success.
        """
        from itertools import groupby
        from operator import attrgetter
        from ambry.orm.exc import NotFoundError
        from ambry.etl import Collect, Head
        from ambry.bundle.events import TAG

        sources = self._resolve_sources(sources, tables, stage, predicate = lambda s: s.is_processable)

        if clean:
            self.dataset.delete_tables_partitions()
            self.commit()

        self._run_events(TAG.BEFORE_SCHEMA, stage)

        # Group the sources by the destination table name
        keyfunc = attrgetter('dest_table')
        for t, sources in groupby(sorted(sources, key=keyfunc), keyfunc):

            if not force and not t.is_empty:
                continue

            if use_pipeline:
                for source in sources:
                    pl = self.pipeline(source)

                    pl.cast = [ambry.etl.CastSourceColumns]
                    pl.select_partition = []
                    pl.write = [Head, Collect]
                    pl.final = []

                    self.log_pipeline(pl)

                    pl.run()
                    pl.phase = 'build_schema'
                    self.log_pipeline(pl)

                    for h, c in zip(pl.write[Collect].headers, pl.write[Collect].rows[1]):
                        t.add_column(name=h, datatype=type(c).__name__ if c is not None else 'str',
                                     update_existing=True)

                self.log("Populated destination table '{}' from pipeline '{}'"
                         .format(t.name, pl.name))

            else:
                # Get all of the header names, for each source, associating the header position in the table
                # with the header, then sort on the postition. This will produce a stream of header names
                # that may have duplicates, but which is generally in the order the headers appear in the
                # sources. The duplicates are properly handled when we add the columns in add_column()
                columns = sorted(set([(i, col.dest_header, col.datatype, col.description) for source in sources
                                 for i, col in enumerate(source.source_table.columns)]))

                for pos, name, datatype, desc in columns:
                    t.add_column(name=name, datatype=datatype, description=desc, update_existing = True)

                self.log("Populated destination table '{}' from source table '{}' with {} columns"
                         .format(t.name, source.source_table.name, len(columns)))

        for i, source in enumerate(self.refs):
            try:
                pass
                source.update_table()  # Generate the source tables.
            except NotFoundError:
                # Ignore not found errors here, because the ref may be to a partition that has not been built yet.
                self.log("Skipping {}".format(source.name))

        self._run_events(TAG.AFTER_SCHEMA, stage)
        self.commit()

        return True


    #
    # Build
    #

    @property
    def is_buildable(self):
        return not self.is_built and not self.is_finalized

    @property
    def is_built(self):
        """Return True is the bundle has been built."""
        return self.state == self.STATES.BUILT

    def pre_build(self, force=False):

        if self.is_finalized:
            self.error("Can't build; bundle is finalized")
            return False

        if self.is_built:
            self.error("Can't run build; bundle is built")
            return False

        if self.state.endswith('error'):
            self.error("Can't run build; bundle is in error state")
            return False

        self.state = self.STATES.BUILDING

        self.import_lib()
        self.load_requirements()

        return True

    def build(self, sources=None, tables=None, stage=None, force=False):
        """

        :param phase:
        :param stage:
        :param sources: Source names or destination table names.
        :return:
        """

        from operator import attrgetter
        from itertools import groupby
        from .concurrent import build_mp
        from ambry.bundle.events import TAG

        if not self.pre_build(force):
            return False

        resolved_sources = self._resolve_sources(sources, None, stage=stage,
                                                 predicate=lambda s : s.is_processable )

        if not resolved_sources:
            self.log("No processable sources, skiping build stage {}".format(stage))
            return True

        for stage, source_group in groupby(sorted(resolved_sources, key=attrgetter('stage')), attrgetter('stage')):

            sources = list(source_group)

            self.log('Processing {} sources, stage {} ; first 10: {}'
                     .format(len(sources), stage, [x.name for x in sources[:10]]))

            self._run_events(TAG.BEFORE_BUILD, stage)

            if self.multi:
                args = [(self.identity.vid, source.vid, force) for source in sources]

                pool = self.library.process_pool

                try:
                    pool.map(build_mp, args)
                except KeyboardInterrupt:
                    self.log("Got keyboard interrrupt; terminating workers")
                    pool.terminate()

            else:

                for source in sources:
                    self.build_source(source, force = force)

                self.record_stage_state(self.STATES.BUILDING, stage)

            self.dataset.commit()

            self.unify_partitions()

            self.dataset.commit()

            self._run_events(TAG.AFTER_BUILD, stage)

        return True

    def post_run(self, phase='build'):
        """After the build, update the configuration with the time required for
        the build, then save the schema back to the tables, if it was revised
        during the build."""

        self.library.search.index_bundle(self, force=True)

        self.state = phase + '_done'

        self.commit()
        self.log("---- Finished phase {} ---- ".format(phase))

        return True

    def build_table(self, table, force = False):
        """Build all of the sources for a table """

        sources = self._resolve_sources(None, [table])

        for source in sources:
            self.build_source(source, force=force)

        self.unify_partitions()

    def build_source(self, source, force = False):
        """Build a single source"""

        assert source.is_processable, source.name

        if source.state == self.STATES.BUILT and not force:
            self.logger.info('Source {} already built'.format(source.name))
            return

        pl = self.pipeline(source)

        source.state = self.STATES.BUILDING
        self.commit()

        try:

            self.logger.info('Running source {} with pipeline {}'.format(source.name, pl.name))

            # Doing this before hand to get at least some information about the pipline,
            # in case there is an error during the run. It will get overwritten with more information
            # after asecussful run
            self.log_pipeline(pl)

            try:
                with self.progress_logging(lambda: ('Run source {}: {} rows, {} rows/sec',
                                                    (source.spec.name,) + pl.sink.report_progress()), 10):

                    pl.run()
            except:
                self.log_pipeline(pl)
                raise

            self.logger.info('Done running source {} with pipeline {}'.format(source.name, pl.name))

            self.commit()

            try:
                for p in pl[ambry.etl.PartitionWriter].partitions:

                    try:
                        p.finalize()
                    except AttributeError:
                        print self.table(p.table_name)
                        raise

                    # FIXME Shouldn't need to do this commit, but without it, some stats get added multiple
                    # times, causing an error later. Probably could be avoided by adding the stats to the
                    # collection in the dataset
                    self.commit()

            except IndexError:
                self.error("Pipeline didn't have a PartitionWriters, won't try to finalize")

            self.log_pipeline(pl)

            source.state = self.STATES.BUILT
            self.commit()

            if pl.stopped:
                # Do something when the pipe has stopped?
                # This was supposed to kick out of the loop of building a table, probably.
                return

        except Exception as e:
            self.error("Failed to build source '{}'; {}".format(source.name, e))
            source.state = source.state + ('_error' if not self.state.endswith('_error') else '')
            self.commit()

            raise


    def unify_partitions(self):
        """For all of the segments for a partition, create the parent partition, combine the children into the parent,
        and delete the children. """

        from collections import defaultdict

        # Group the segments by their parent partition name, which is the
        # same name, but without the segment.
        partitions = defaultdict(set)
        for p in self.dataset.partitions:
            if p.type == p.TYPE.SEGMENT:
                name = p.identity.name
                name.segment = None
                partitions[name].add(p)

        # For each group, copy the segment partitions to the parent partitions, then
        # delete the segment partitions.


        for name, segments in iteritems(partitions):
            self.unify_partition(name, segments)

    def unify_partition(self, partition_name, segments=None):
        from ..orm.partition import Partition

        if segments is None:
            segments = set()
            for p in self.dataset.partitions:
                if p.type == p.TYPE.SEGMENT:
                    name = p.identity.name
                    name.segment = None

                    if name == partition_name:
                        segments.add(p)

        self.log('Coalescing segments for partition {} '.format(partition_name))

        parent = self.partitions.get_or_new_partition(partition_name, type=Partition.TYPE.UNION)

        if parent.datafile.exists:
            self.log("Removing exising datafile {}".format(parent.datafile.path))
            parent.datafile.remove()

        headers = None
        i = 1  # Row id.

        logger = lambda: ("Coalescing: {} {} of {}, rate: {}", parent.datafile.report_progress())

        with parent.datafile.writer as w, self.progress_logging(logger, 10):

            for seg in sorted(segments, key=lambda x: b(x.name)):

                self.debug(indent + 'Coalescing segment  {} '.format(seg.identity.name))

                with self.wrap_partition(seg).datafile.reader as reader:
                    for row in reader.rows:
                        w.insert_row((i,) + row[1:])
                        i += 1

            self.debug(indent + "Coalesced {} rows into ".format(i))

        parent.finalize()
        self.log("Coalesced {}".format(parent.name))
        self.commit()

        for s in segments:
            assert s.segment is not None
            self.wrap_partition(s).datafile.remove()
            self.session.delete(s)

        self.commit()

    def exec_context(self, **kwargs):
        """Base environment for evals, the stuff that is the same for all evals. Primarily used in the
        Caster pipe"""
        import inspect
        import dateutil.parser
        import datetime
        import random
        from functools import partial
        from ambry.valuetype.types import parse_date, parse_time, parse_datetime
        import ambry.valuetype.types
        import ambry.valuetype.math
        import ambry.valuetype.string
        import ambry.valuetype.exceptions
        import ambry.valuetype.test

        def set_from(f, frm):
            try:
                try:
                    f.ambry_from = frm
                except AttributeError:  # for instance methods
                    f.im_func.ambry_from = frm
            except (TypeError, AttributeError):  # Builtins, non python code
                pass

            return f

        test_env = dict(

            parse_date=parse_date,
            parse_time=parse_time,
            parse_datetime=parse_datetime,
            partial=partial,
            bundle = self,
        )

        test_env.update(kwargs)
        test_env.update(dateutil.parser.__dict__)
        test_env.update(datetime.__dict__)
        test_env.update(random.__dict__)
        test_env.update(ambry.valuetype.math.__dict__)
        test_env.update(ambry.valuetype.string.__dict__)
        test_env.update(ambry.valuetype.types.__dict__)
        test_env.update(ambry.valuetype.exceptions.__dict__)
        test_env.update(ambry.valuetype.test.__dict__)

        localvars = {}

        for f_name, func in test_env.items():
            if not isinstance(func, (str, tuple) ):
                localvars[f_name] = set_from(func, 'env')

        # The 'b' parameter of randint is assumed to be a bundle, but
        # replacing it with a lambda prevents the param assignment
        localvars['randint'] = lambda a,b: random.randint(a,b)

        if self != Bundle:

            # Functions from the bundle
            base = set(inspect.getmembers(Bundle, predicate=inspect.isfunction))
            mine = set(inspect.getmembers(self.__class__, predicate=inspect.isfunction))

            localvars.update({f_name:set_from(func, 'bundle') for f_name, func in mine - base})

            # Bound methods. In python 2, these must be called referenced from the bundle, since
            # there is a difference between bound and unbound methods. In Python 3, there is no differnce,
            # so the lambda functions may not be necessary.
            base = set(inspect.getmembers(Bundle, predicate=inspect.ismethod))
            mine = set(inspect.getmembers(self.__class__, predicate=inspect.ismethod))

            # Functions are descriptors, and the __get__ call binds the function to its object to make a bound method
            localvars.update({f_name:set_from(func.__get__(self), 'bundle') for f_name, func in (mine - base)})

        # Bundle module functions
        module_entries = inspect.getmembers(sys.modules['ambry.build'], predicate=inspect.isfunction)
        localvars.update({f_name:set_from(func, 'module') for f_name, func in module_entries})

        return localvars

    def build_caster_code(self, source, source_headers, pipe = None):

        from ambry.etl.codegen import make_row_processors,make_env

        env_dict = self.exec_context()

        code = make_row_processors(pipe.bundle, source_headers, source.dest_table, env=env_dict)

        path = '/code/casters/{}.py'.format(source.name)

        self.build_fs.makedir(os.path.dirname(path), allow_recreate=True, recursive=True)
        self.build_fs.setcontents(path, code, encoding='utf8')

        # The abs_path is just for reporting line numbers in debuggers, etc.
        try:
            abs_path = self.build_fs.getsyspath(path)
        except:
            abs_path = '<string>'

        env_dict['bundle'] = self
        env_dict['source'] = source
        env_dict['pipe'] = pipe

        assert not pipe or (pipe.source is source and pipe.bundle is self)

        exec compile(code, abs_path, 'exec') in env_dict

        return env_dict['row_processors']


    def finalize_write_bundle_file(self):

        path = self.library.create_bundle_file(self)

        with open(path) as f:
            self.build_fs.makedir(os.path.dirname(self.identity.cache_key), allow_recreate=True)
            self.build_fs.setcontents(self.identity.cache_key + '.db', data=f)

        self.log('Wrote bundle sqlite file to {}'.format(path))

    def post_build_test(self):

        f = getattr(self, 'test', False)

        if f:
            try:
                f()
            except AssertionError:
                _, _, tb = sys.exc_info()
                traceback.print_tb(tb)  # Fixed format
                tb_info = traceback.extract_tb(tb)
                filename, line, func, text = tb_info[-1]
                self.error("Test case failed on line {} : {}".format(line, text))
                return False

    def post_build_time_coverage(self):
        """Collect all of the time coverage for the bundle."""
        from ambry.util.datestimes import expand_to_years

        years = set()

        # From the bundle about
        if self.metadata.about.time:
            for year in expand_to_years(self.metadata.about.time):
                years.add(year)

        # From the bundle name
        if self.identity.btime:
            for year in expand_to_years(self.identity.btime):
                years.add(year)

        # From all of the partitions
        for p in self.partitions:
            years |= set(p.time_coverage)

    def post_build_geo_coverage(self):
        """Collect all of the geocoverage for the bundle."""

        spaces = set()
        grains = set()

        def resolve(term):
            places = list(self.library.search.search_identifiers(term))

            if not places:
                raise BuildError(
                    "Failed to find space identifier '{}' in full text identifier search".format(term))

            return places[0].vid

        if self.metadata.about.space:  # From the bundle metadata
            spaces.add(resolve(self.metadata.about.space))

        if self.metadata.about.grain:  # From the bundle metadata
            grains.add(self.metadata.about.grain)

        if self.identity.bspace:  # And from the bundle name
            spaces.add(resolve(self.identity.bspace))

        # From all of the partitions
        for p in self.partitions.all:
            if 'geo_coverage' in p.record.data:
                for space in p.record.data['geo_coverage']:
                    spaces.add(space)

            if 'geo_grain' in p.record.data:
                for grain in p.record.data['geo_grain']:
                    grains.add(grain)

        def conv_grain(g):
            """Some grain are expressed as summary level names, not gvids."""
            try:
                c = GVid.get_class(g)
                return b(c().summarize())
            except NotASummaryName:
                return g

        self.metadata.coverage.geo = sorted(spaces)
        self.metadata.coverage.grain = sorted(conv_grain(g) for g in grains)

        self.metadata.write_to_dir()

    #
    # Finalize
    #

    @property
    def is_finalized(self):
        """Return True if the bundle is installed."""

        return self.state == self.STATES.FINALIZED or self.state == self.STATES.INSTALLED

    def do_finalize(self):
        """Call's finalize(); for similarity with other process commands. """
        return self.finalize()

    def finalize(self):
        self.state = self.STATES.FINALIZED
        self.commit()
        self.finalize_write_bundle_file()
        return True

    def import_tests(self):
        bsf = self.build_source_files.file(File.BSFILE.TEST)
        module = bsf.import_module(library=lambda: self.library,
                                   bundle=lambda: self)

        try:
            return module.Test
        except AttributeError:
            return None

    def run_tests(self, tests=None):
        """Run the unit tests in the test.py file"""

        import unittest

        if not self.test_class:
            self.test_class = self.import_tests()

        if tests:
            suite = unittest.TestSuite()
            for test in tests:
                suite.addTest(self.test_class(test))

            unittest.TextTestRunner(verbosity=2).run(suite)
        else:
            suite = unittest.TestLoader().loadTestsFromTestCase(self.test_class)
            unittest.TextTestRunner(verbosity=2).run(suite)

    def _run_events(self, tag, stage=None):
        """Run tests marked with a particular tag and stage"""

        self._run_event_methods(tag, stage)

        self._run_tests(tag, stage)


    def _run_event_methods(self, tag, stage=None):
        """Run code in the bundle that is marked with events. """
        import inspect
        from ambry.bundle.events import _runable_for_event

        funcs = []

        for func_name, f in inspect.getmembers(self, predicate=inspect.ismethod):
            if _runable_for_event(f, tag, stage):
                funcs.append(f)

        for func in funcs:
            func()

    def _run_tests(self,tag, stage=None):
        """Run test codes, defined in the test.py file, at event points"""
        import inspect
        import unittest
        from ambry.bundle.events import _runable_for_event
        import StringIO

        suite = unittest.TestSuite()

        if not self.test_class:
            self.test_class = self.import_tests()

        funcs = inspect.getmembers(self.test_class, predicate=inspect.ismethod)

        tests = []

        for func_name, f in funcs:
            if _runable_for_event(f, tag, stage):
                tests.append(self.test_class(f.__name__))

        if tests:
            suite.addTests(tests)
            stream = StringIO.StringIO()

            r = unittest.TextTestRunner(stream=stream,verbosity=2).run(suite)

            #self.log(stream.getvalue())

            self.log("Ran {} tests for tag '{}', {}{} failed, {} errors, {} skipped".format(
                r.testsRun, tag, ' stage {}, '.format(stage) if stage else '',
                len(r.failures), len(r.errors), len(r.skipped)
            ))
            for test, trace in r.failures:
                self.error('Test Failure: {} {}'.format(test, trace))

            for test, trace in r.errors:
                self.error('Test Error: {} {}'.format(test, trace))

            if len(r.failures) + len(r.errors) > 0:
                from ambry.dbexceptions import TestError
                self.set_error_state()
                self.dataset.config.build.state.test_error = [
                    (str(test), str(trc)) for test, trc in r.failures+r.errors
                ]
                raise TestError("Failed tests: {}"
                                .format(', '.join([str(test) for test, trc in r.failures+r.errors])))

    #
    # Check in to remote
    #

    def checkin(self):

        if self.is_built:
            self.finalize()

        if not self.is_finalized :
            self.error("Can't checkin; bundle state must be either finalized or prepared")
            return False, False

        self.commit()
        remote, path = self.library.checkin(self)

        return remote, path

    @property
    def is_installed(self):
        """Return True if the bundle is installed."""

        r = self.library.resolve(self.identity.vid)

        return r is not None

    def remove(self):
        """Delete resources associated with the bundle."""
        pass  # Remove files in the file system other resource.


