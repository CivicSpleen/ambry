"""The Bundle object is the root object for a bundle, which includes accessors
for partitions, schema, and the filesystem.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from ..util import get_logger
from ..dbexceptions import ConfigurationError, ProcessError
from ..util import Constant, memoize

class Bundle(object):

    def __init__(self, dataset, library, source_url=None, build_url=None):
        import logging

        self._dataset = dataset
        self._library = library
        self._logger = None


        assert bool(library)

        self._log_level = logging.INFO

        self._errors = []

        self._source_url = source_url
        self._build_url = build_url

        self._pipeline_editor = None # A function that can be set to edit the pipeline, rather than overriding the method


    def set_file_system(self, source_url=None, build_url=None):
        """Set the source file filesystem and/or build  file system"""

        assert isinstance(source_url, basestring) or source_url is  None
        assert isinstance(build_url, basestring) or build_url is  None

        if source_url:
            self._source_url = source_url
            self.dataset.config.library.source.url = self._source_url
            self.dataset.commit()

        if build_url:
            self._build_url = build_url
            self.dataset.config.library.build.url = self._build_url
            self.dataset.commit()


    def cast_to_subclass(self, clz):
        return clz(self._dataset, self._library, self._source_url, self._build_url)

    def cast_to_build_subclass(self):
        """
        Load the bundle file from the database to get the derived bundle class,
        then return a new bundle built on that class

        :return:
        """
        from ambry.orm import File
        bsf = self.build_source_files.file(File.BSFILE.BUILD)
        return self.cast_to_subclass(bsf.import_bundle_class())

    def cast_to_meta_subclass(self):
        """
        Load the bundle file from the database to get the derived bundle class,
        then return a new bundle built on that class

        :return:
        """
        from ambry.orm import File

        bsf = self.build_source_files.file(File.BSFILE.BUILDMETA)
        return self.cast_to_subclass(bsf.import_bundle_class())

    def commit(self):
        return self.dataset.commit()

    @property
    def dataset(self):
        from sqlalchemy import inspect

        if inspect(self._dataset).detached:
            vid = self._dataset.vid
            self._dataset = self._dataset._database.dataset(vid)

        return self._dataset

    @property
    def identity(self):
        return self.dataset.identity

    @property
    def schema(self):
        """Return the Schema acessor"""
        from schema import Schema
        return Schema(self)

    @property
    def partitions(self):
        """Return the Schema acessor"""
        from partitions import Partitions
        return Partitions(self)

    def source(self, name):
        source =  self.dataset.source_file(name)

        if not source:
            return None

        source._cache_fs = self._library.download_cache
        return source

    @property
    def sources(self):

        for source in self.dataset.sources:
            source._cache_fs = self._library.download_cache
            yield source

    @property
    def metadata(self):
        """Return the Metadata acessor"""
        return self.dataset.config.metadata

    @property
    def build_source_files(self):
        """Return acessors to the build files"""

        from files import BuildSourceFileAccessor
        return BuildSourceFileAccessor(self.dataset, self.source_fs)

    @property
    @memoize
    def source_fs(self):
        from fs.opener import fsopendir

        source_url = self._source_url if self._source_url else self.dataset.config.library.source.url 
        
        if not source_url:
            raise ConfigurationError('Must set source URL either in the constructor or the configuration')
        
        return fsopendir(source_url)

    @property
    @memoize
    def build_fs(self):
        from fs.opener import fsopendir

        build_url = self._build_url if self._build_url else self.dataset.config.library.build.url
        
        if not build_url:
            raise ConfigurationError('Must set build URL either in the constructor or the configuration')

        return fsopendir(build_url)

    def do_pipeline(self, source=None):
        """COnstruct the meta pipeline. This method shold not be overridden; iverride meta_pipeline() instead. """
        source = self.source(source) if isinstance(source, basestring) else source


        pl = self.pipeline(source)

        self.edit_pipeline(pl)

        return pl

    def pipeline(self, source=None):
        """Construct the ETL pipeline for all phases. Segments that are not used for the current phase
        are filtered out later. """
        from ambry.etl.pipeline import Pipeline, MergeHeader, MangleHeader, WriteToPartition
        from ambry.etl.intuit import TypeIntuiter
        from ambry.etl.stats import Stats

        if source:
            source = self.source(source) if isinstance(source, basestring) else source
        else:
            source = None

        return Pipeline(
                bundle = self,
                source=source.source_pipe() if source else None,
                source_first=None,
                source_row_intuit=None,
                source_coalesce_rows=MergeHeader(),
                source_type_intuit=TypeIntuiter(),
                source_last=None,
                write_source_schema=None,
                schema_first=None,
                source_map_header=MangleHeader(),
                dest_map_header=None,
                dest_cast_columns=None,
                dest_augment=None,
                schema_last=None,
                write_dest_schema=None,
                build_first=None,
                dest_statistics=Stats(),
                build_last=None,
                write_to_table= WriteToPartition()
        )

    def set_edit_pipeline(self, f):
        """Set a function to edit the pipeline"""

        self._pipeline_editor = f

    def edit_pipeline(self, pipeline):
        """Called after the meta pipeline is constructed, to allow per-pipeline modification."""

        if self._pipeline_editor:
            self._pipeline_editor(pipeline)

        return pipeline

    @property
    def logger(self):
        """The bundle logger."""
        import sys

        if not self._logger:

            ident = self.identity
            template = "%(levelname)s " + ident.sname + " %(message)s"

            self._logger = get_logger(__name__, template=template, stream=sys.stdout)

            self._logger.setLevel(self._log_level)

        return self._logger

    def log(self, message, **kwargs):
        """Log the messsage."""
        self.logger.info(message)

    def error(self, message):
        """Log an error messsage.

        :param message:  Log message.

        """
        if message not in self._errors:
            self._errors.append(message)

        self.logger.error(message)

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
        import sys

        self.logger.fatal(message)
        sys.stderr.flush()
        if self.exit_on_fatal:
            sys.exit(1)
        else:
            from ..dbexceptions import FatalError

            raise FatalError(message)

    ##
    ## Build Process
    ##

    STATES = Constant()
    STATES.NEW = 'new'
    STATES.SYNCED = 'synced'
    STATES.DOWNLOADED = 'downloaded'
    STATES.CLEANING = 'cleaning'
    STATES.CLEANED = 'cleaned'
    STATES.PREPARING = 'preparing'
    STATES.PREPARED = 'prepared'
    STATES.BUILDING = 'building'
    STATES.BUILT = 'built'
    STATES.FINALIZING = 'finalizing'
    STATES.FINALIZED = 'finalized'
    STATES.INSTALLING = 'installing'
    STATES.INSTALLED = 'installed'

    ##
    ## Source Synced
    ##

    def do_sync(self, force = None, defaults = False):
        ds = self.dataset

        syncs  = self.build_source_files.sync(force, defaults)

        self.state = self.STATES.SYNCED
        self.log("---- Synchronized ----")
        self.dataset.commit()

        return syncs

    def sync(self, force=None, defaults = False):
        """

        :param force: Force a sync in one direction, either ftr or rtf.
        :param defaults [False] If True and direction is rtf, write default source files
        :return:
        """
        self.do_sync(force, defaults = defaults)

        return True

    ##
    ## Clean
    ##

    def do_clean(self):

        self.state = self.STATES.CLEANING

        if self.clean():
            if self.post_clean():
                self.log("---- Done Cleaning ----")
                self.state = self.STATES.CLEANED
                r = True
            else:
                self.log("---- Post-cleaning ended in Failure ----")
                self.set_error_state()
                self.state = self.STATES.CLEANING  # the clean() method removes states, so we put it back
                r = False

        else:
            self.log("---- Cleaning ended in failure ----")
            self.state = self.STATES.CLEANING  # the clean() method removes states, so we put it back
            self.set_error_state()
            r = False

        self.dataset.commit()
        return r

    def clean(self):
        """Clean generated objects from the dataset, but only if there are File contents
         to regenerate them"""
        from ambry.orm import ColumnStat, File
        from sqlalchemy.orm import object_session

        ds = self.dataset
        s = object_session(ds)

        # FIXME. There is a problem with the cascades for ColumnStats that prevents them from beling deleted with the
        # partitions. Probably, the are seen to be owed by the columns instead.
        s.query(ColumnStat).filter(ColumnStat.d_vid == ds.vid).delete()

        self.dataset.partitions[:] = []

        if ds.bsfile(File.BSFILE.SOURCES).has_contents:
            self.dataset.sources[:] = []

        if ds.bsfile(File.BSFILE.SOURCESCHEMA).has_contents:
            self.dataset.source_tables[:] = []

        if ds.bsfile(File.BSFILE.SCHEMA).has_contents:
            self.dataset.tables[:] = []

        if ds.bsfile(File.BSFILE.COLMAP).has_contents:
            self.dataset.colmaps[:] = []

        #ds.config.metadata.clean()
        ds.config.build.clean()
        ds.config.process.clean()

        ds.commit()

        return True

    def post_clean(self):
        pass
        return True

    def download(self):
        """Download referenced source files and bundles. BUndles will be instlaled in the warehouse"""

        raise NotImplementedError()

        for source in self.dataset.sources:
            print source


    ##
    ## Prepare
    ##

    @property
    def is_prepared(self):
        return bool(self.dataset.config.build.state.prepared)

    def do_prepare(self):
        """This method runs pre_, main and post_ prepare methods."""

        r = True

        if self.pre_prepare():
            self.state = self.STATES.PREPARING
            self.log("---- Preparing ----")
            if self.prepare_main():
                self.state = self.STATES.PREPARED
                if self.post_prepare():
                    self.log("---- Done Preparing ----")
                else:
                    self.set_error_state()
                    self.log("---- Post-prepare exited with failure ----")
                    r = False
            else:
                self.set_error_state()
                self.log("---- Prepare exited with failure ----")
                r = False
        else:
            self.log("---- Skipping prepare ---- ")

        self.dataset.commit()
        return r

    def prepare_main(self):
        """This is the methods that is actually called in do_prepare; it
        dispatches to developer created prepare() methods."""
        return self.prepare()

    def prepare(self):
        from ambry.orm.exc import NotFoundError
        from ambry.orm import File

        try:
            # Implement later ...
            #self.sources.check_dependencies()
            pass
        except NotFoundError as e:
            self.error(e.message)
            return False

        self.build_source_files.file(File.BSFILE.META).record_to_objects()
        # SOURCES must come before SOURCESCHEMA, to create required source_tables.
        # The SOURCESCHEMA r_to_o proess won't load columns for source tables that weren't created
        # beforehand.
        self.build_source_files.file(File.BSFILE.SOURCES).record_to_objects()
        self.build_source_files.file(File.BSFILE.SOURCESCHEMA).record_to_objects()
        self.build_source_files.file(File.BSFILE.SCHEMA).record_to_objects()

        self.commit()

        return True

    def pre_prepare(self):
        """"""
        return True

    def post_prepare(self):
        """"""

        for t in self.schema.tables:
            if not bool(t.description.strip()):
                self.error("No title ( Description of id column ) set for table: {} ".format(t.name))

        return True

    ##
    ## Meta
    ##

    def do_meta(self, source_name = None, force=False):
        """
        Synchronize with the files and run the meta pipeline, possibly creating new objects. Then, write the
        objects back to file records and synchronize.

        :param force:
        :return:
        """
        from ambry.orm.file import File

        self.do_sync()

        self.do_prepare()

        self.log("---- Meta ---- ")

        self.meta(source_name=source_name)

        self.build_source_files.file(File.BSFILE.SOURCES).objects_to_record()
        self.build_source_files.file(File.BSFILE.SOURCESCHEMA).objects_to_record()
        self.build_source_files.file(File.BSFILE.SCHEMA).objects_to_record()

        self.do_sync()

    def meta_make_source_tables(self, pl):
        from ambry.etl.intuit import TypeIntuiter

        ti = pl[TypeIntuiter]

        source = pl.source.source

        if not source.st_id:

            for c in ti.columns:
                source.source_table.add_column(c.position, source_header=c.header, dest_header=c.header,
                                               datatype=c.resolved_type)

    def meta_make_dest_tables(self, pl):

        source = pl.source.source
        dest = pl.source.source.dest_table

        for c in source.source_table.columns:

            dest.add_column(name=c.dest_header, datatype =  c.column_datatype,
                            derivedfrom=c.dest_header,
                            summary=c.summary, description=c.description)

    def meta_log_pipeline(self, pl):
        """Write a report of the pipeline out to a file """
        import os

        self.build_fs.makedir('pipeline', allow_recreate=True)
        self.build_fs.setcontents(os.path.join('pipeline','meta-'+pl.file_name+'.txt' ), unicode(pl), encoding='utf8')

    def meta(self, source_name = None, phase = 'all'):
        from ambry.etl.pipeline import PrintRows

        for i, source in enumerate(self.sources):

            if source_name and source.name != source_name:
                self.logger.info("Skipping {} ( only running {} ) ".format(source.name, source_name))
                continue

            self.logger.info("Running meta for source: {} ".format(source.name))

            if phase in ('source','all'):
                pl = self.do_pipeline(source).source_phase
                pl.run()

                self.meta_log_pipeline(pl)
                self.meta_make_source_tables(pl)
                self.meta_make_dest_tables(pl)

            if phase in ('schema','all'):
                pl = self.do_pipeline(source).schema_phase
                pl.run()

        source.dataset.commit()

    ##
    ## Build
    ##

    @property
    def is_built(self):
        """Return True is the bundle has been built."""
        return bool(self.dataset.config.build.state.built)

    def do_build(self, table_name = None, force=False):

        if not self.is_prepared:
            if not self.do_prepare():
                self.log("Prepare failed; skipping build")
                return False

        self.state = self.STATES.BUILDING
        if self.pre_build(force):

            self.log("---- Build ---")
            if self.build_main(table_name):
                self.state = self.STATES.BUILT
                if self.post_build():
                    self.log("---- Done Building ---")
                    r = True
                else:
                    self.log("---- Post-build failed ---")
                    self.set_error_state()
                    r = False

            else:
                self.log("---- Build exited with failure ---")
                self.set_error_state()
                r = False
        else:
            self.log("---- Skipping Build ---- ")
            self.set_error_state()
            r = False

        return r

    # Build the final package
    def pre_build(self, force = False):

        if self.is_built and not force:
            self.error("Bundle is already built. Skipping  ( Use --clean  or --force to force build ) ")
            return False

        if not self.is_prepared:
            self.error("Build called before prepare completed")
            return False

        # python_dir = self.config.python_dir()
        # if python_dir and python_dir not in sys.path:
        #    sys.path.append(python_dir)

        return True

    def build(self, table_name = None, source_name = None):

        for i, source in enumerate(self.sources):

            if table_name and source.dest_table.name != table_name:
                self.logger.info("Skipping source {}, table {} ( only running table {} ) "
                                 .format(source.name, source.dest_table.name, table_name))
                continue

            self.logger.info("Running build for table: {} ".format(source.dest_table.name))

            pl = self.do_pipeline(source).build_phase

            pl.run()

            self.build_post_build_source(pl)

        source.dataset.commit()

        return True

    def build_post_build_source(self, pl):
        pass

    def build_main(self, table_name = None):
        """This is the methods that is actually called in do_build; it
        dispatches to developer created prepare() methods."""
        return self.build(table_name = table_name)

    def post_build(self):
        """After the build, update the configuration with the time required for
        the build, then save the schema back to the tables, if it was revised
        during the build."""

        return True


    ##
    ## Update
    ##

    # Update is like build, but calls into an earlier version of the package.
    def pre_update(self):
        from time import time

        if not self.database.exists():
            raise ProcessError(
                "Database does not exist yet. Was the 'prepare' step run?")

        if not self.get_value('process', 'prepared'):
            raise ProcessError("Update called before prepare completed")

        self._update_time = time()

        self._build_time = time()

        return True

    def update_main(self):
        """This is the methods that is actually called in do_update; it
        dispatches to developer created update() methods."""
        return self.update()

    def update(self):

        self.update_copy_schema()
        self.prepare()
        self.update_copy_partitions()

        return True

    ##
    ## Finalize
    ##

    def post_build_test(self):

        f = getattr(self, 'test', False)

        if f:
            try:
                f()
            except AssertionError:
                import traceback
                import sys

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
        for p in self.partitions.all:
            if 'time_coverage' in p.record.data:
                for year in p.record.data['time_coverage']:
                    years.add(year)

        self.metadata.coverage.time = sorted(years)

        self.metadata.write_to_dir()

    def post_build_geo_coverage(self):
        """Collect all of the geocoverage for the bundle."""
        from ..dbexceptions import BuildError
        from geoid.civick import GVid
        from geoid import NotASummaryName

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
                return str(c().summarize())
            except NotASummaryName:
                return g

        self.metadata.coverage.geo = sorted(spaces)
        self.metadata.coverage.grain = sorted(conv_grain(g) for g in grains)

        self.metadata.write_to_dir()

    def finalize(self):
        self.state = self.STATES.FINALIZED

    ##
    ## Install
    ##

    @property
    def is_installed(self):
        """Return True if the bundle is installed."""

        r = self.library.resolve(self.identity.vid)

        return r is not None

    ##
    ##
    ##

    def remove(self):
        """Delete resources associated with the bundle."""
        pass # Remove files in the file system other resource.


    #######
    #######

    def field_row(self, fields):
        """Return a list of values to match the fielsds values"""

        row = self.dataset.row(fields)

        for i, f in enumerate(fields):
            if f == 'state':
                row[i] = self.state

        return row

    def clear_states(self):
        return self.dataset.config.build.clean()

    @property
    def state(self):
        return self.dataset.config.build.state.current

    @property
    def error_state(self):
        """Set the error condition"""
        from time import time

        self.dataset.config.build.state.lastime = time()
        return self.dataset.config.build.state.error

    @state.setter
    def state(self, state):
        """Set the current build state and record the tim eto maintain history"""
        from time import time

        self.dataset.config.build.state.current = state
        self.dataset.config.build.state.error = False
        self.dataset.config.build.state[state] = time()
        self.dataset.config.build.state.lastime = time()

    def set_error_state(self):
        from time import time

        self.dataset.config.build.state.error = time()

    def set_last_access(self, tag):
        """Mark the time that this bundle was last accessed"""

        self.dataset.config.build.access.last = tag
        self.dataset.commit()
