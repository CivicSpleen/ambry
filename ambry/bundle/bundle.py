"""The Bundle object is the root object for a bundle, which includes accessors
for partitions, schema, and the filesystem.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from ..util import get_logger
from ..dbexceptions import ConfigurationError, ProcessError
from ..util import Constant, memoize
import ambry.etl

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

    # Other things that can be part of the 'last action'
    STATES.INFO = 'info'

    # Default body content for pipelines
    default_pipelines = {
        # Classifies rows in multi-header sources
        'rowintuit': {
            'body':[
                ambry.etl.RowIntuiter
            ]
        },
        # Creates the source schemas
        'source': {
            'body' :[
                ambry.etl.MergeHeader,
                ambry.etl.MangleHeader,
                ambry.etl.TypeIntuiter,
            ],
            'final': [
                'log_pipeline',
                'meta_make_source_tables'
            ]

        },
        'schema': {
            'body': [
                ambry.etl.MergeHeader,
                ambry.etl.MangleHeader,
                ambry.etl.MapToSourceTable,
                ambry.etl.TypeIntuiter
            ],
            'final': [
                'log_pipeline',
                'meta_make_dest_tables'
            ]

        },
        'build': {
            'body': [
                ambry.etl.MergeHeader,
                ambry.etl.MangleHeader,
                ambry.etl.MapToSourceTable,

            ],
            'store':[
                ambry.etl.CasterPipe,
                ambry.etl.SelectPartition,
                ambry.etl.WriteToPartition
            ],
            'final': [
                'log_pipeline',
                'build_post_build_source'
            ]

        },
    }



    def __init__(self, dataset, library, source_url=None, build_url=None):
        import logging

        self._dataset = dataset
        self._library = library
        self._logger = None

        assert bool(library)

        self._log_level = logging.INFO

        self._errors = []
        self._warnings = []

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
        return self.cast_to_subclass(bsf.import_bundle())

    def cast_to_meta_subclass(self):
        """
        Load the bundle file from the database to get the derived bundle class,
        then return a new bundle built on that class

        :return:
        """
        from ambry.orm import File

        bsf = self.build_source_files.file(File.BSFILE.BUILDMETA)
        return self.cast_to_subclass(bsf.import_bundle())

    def import_lib(self):
        """Import the lib.py file from the bundle"""
        from ambry.orm import File
        self.build_source_files.file(File.BSFILE.LIB).import_lib()

    def load_requirements(self):
        """If there are python library requirements set, append the python dir
        to the path."""

        import sys

        for module_name, pip_name in self.metadata.requirements.items():
            self._library.install_packages(module_name, pip_name)

        python_dir = self._library.filesystem.python()
        sys.path.append(python_dir)

    def commit(self):
        return self.dataset.commit()

    @property
    def session(self):
        from sqlalchemy.orm import object_session
        return object_session(self.dataset)

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
    def library(self):
        return self._library

    @property
    def partitions(self):
        """Return the Schema acessor"""
        from partitions import Partitions
        return Partitions(self)

    def partition(self, ref):
        """Return the Schema acessor"""
        from partitions import Partitions
        for p in self.partitions:
            if p.vid == str(ref):
                return p
        return None

    def table(self, ref):
        return self.dataset.table(ref)

    def wrap_partition(self, p):
        from partitions import PartitionProxy

        if isinstance(p, PartitionProxy):
            return p

        return PartitionProxy(self, p)

    def delete_partition(self, vid_or_p):

        try:
            vid = vid_or_p.vid
        except AttributeError:
            vid = vid_or_p

        vid = vid_or_p.vid

        p = self.partition(vid)

        self.dataset._database.session.delete(p._partition)

    def source(self, name):
        source =  self.dataset.source_file(name)

        if not source:
            return None

        source._cache_fs = self._library.download_cache
        source._library = self._library
        return source

    @property
    def sources(self):

        for source in self.dataset.sources:

            # Dun't terate over references to other bundles
            if source.ref:
                continue

            source._cache_fs = self._library.download_cache
            source._library = self._library
            yield source

    @property
    def metadata(self):
        """Return the Metadata acessor"""
        return self.dataset.config.metadata

    @property
    def build_source_files(self):
        """Return acessors to the build files"""

        from files import BuildSourceFileAccessor
        return BuildSourceFileAccessor(self, self.dataset, self.source_fs)

    @property
    @memoize
    def source_fs(self):
        from fs.opener import fsopendir
        from fs.errors import ResourceNotFoundError

        source_url = self._source_url if self._source_url else self.dataset.config.library.source.url 
        
        if not source_url:
            source_url = self.library.filesystem.source(self.identity.cache_key)

        try:
            return fsopendir(source_url)
        except ResourceNotFoundError:
            self.logger.warn("Failed to locate source dir {}; using default".format(source_url))
            source_url = self.library.filesystem.source(self.identity.cache_key)
            return fsopendir(source_url)

    @property
    @memoize
    def build_fs(self):
        from fs.opener import fsopendir

        build_url = self._build_url if self._build_url else self.dataset.config.library.build.url
        
        if not build_url:
            build_url = self.library.filesystem.build(self.identity.cache_key)
            #raise ConfigurationError('Must set build URL either in the constructor or the configuration')

        return fsopendir(build_url)

    def phase_search_names(self, source, phase):
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

        self.set_error_state()
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
    ## Source Synced
    ##

    def sync(self, force=None, defaults = False):
        """

        :param force: Force a sync in one direction, either ftr or rtf.
        :param defaults [False] If True and direction is rtf, write default source files
        :return:
        """

        if self.is_finalized:
            self.error("Can't sync; bundle is finalized")
            return False

        if self.is_finalized:
            self.error("Can't sync; bundle is finalized")
            return False

        ds = self.dataset

        syncs = self.build_source_files.sync(force, defaults)

        self.state = self.STATES.SYNCED
        self.log("---- Synchronized ----")
        self.dataset.commit()

        return syncs

    def sync_in(self):
        """Synchronize from files to records, and records to objects"""
        from ambry.orm import File
        from ambry.bundle.files import BuildSourceFile
        syncs = self.build_source_files.sync(BuildSourceFile.SYNC_DIR.FILE_TO_RECORD)
        self.build_source_files.record_to_objects()
        self.state = self.STATES.SYNCED

    def sync_objects_in(self):
        """Synchronize from records to objects"""
        from ambry.orm import File
        from ambry.bundle.files import BuildSourceFile

        self.build_source_files.record_to_objects()

    def sync_out(self):
        from ambry.bundle.files import BuildSourceFile
        """Synchronize from objects to records"""
        self.build_source_files.objects_to_record()
        self.build_source_files.sync(BuildSourceFile.SYNC_DIR.RECORD_TO_FILE)
        self.state = self.STATES.SYNCED


    def sync_objects_out(self):
        from ambry.bundle.files import BuildSourceFile
        """Synchronize from objects to records, and records to files"""

        self.build_source_files.objects_to_record()


    def sync_objects(self):
        self.build_source_files.record_to_objects()
        self.build_source_files.objects_to_record()


    ##
    ## Do All; Run the full process

    def run(self):

        if self.is_finalized:
            self.error("Can't run; bundle is finalized")
            return False

        self.sync_in()

        if not self.meta():
            self.error('Run: failed to meta')
            return False

        if not self.build():
            self.error('Run: failed to build')
            return False

        self.sync_out()

        self.finalize()

        return True

    ##
    ## Clean
    ##

    @property
    def is_clean(self):
        return self.state == self.STATES.CLEANED

    def clean(self):

        """Clean generated objects from the dataset, but only if there are File contents
         to regenerate them"""
        from ambry.orm import ColumnStat, File
        from sqlalchemy.orm import object_session

        if self.is_finalized:
            self.warn("Can't clean; bundle is finalized")
            return False

        self.log("---- Cleaning ----")
        self.state = self.STATES.CLEANING

        ds = self.dataset
        s = object_session(ds)

        # FIXME. There is a problem with the cascades for ColumnStats that prevents them from
        # being  deleted with the partitions. Probably, the are seen to be owed by the columns instead.
        s.query(ColumnStat).filter(ColumnStat.d_vid == ds.vid).delete()

        self.dataset.partitions[:] = []

        ds.commit()

        for src in self.dataset.sources:
            src.st_id = None
            src.t_id = None

        ds.commit()

        if ds.bsfile(File.BSFILE.SOURCES).has_contents:
            self.dataset.sources[:] = []

        ds.commit()

        if ds.bsfile(File.BSFILE.SOURCESCHEMA).has_contents:
            self.dataset.source_tables[:] = []

        if ds.bsfile(File.BSFILE.SCHEMA).has_contents:
            self.dataset.tables[:] = []

        ds.config.build.clean()
        ds.config.process.clean()

        ds.commit()

        self.state = self.STATES.CLEANED

        self.log("---- Done Cleaning ----")

        return True

    ##
    ## Prepare
    ##

    @property
    def is_prepared(self):
        return self.state == Bundle.STATES.PREPARED

    def prepare(self):
        """This method runs pre_, main and post_ prepare methods."""

        self.load_requirements()

        self.import_lib()

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
            r = False


        self.dataset.commit()

        return r

    def prepare_main(self):

        return True

    def pre_prepare(self):
        """"""
        from ambry.orm import File

        if self.is_finalized:
            self.warn("Can't prepare; bundle is finalized")
            return False

        return True

    def post_prepare(self):
        """"""

        for t in self.dataset.tables:
            if not bool(t.description):
                self.error("No title ( Description of id column ) set for table: {} ".format(t.name))

        self.commit()

        return True

    ##
    ## General Phase Runs
    ##

    def pipeline(self, phase, source=None):
        """Construct the ETL pipeline for all phases. Segments that are not used for the current phase
        are filtered out later. """

        from ambry.etl.pipeline import Pipeline, PartitionWriter

        if source:
            source = self.source(source) if isinstance(source, basestring) else source
        else:
            source = None

        pl = Pipeline(self, source=source.source_pipe() if source else None)

        try:
            phase_config = self.default_pipelines[phase]
        except KeyError:
            phase_config = None  # Ok for non-conventional pipe names

        if phase_config:
            pl.configure(phase_config)

        body = []

        # Find the pipe configuration:
        pipe_config = None
        pipe_name = None
        for name in self.phase_search_names(source, phase):
            if name in self.metadata.pipelines:
                pipe_config = self.metadata.pipelines[name]
                pipe_name = name
                break


        # The pipe_config can either be a list, in which case it is a list of pipe pipes for the body segment
        # or it could be a dict, in which case each is a list of pipes for the named segments.

        if isinstance(pipe_config, (list, tuple)):
            # Just convert it to dict form for the next section

            # PartitionWriters are always moved to the 'store' section
            store, body = [], []

            for pipe in pipe_config:
                store.append(pipe) if isinstance(pipe, PartitionWriter) else body.append(pipe)

            pipe_config = dict(body=body, store=store)

        if pipe_config:
            pl.configure(pipe_config)

        if pipe_name:
            pl.name = pipe_name
        else:
            pl.name = phase

        pl.phase = phase

        self.edit_pipeline(pl)

        return pl

    def set_edit_pipeline(self, f):
        """Set a function to edit the pipeline"""

        self._pipeline_editor = f

    def edit_pipeline(self, pipeline):
        """Called after the meta pipeline is constructed, to allow per-pipeline modification."""

        if self._pipeline_editor:
            self._pipeline_editor(pipeline)

        return pipeline

    def pre_phase(self, phase, force=False):

        if self.is_built and not force:
            self.error("Bundle is already built. Skipping  ( Use --clean  or --force to force build ) ")
            return False

        if self.is_finalized and not force:
            self.error("Can't build; bundle is finalized")
            return False

        self.load_requirements()

        self.import_lib()

        return True

    def phase_main(self, phase,  sources=None):
        """
        Synchronize with the files and run the meta pipeline, possibly creating new objects. Then, write the
        objects back to file records and synchronize.

        :param force:
        :return:
        """
        from ambry.orm.file import File

        if self.is_finalized:
            self.error("Can't run phase {}; bundle is finalized".format(phase))
            return False

        if self.is_built:
            self.error("Can't run phase {}; bundle is built".format(phase))
            return False

        self.import_lib()
        self.load_requirements()

        self.log("---- Phase {} ---- ".format(phase))

        for i, source in enumerate(self.sources):

            if sources and source.name not in sources:
                continue

            self.logger.info("Running phase {} for source {} ".format(phase, source.name))

            pl = self.pipeline(phase, source)

            pl.run()

            for m in pl.final:
                self.log("Run final method {}".format(m))
                getattr(self, m)(pl)

        self.dataset.commit()


        return True

    def post_phase(self, phase):
        """After the build, update the configuration with the time required for
        the build, then save the schema back to the tables, if it was revised
        during the build."""

        return True

    def run_phase(self, phase, sources = None):

        phase_pre_name = 'pre_{}'.format(phase)
        phase_post_name = 'post_{}'.format(phase)

        if hasattr(self, phase_pre_name):
            phase_pre = getattr(self, phase_pre_name)
        else:
            phase_pre = self.pre_phase

        if hasattr(self, phase_post_name):
            phase_post = getattr(self, phase_post_name)
        else:
            phase_post = self.post_phase

        if phase_pre(phase):
            self.state = phase
            self.log("---- Phase: {} ---".format(phase))
            if self.phase_main(phase):

                if phase_post(phase):
                    self.log("---- Done {} ---".format(phase))
                    r = True
                else:
                    self.log("---- {} failed ---".format(phase_post_name))
                    self.set_error_state()
                    r = False

            else:
                self.log("---- Phase {} exited with failure ---".format(phase))
                self.set_error_state()
                r = False
        else:
            self.log("---- Skipping {} ---- ".format(phase))
            self.set_error_state()
            r = False

        if r:

            self.state = phase+'_done'

        return r

    def log_pipeline(self, pl):
        """Write a report of the pipeline out to a file """
        import os

        self.build_fs.makedir('pipeline', allow_recreate=True)
        self.build_fs.setcontents(os.path.join('pipeline', pl.phase + '-' + pl.file_name + '.txt'),
                                  unicode(pl), encoding='utf8')

    ##
    ## Meta
    ##

    def meta(self, sources=None):

        r = self.run_phase('source',sources=sources)

        if r:
            r = self.run_phase('schema',sources=sources)

        return r

    def meta_schema(self, sources=None):
        return self.run_phase('schema', sources=sources)

    def meta_source(self, sources=None):
        return self.run_phase('source', sources=sources)

    def meta_make_source_tables(self, pl):
        from ambry.etl.intuit import TypeIntuiter

        ti = pl[TypeIntuiter]

        source = pl.source.source
        st = source.source_table
        if not source.st_id:
            for tic in ti.columns:
                c = st.column(tic.header)
                if c:
                    c.datatype = TypeIntuiter.promote_type(c.datatype, tic.resolved_type)
                else:
                    st.add_column(tic.position, source_header=tic.header, dest_header=tic.header,
                                               datatype=tic.resolved_type_name)

    def meta_make_dest_tables(self, pl):

        source = pl.source.source
        dest = pl.source.source.dest_table

        for c in source.source_table.columns:

            dest.add_column(name=c.dest_header, datatype =  c.column_datatype,
                            derivedfrom=c.dest_header,
                            summary=c.summary, description=c.description)

    ##
    ## Build
    ##

    @property
    def is_built(self):
        """Return True is the bundle has been built."""
        return self.state == self.STATES.BUILT

    def build(self,  sources = None):
        return self.run_phase('build',sources)

    def post_build(self,phase):
        """After the build, update the configuration with the time required for
        the build, then save the schema back to the tables, if it was revised
        during the build."""

        self.build_post_unify_partitions()

        self.build_post_write_bundle_file()

        return True

    def build_post_unify_partitions(self):
        """For all of the segments for a partition, create the parent partition, combine the children into the parent,
        and delete the children. """

        from collections import defaultdict
        from ..orm.partition import Partition
        from ..etl.stats import Stats
        import time

        # Group the segments by their parent partition name, which is the
        # same name, without the segment.
        partitions = defaultdict(set)
        for p in self.dataset.partitions:
            if p.type == p.TYPE.SEGMENT:
                name = p.identity.name
                name.segment = None
                partitions[name].add(p)

        # For each group, copy the segment partitions to the parent partitions, then
        # delete the segment partitions.

        for name, segments in partitions.items():

            self.log("Coalescing segments for partition {} ".format(name))

            parent = self.partitions.get_or_new_partition(name, type = Partition.TYPE.UNION)
            stats = Stats(parent.table)

            pdf = parent.datafile
            for seg in sorted(segments, key = lambda s: str(s.name)):

                self.log("Coalescing segment  {} ".format(seg.identity.name))

                reader = self.wrap_partition(seg).datafile.reader()
                header = None
                start_time = time.time()
                for i, row in enumerate(reader):

                    if header is None:
                        header = row
                        pdf.insert_header(row)
                        stats.process_header(row)
                    else:
                        pdf.insert_body(row)
                        stats.process_body(row)

                reader.close()
                self.log("Coalesced {} rows, {} rows/sec ".format(i, float(i)/(time.time()-start_time)))

            pdf.close()
            self.commit()
            parent.finalize(stats)
            self.commit()

            for s in segments:
                self.wrap_partition(s).datafile.delete()
                self.dataset._database.session.delete(s)

    def build_post_build_source(self, pl):

        from ..etl import PartitionWriter

        try:
            for p in pl[PartitionWriter].partitions:
                self.logger.info("Finalizing partition {}".format(p.identity.name))
                # We're passing the datafile path into filanize b/c finalize is on the ORM object, and datafile is
                # on the proxy.
                p.finalize()
                # FIXME SHouldn't need to do this commit, but without it, somce stats get added multiple
                # times, causing an error later. Probably could be avoided by adding the states to the
                # collection in the dataset
                self.commit()

        except IndexError:
            self.error("Pipeline didn't have a PartitionWriters, won't try to finalize")

        self.commit()

    def build_post_write_bundle_file(self):
        import os

        path = self.library.create_bundle_file(self)

        with open(path) as f:
            self.build_fs.makedir(os.path.dirname(self.identity.cache_key), allow_recreate=True)
            self.build_fs.setcontents(self.identity.cache_key+'.db', data = f)

        self.log("Wrote bundle sqlite file to {}".format(path))

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

    ##
    ## Finalize
    ##

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
        return True

    ##
    ## check in to remote
    ##

    def checkin(self):

        if self.is_built:
            self.finalize()

        if not ( self.is_finalized or self.is_prepared):
            self.error("Can't checkin; bundle state must be either finalized or prepared")
            return False

        self.commit()
        remote, path = self.library.checkin(self)

        return remote, path

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
