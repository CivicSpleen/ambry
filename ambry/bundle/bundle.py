"""The Bundle object is the root object for a bundle, which includes accessors
for partitions, schema, and the filesystem.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import os
import sys
from time import time
import traceback

from six import string_types, iteritems, u, b

from geoid.civick import GVid
from geoid import NotASummaryName

from ambry.dbexceptions import PhaseError, BuildError, BundleError, FatalError

import ambry.etl
from ..util import get_logger, Constant

indent = '    '  # Indent for structured log output


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
            'intuit': [
                ambry.etl.RowIntuiter
            ]
        },
        # Creates the source schemas
        'source': {
            'body': [
                ambry.etl.MergeHeader,
                ambry.etl.MangleHeader,
            ],
            'intuit': [
                ambry.etl.TypeIntuiter,
            ],
            'final': [
                'final_log_pipeline',
                'final_make_source_tables'
            ]

        },
        'schema': {
            'body': [
                ambry.etl.MergeHeader,
                ambry.etl.MangleHeader
            ],
            'cast': [
                ambry.etl.MapToSourceTable
            ],
            'intuit': [
                ambry.etl.TypeIntuiter,
            ],
            'final': [
                'final_log_pipeline',
                'final_make_dest_tables'
            ]

        },
        'build': {
            'body': [
                ambry.etl.MergeHeader,
                ambry.etl.MangleHeader,
                ambry.etl.MapToSourceTable
            ],
            'cast': [
                ambry.etl.CasterPipe
            ],
            'store': [
                ambry.etl.SelectPartition,
                ambry.etl.WriteToPartition
            ],
            'final': [
                'final_log_pipeline',
                'final_finalize_segments',
                'final_cast_errors'
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

        self._pipeline_editor = None  # A function that can be set to edit the pipeline, rather than overriding the method

        self._source_fs = None
        self._build_fs = None

    def set_file_system(self, source_url=None, build_url=None):
        """Set the source file filesystem and/or build  file system"""

        assert isinstance(source_url, string_types) or source_url is None
        assert isinstance(build_url, string_types) or build_url is None

        if source_url:
            self._source_url = source_url
            self.dataset.config.library.source.url = self._source_url
            self.dataset.commit()

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
        from ambry.orm import File
        bsf = self.build_source_files.file(File.BSFILE.BUILD)
        try:
            clz = bsf.import_bundle()

            return clz(self._dataset, self._library, self._source_url, self._build_url)

        except Exception as e:
            raise BundleError("Failed to load bundle code file, skipping : {}".format(e))

    def import_lib(self):
        """Import the lib.py file from the bundle"""
        from ambry.orm import File
        self.build_source_files.file(File.BSFILE.LIB).import_lib()

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
        return self.dataset._database.session.rollback()

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

    def dep(self, source_name):
        """Return a bundle dependency from the sources list

        :param source_name: Source name. The URL field must be a bundle or partition reference
        :return:
        """

        source = self.source(source_name)

        return self.library.partition(source.url)

    @property
    def partitions(self):
        """Return the Schema acessor"""
        from .partitions import Partitions
        return Partitions(self)

    def partition(self, ref):
        """Return the Schema acessor"""
        for p in self.partitions:
            if p.vid == b(ref) or p.name == b(ref):
                return p
        return None

    def table(self, ref):
        return self.dataset.table(ref)

    def wrap_partition(self, p):
        from .partitions import PartitionProxy

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

        self.session.delete(p._partition)

    def source(self, name):
        return self.dataset.source_file(name)

    def source_pipe(self, source):
        """Create a source pipe for a source, giving it access to download files to the local cache"""
        from ambry.etl.rowgen import source_pipe

        if isinstance(source, string_types):
            source = self.source(source)

        sp = source_pipe(self, source)
        sp.bundle = self
        return sp

    @property
    def sources(self):
        """Iterate over downloadable sources"""
        return list(s for s in self.dataset.sources if s.is_downloadable)

    @property
    def refs(self):
        """Iterate over downloadable sources -- referecnes and templates"""
        return list(s for s in self.dataset.sources if not s.is_downloadable)

    @property
    def metadata(self):
        """Return the Metadata acessor"""
        return self.dataset.config.metadata

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
        from fs.opener import fsopendir

        if not self._build_fs:
            build_url = self._build_url if self._build_url else self.dataset.config.library.build.url

            if not build_url:
                build_url = self.library.filesystem.build(self.identity.cache_key)
                # raise ConfigurationError(
                #    'Must set build URL either in the constructor or the configuration')

            self._build_fs = fsopendir(build_url)

        return self._build_fs

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

        self.logger.fatal(message)
        sys.stderr.flush()
        if self.exit_on_fatal:
            sys.exit(1)
        else:
            raise FatalError(message)

    #
    # Source Synced
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

        if self.is_finalized:
            self.error("Can't sync; bundle is finalized")
            return False

        ds = self.dataset  # FIXME: Unused variable. Remove after testing.

        syncs = self.build_source_files.sync(force, defaults)

        self.state = self.STATES.SYNCED
        self.log("---- Synchronized ----")
        self.dataset.commit()

        return syncs

    def sync_in(self):
        """Synchronize from files to records, and records to objects"""
        from ambry.bundle.files import BuildSourceFile
        self.build_source_files.sync(BuildSourceFile.SYNC_DIR.FILE_TO_RECORD)
        self.build_source_files.record_to_objects()
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
        self.build_source_files.objects_to_record()

    def sync_objects(self):
        self.build_source_files.record_to_objects()
        self.build_source_files.objects_to_record()

    #
    # Do All; Run the full process

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

    #
    # Clean
    #

    @property
    def is_clean(self):
        return self.state == self.STATES.CLEANED

    def clean(self, force=False):

        """Clean generated objects from the dataset, but only if there are File contents
         to regenerate them"""
        from ambry.orm import ColumnStat, File

        if self.is_finalized and not force:
            self.warn("Can't clean; bundle is finalized")
            return False

        self.log('---- Cleaning ----')
        self.state = self.STATES.CLEANING

        ds = self.dataset
        s = self.session

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

        self.log('---- Done Cleaning ----')

        return True

    def clean_all(self, force=False):
        """Like clean, but also clears out files. """

        self.clean()
        self.dataset.files[:] = []

    #
    # Prepare
    #

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
            self.log('---- Preparing ----')
            if self.prepare_main():
                self.state = self.STATES.PREPARED
                if self.post_prepare():
                    self.log('---- Done Preparing ----')
                else:
                    self.set_error_state()
                    self.log('---- Post-prepare exited with failure ----')
                    r = False
            else:
                self.set_error_state()
                self.log('---- Prepare exited with failure ----')
                r = False
        else:
            self.log('---- Skipping prepare ---- ')
            r = False

        self.dataset.commit()

        return r

    def prepare_main(self):

        return True

    def pre_prepare(self):
        """"""
        if self.is_finalized:
            self.warn("Can't prepare; bundle is finalized")
            return False
        return True

    def post_prepare(self):
        """"""

        for t in self.dataset.tables:
            if not bool(t.description):
                self.error('No title ( Description of id column ) set for table: {} '.format(t.name))

        self.commit()

        return True

    #
    # General Phase Runs
    #

    def pipeline(self, phase, source=None):
        """Construct the ETL pipeline for all phases. Segments that are not used for the current phase
        are filtered out later. """

        from ambry.etl.pipeline import Pipeline, PartitionWriter

        if source:
            source = self.source(source) if isinstance(source, string_types) else source
        else:
            source = None

        pl = Pipeline(self, source=self.source_pipe(source) if source else None)

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

        self.state = phase

        self.load_requirements()

        self.import_lib()

        return True

    def phase_main(self, phase,  stage='main', sources=None):
        """
        Synchronize with the files and run the meta pipeline, possibly creating new objects. Then, write the
        objects back to file records and synchronize.

        :param force:
        :return:
        """

        assert isinstance(stage, string_types) or stage is None

        if self.is_finalized:
            self.error("Can't run phase {}; bundle is finalized".format(phase))
            return False

        if self.is_built:
            self.error("Can't run phase {}; bundle is built".format(phase))
            return False

        self.import_lib()
        self.load_requirements()

        def stage_match(source_stage, this_stage):
            if not bool(source_stage) and (this_stage == 'main' or this_stage is None):
                return True

            if bool(source_stage) and source_stage == this_stage:
                return True

            return False

        # Enumerate all of the sources first.
        if not sources:
            # Select sources that match this stage
            sources = []
            for i, source in enumerate(self.sources):

                if stage_match(source.stage, stage):
                    # print 'MATCH ', source.stage, stage
                    sources.append(source)
        else:
            # Use the named soruces, but ensure they are all source objects.
            source_objs = []

            if not isinstance(sources, (list, tuple)):
                sources = [sources]

            for source in sources:
                if isinstance(source, string_types):
                    source_obj = self.source(source)
                    if not source_obj:
                        raise PhaseError("Could not find source named '{}' ".format(source))
                    source_objs.append(source_obj)
                else:
                    source_objs.append(source)

            sources = source_objs

        log_msg = 'Processing {} sources, stage {} ; {}'\
            .format(len(sources), stage, [x.name for x in sources[:10]])
        self.log(log_msg)

        for i, source in enumerate(sources):

            self.logger.info('Running phase {} for source {} '.format(phase, source.name))

            pl = self.pipeline(phase, source)

            pl.run()

            self.log('Final methods')
            for m in pl.final:
                self.log(indent + b(m))
                getattr(self, m)(pl)

            if pl.stopped:
                break

        self.dataset.commit()

        return True

    def post_phase(self, phase):
        """After the build, update the configuration with the time required for
        the build, then save the schema back to the tables, if it was revised
        during the build."""

        self.state = phase + '_done'

        return True

    def run_phase(self, phase, stage='main', sources=None):

        assert isinstance(stage, string_types) or stage is None

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

        try:

            step_name = 'Pre-{}'.format(phase)
            if not phase_pre(phase):
                self.log("---- Skipping {} ---- ".format(phase))
                return False

            self.log("---- Phase: {} ---".format(phase))
            step_name = phase.title()
            self.phase_main(phase, stage=stage, sources=sources)

            step_name = 'Post-{}'.format(phase)
            phase_post(phase)

        except Exception as e:
            self.rollback()
            self.error('{} phase, stage {} failed: {}'.format(step_name, stage, e))
            self.set_error_state()
            raise

        return True

    def final_log_pipeline(self, pl):
        """Write a report of the pipeline out to a file """

        self.build_fs.makedir('pipeline', allow_recreate=True)
        v = """
Pipeline
========
{}

Pipeline Headers
================
{}
""".format(u(pl), pl.headers_report())

        self.build_fs.setcontents(
            os.path.join('pipeline', pl.phase + '-' + pl.file_name + '.txt'), v, encoding='utf8')

    #
    # Meta
    #

    def meta(self, sources=None):
        self.run_phase('source', sources=sources)
        self.run_phase('schema', sources=sources)
        return True

    def meta_schema(self, sources=None):
        return self.run_phase('schema', sources=sources)

    def meta_source(self, sources=None):
        return self.run_phase('source', sources=sources)

    def final_make_source_tables(self, pl):
        from ambry.etl.intuit import TypeIntuiter

        ti = pl[TypeIntuiter]

        source = pl.source.source
        st = source.source_table

        # if not source.st_id:
        for tic in ti.columns:
            c = st.column(tic.header)
            if c:
                c.datatype = TypeIntuiter.promote_type(c.datatype, tic.resolved_type)
                self.log('Update column: {}.{}'.format(st.name, c.source_header))
            else:
                c = st.add_column(tic.position, source_header=tic.header, dest_header=tic.header,
                                  datatype=tic.resolved_type_name)
                self.log('Created soruce table column: {}.{}'.format(st.name, c.source_header))

    def final_make_dest_tables(self, pl):
        from ambry.etl.intuit import TypeIntuiter
        from ambry.orm.source_table import SourceColumn
        source = pl.source.source
        dest = pl.source.source.dest_table
        st = source.source_table

        col_type_map = SourceColumn.column_type_map

        ti = pl[TypeIntuiter]

        for tic in ti.columns:
            sc = st.column(tic.header)

            dest.add_column(name=tic.header,
                            datatype=col_type_map[tic.resolved_type_name],
                            summary=sc.summary if sc else None,
                            description=sc.description if sc else None)

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

    def pre_build(self, phase='build', force=False):
        assert isinstance(force, bool)
        return self.pre_phase(phase, force=force)

    def build(self,  stage='main', sources=None):
        assert isinstance(stage, string_types) or stage is None
        return self.run_phase('build', sources=sources, stage=stage)

    def post_build(self, phase='build'):
        """After the build, update the configuration with the time required for
        the build, then save the schema back to the tables, if it was revised
        during the build."""

        self.build_post_cast_error_codes()

        self.build_post_unify_partitions()

        self.build_post_write_bundle_file()

        self.state = phase + '_done'

        return True

    def build_post_cast_error_codes(self):
        """If there are casting errors, final_cast_errors will generate codes for the values. This rounte
         will report all of them and report an error. """

        if len(self.dataset.codes):
            cast_errors = 0
            self.error('Casting Errors')
            for c in self.dataset.codes:
                if c.source == 'cast_error':
                    self.error(
                        indent + "Casting Errors {}.{} {}".format(c.column.table.name, c.column.name, c.key))
                    cast_errors += 1

            if cast_errors > 0:
                raise PhaseError('Too many casting errors')

    def build_post_unify_partitions(self):
        """For all of the segments for a partition, create the parent partition, combine the children into the parent,
        and delete the children. """

        from collections import defaultdict
        from ..orm.partition import Partition
        from ..etl.stats import Stats

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

        for name, segments in iteritems(partitions):

            self.log('Coalescing segments for partition {} '.format(name))

            parent = self.partitions.get_or_new_partition(name, type=Partition.TYPE.UNION)
            stats = Stats(parent.table)

            pdf = parent.datafile
            for seg in sorted(segments, key=lambda x: b(x.name)):

                self.log(indent + 'Coalescing segment  {} '.format(seg.identity.name))

                reader = self.wrap_partition(seg).datafile.reader()
                header = None
                start_time = time()
                for i, row in enumerate(reader):

                    if header is None:
                        header = row
                        pdf.insert_header(row)
                        stats.process_header(row)
                    else:
                        row[0] = i
                        pdf.insert_body(row)
                        stats.process_body(row)

                reader.close()
                self.log(indent + "Coalesced {} rows, {} rows/sec ".format(i, float(i)/(time()-start_time)))

            pdf.close()
            self.commit()
            parent.finalize(stats)
            self.commit()

            for s in segments:
                self.wrap_partition(s).datafile.delete()
                self.session.delete(s)

    def final_finalize_segments(self, pl):

        try:
            for p in pl[ambry.etl.PartitionWriter].partitions:
                self.logger.info(indent + indent + 'Finalizing {}'.format(p.identity.name))
                # We're passing the datafile path into filanize b/c finalize is on the ORM object,
                # and datafile is on the proxy.
                p.finalize()

                # FIXME SHouldn't need to do this commit, but without it, some stats get added multiple
                # times, causing an error later. Probably could be avoided by adding the states to the
                # collection in the dataset
                self.commit()

        except IndexError:
            self.error("Pipeline didn't have a PartitionWriters, won't try to finalize")

        self.commit()

    def final_cast_errors(self, pl):
        cp = pl[ambry.etl.CasterPipe]

        n = 0
        seen = set()
        for errors in cp.errors:
            for col, error in list(errors.items()):

                n += 1

                key = (col, error['value'])
                if key not in seen:
                    seen.add(key)
                    self.error('Cast Error on column {}; {}'.format(cp.headers[col], error))

                    column = cp.source.dest_table.column(cp.headers[col])
                    column.add_code(error['value'], error['value'], source='cast_error')

    def build_post_write_bundle_file(self):

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
        return True

    #
    # Check in to remote
    #

    def checkin(self):

        if self.is_built:
            self.finalize()

        if not (self.is_finalized or self.is_prepared):
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

    def remove(self):
        """Delete resources associated with the bundle."""
        pass  # Remove files in the file system other resource.

    def field_row(self, fields):
        """Return a list of values to match the fielsds values"""

        row = self.dataset.row(fields)

        # Modify for special fields
        for i, f in enumerate(fields):
            if f == 'state':
                row[i] = self.state
            elif f == 'source_fs':
                row[i] = self.source_fs

        return row

    def clear_states(self):
        return self.dataset.config.build.clean()

    @property
    def state(self):
        return self.dataset.config.build.state.current

    @property
    def error_state(self):
        """Set the error condition"""
        self.dataset.config.build.state.lastime = time()
        return self.dataset.config.build.state.error

    @state.setter
    def state(self, state):
        """Set the current build state and record the tim eto maintain history"""

        self.dataset.config.build.state.current = state
        self.dataset.config.build.state.error = False
        self.dataset.config.build.state[state] = time()
        self.dataset.config.build.state.lastime = time()

    def set_error_state(self):
        self.dataset.config.build.state.error = time()

    def set_last_access(self, tag):
        """Mark the time that this bundle was last accessed"""
        self.dataset.config.build.access.last = tag
        self.dataset.commit()
