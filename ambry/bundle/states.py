""" Build states and state machine.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""


from ..dbexceptions import ConfigurationError, ProcessError
from ..util import Constant
class StateMachine(object):

    STATES = Constant()
    STATES.SYNCED = 'synced'
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


    def __init__(self, bundle):

        self._bundle = bundle


    ##
    ## Main Loop
    ##

    ##
    ## Source Synced
    ##

    def sync(self):

        ds = self._bundle.dataset
        self._bundle.source_files.sync()

        self.state = self.STATES.SYNCED

        self._bundle.dataset.commit()

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

        self._bundle.dataset.commit()
        return r

    def clean(self):
        """Clean generated objects from the dataset, but only if there are File contents
         to regenerate them"""
        from ambry.orm import File

        ds = self._bundle.dataset

        self._bundle.dataset.partitions[:] = []

        if ds.bsfile(File.BSFILE.SOURCES).has_contents:
            self._bundle.dataset.sources[:] = []

        if ds.bsfile(File.BSFILE.SCHEMA).has_contents:
            self._bundle.dataset.tables[:] = []

        if ds.bsfile(File.BSFILE.COLMAP).has_contents:
            self._bundle.dataset.colmaps[:] = []

        ds.config.metadata.clean()
        ds.config.build.clean()
        ds.config.process.clean()

        ds.commit()

        return True

    def post_clean(self):
        pass
        return True

    ##
    ## Prepare
    ##

    @property
    def is_prepared(self):
        return bool(self._bundle.dataset.config.build.state.prepared)

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

        self._bundle.dataset.commit()
        return r

    def prepare_main(self):
        """This is the methods that is actually called in do_prepare; it
        dispatches to developer created prepare() methods."""
        return self.prepare()

    def prepare(self):
        from ambry.orm.exc import NotFoundError
        from ambry.orm import File

        try:
            self._bundle.sources.check_dependencies()
        except NotFoundError as e:
            self.error(e.message)
            return False

        try:
            self._bundle.schema.read()
        except Exception as e:
            self.error(e.message)
            raise
            return False

        self._bundle.source_files.file(File.BSFILE.META).record_to_objects()

        return True

    def pre_prepare(self):
        """"""
        return True

    def post_prepare(self):
        """"""

        for t in self._bundle.schema.tables:
            if not bool(t.description.strip()):
                self.error("No title ( Description of id column ) set for table: {} ".format(t.name))

        return  True

    def prepare_update_configuration(self, identity=None, rewrite_database=True):
        # Re-writes the bundle.yaml file, with updates to the identity and partitions
        # sections.

        if not identity:
            identity = self.identity

        md.identity = identity.ident_dict
        md.names = identity.names_dict

        # Ensure there is an entry for every revision, if only to nag the maintainer to fill it in.
        # for i in range(1, md.identity.revision+1):
        # md.versions[i]
        #    if i == md.identity.revision:
        #        md.versions[i].version = md.identity.version

        # Load the documentation

        def read_file(fn):
            try:
                with open(self.filesystem.path(fn)) as f:
                    return f.read()
            except IOError:
                return ''

        self.update_source()

        self.write_doc_html()

        md.documentation.main = self.sub_template(read_file(self.DOC_FILE))
        md.documentation.title = md.about.title.text
        md.documentation.summary = md.about.summary.text
        md.documentation.source = md.about.source.text
        md.documentation.processed = md.about.processed.text
        md.documentation.summary = md.about.summary.text

        md.write_to_dir(write_all=True)

        # Reload some of the values from bundle.yaml into the database
        # configuration

        if rewrite_database:
            if self.database.exists():

                if self.config.build.get('dependencies'):
                    for k, v in self.config.build.get('dependencies').items():
                        self.set_value('odep', k, v)

                self.database.rewrite_dataset()

    ##
    ## Build
    ##

    @property
    def is_built(self):
        """Return True is the bundle has been built."""
        return bool(self._bundle.dataset.config.build.state.built)

    def do_build(self):

        if not self.is_prepared:
            if not self.do_prepare():
                self.log("Prepare failed; skipping build")
                return False

        self.state = self.STATES.BUILDING
        if self.pre_build():
            self.log("---- Build ---")
            if self.build_main():
                self.state = self.STATES.BUILT
                if self.post_build():
                    self.log("---- Done Building ---")
                    self.log("Bundle DB at: {}".format(self.database.dsn))
                    r = True
                else:
                    self.log("---- Post-build failed ---")
                    self.log("Bundle DB at: {}".format(self.database.dsn))
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
    def pre_build(self):
        from time import time
        import sys


        if self.is_built:
            self.error("Bundle is already built. Skipping  ( Use --clean  or --force to force build ) ")
            return False

        if not self.is_prepared:
            self.error("Build called before prepare completed")
            return False


        #python_dir = self.config.python_dir()
        #if python_dir and python_dir not in sys.path:
        #    sys.path.append(python_dir)

        return True

    def build(self):
        return False

    def build_main(self):
        """This is the methods that is actually called in do_build; it
        dispatches to developer created prepare() methods."""
        return self.build()

    def post_build(self):
        """After the build, update the configuration with the time required for
        the build, then save the schema back to the tables, if it was revised
        during the build."""
        from time import time

        with self.session:
            if self._build_time:
                self.set_value('process', 'buildtime', time() - self._build_time)

            self.post_build_finalize()

            if self.config.environment.category == 'development':
                pass

            self.update_configuration()
            self._revise_schema()
            self.schema.move_revised_schema()
            self.post_build_write_partitions()
            self.write_config_to_bundle()
            self.post_build_geo_coverage()
            self.post_build_time_coverage()
            # self.schema.write_codes()

            self.post_build_test()

            self.set_build_state('built')

        self.close()

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

            score, gvid, type, name = places[0]

            return gvid

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

    def post_build_finalize(self):
        from sqlalchemy.exc import OperationalError

        # Create stat entries for all of the partitions.
        for p in self.partitions:
            try:
                from ..partition.sqlite import SqlitePartition
                from ..partition.geo import GeoPartition

                if p.ref or p.is_finalized:
                    continue

                self.log("Finalizing partition: {}".format(p.identity.name))

                p.finalize()

            except NotImplementedError:
                self.log(
                    "Can't finalize (unimplemented) for partition: {}".format(
                        p.identity.name))
            except ConfigurationError as e:
                self.error(e.message)
            except OperationalError as e:
                self.error(
                    "Failed to write stats for partition {}: {}".format(
                        p.identity.name,
                        e.message))
                raise

            # Or, will run out of files/connections and get operational error
            p.close()

    ##
    ## Install
    ##

    @property
    def is_installed(self):
        """Return True if the bundle is installed."""

        r = self.library.resolve(self.identity.vid)

        return r is not None

    #######
    #######




    def clear_states(self):
        return self._bundle.dataset.config.build.clean()

    @property
    def state(self):
        return self._bundle.dataset.config.build.state.current

    @property
    def error_state(self):
        """Set the error condition"""
        from time import time
        self._bundle.dataset.config.build.state.lastime = time()
        return self._bundle.dataset.config.build.state.error

    @state.setter
    def state(self, state):
        """Set the current build state and record the tim eto maintain history"""
        from time import time

        self._bundle.dataset.config.build.state.current = state
        self._bundle.dataset.config.build.state.error = False
        self._bundle.dataset.config.build.state[state] = time()
        self._bundle.dataset.config.build.state.lastime = time()

    def set_error_state(self):
        from time import time

        self._bundle.dataset.config.build.state.error = time()

    @property
    def x_build_state(self):
        from ambry.orm.exc import DatabaseMissingError
        from ambry.orm.exc import NotFoundError

        try:
            c = self.get_value('process', 'state')
            return c.value
        except (DatabaseMissingError, NotFoundError):
            return 'new'

    def x_set_build_state(self, state):
        from datetime import datetime

        if state not in ('cleaned', 'meta'):  # If it is cleaned, the DB is deleted, so this isn't necessary
            self.set_value('process', state, datetime.now().isoformat())
            self.set_value('process', 'state', state)
            self.set_value('process', 'last', datetime.now().isoformat())

        if self.library.source:
            self.library.source.set_bundle_state(self.identity, state)


    #
    # Logging
    #

    def log(self, message, **kwargs):
        """Log the messsage."""
        self._bundle.log(message, **kwargs)

    def error(self, message):
        """Log an error messsage.

        :param message:  Log message.

        """
        self._bundle.error(message)

    def warn(self, message):
        """Log an error messsage.

        :param message:  Log message.

        """
        self._bundle.warn(message)

    def fatal(self, message):
        """Log a fatal messsage and exit.

        :param message:  Log message.

        """
        import sys

        self._bundle.fatal(message)


