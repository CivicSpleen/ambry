"""
Created on Jun 22, 2012

@author: eric
"""
import os

from test.test_base import TestBase

class Test(TestBase):

    l = None

    @classmethod
    def setUpClass(cls):

        TestBase.setUpClass()

        cls.db_type = os.environ.get('AMBRY_TEST_DB', 'sqlite')
        cls.config = TestBase.get_rc()
        cls.l = TestBase._get_library(cls.config)
        print "Database: ", cls.l.database.dsn

        cls._import_bundles(cls.l, clean=True, force_import=False)



    def setUp(self):
        super(TestBase, self).setUp()

    def tearDown(self):
        self.l.close()

    def run_bundle(self, name, reimport=False):
        """
        Ingest and build a bundle

        :param name: The name or vid of the bundle
        :param reimport: If True, reimport the bundle source
        :return:
        """
        from test import bundle_tests
        b = self.l.bundle(name).cast_to_subclass()
        b.capture_exceptions = False

        if reimport:
            orig_source = os.path.join(os.path.dirname(bundle_tests.__file__), b.identity.source_path)
            self.l.import_bundles(orig_source, detach=True, force=True)

        b.clean_except_files() # Clean objects, but leave the import files
        b.sync_objects_in() # Sync from file records to objects.
        b.commit()

        b.run()

    def ingest_bundle(self, name, reimport=False):
        """
        Ingest and build a bundle

        :param name: The name or vid of the bundle
        :param reimport: If True, reimport the bundle source
        :return:
        """

        from test import bundle_tests
        b = self.l.bundle(name).cast_to_subclass()
        b.capture_exceptions = False

        if reimport:
            orig_source = os.path.join(os.path.dirname(bundle_tests.__file__), b.identity.source_path)
            self.l.import_bundles(orig_source, detach=True, force=True)

        b.clean_except_files()  # Clean objects, but leave the import files
        b.sync_objects_in()  # Sync from file records to objects.
        b.commit()

        b.ingest()

    def test_ingest_basic(self):
        self.ingest_bundle('ingest.example.com-basic')

    def test_ingest_stages(self):
        self.ingest_bundle('ingest.example.com-stages')

    def test_ingest_headerstypes(self):
        self.ingest_bundle('ingest.example.com-headerstypes')

    def test_ingest_variety(self):
        self.ingest_bundle('ingest.example.com-variety')

    def test_build_generators(self):
        self.run_bundle('build.example.com-generators')

    def test_build_casters(self):
        self.run_bundle('build.example.com-casters')

    def test_build_coverage(self):
        self.run_bundle('build.example.com-coverage')

    def test_pipes_geoid(self):
        self.run_bundle('pipes.example.com-geoidpipes')