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
        cls.import_bundles(clean=True) # If clean==true, class setup completely reloads the library
        cls.l = cls.library()

    def setUp(self):
        super(TestBase, self).setUp()

    def tearDown(self):
        pass

    def run_bundle(self, name, reimport = False):
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
            self.l.import_bundles(orig_source, detach=True, force = True)

        b.clean_except_files() # Clean objects, but leave the import files
        b.sync_objects_in() # Sync from file records to objects.
        b.commit()

        b.run()

    def test_ingest_basic(self):
        self.run_bundle('ingest.example.com-basic')

    def test_ingest_stages(self):
        self.run_bundle('ingest.example.com-stages', reimport = True)

    def test_ingest_headerstypes(self):
        self.run_bundle('ingest.example.com-headerstypes')

    def test_ingest_variety(self):
        self.run_bundle('ingest.example.com-variety')

    def test_build_generators(self):
        self.run_bundle('build.example.com-generators')

    def test_build_casters(self):
        self.run_bundle('build.example.com-casters')

    def test_build_coverage(self):
        self.run_bundle('build.example.com-coverage')

    def test_pipes_geoid(self):
        self.run_bundle('pipes.example.com-geoidpipes')