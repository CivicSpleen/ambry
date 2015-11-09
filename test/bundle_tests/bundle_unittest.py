"""
Created on Jun 22, 2012

@author: eric
"""
import os

import unittest

from ambry.run import get_runconfig

class TestBase(unittest.TestCase):
    def setUp(self):

        super(TestBase, self).setUp()

    def tearDown(self):
        pass

    @classmethod
    def get_rc(self, name='ambry.yaml'):
        import os
        from test import bundlefiles

        def bf_dir(fn):
            return os.path.join(os.path.dirname(bundlefiles.__file__), fn)

        return get_runconfig(bf_dir('ambry.yaml'))

    def copy_bundle_files(self, source, dest):
        from ambry.bundle.files import file_info_map
        from fs.errors import ResourceNotFoundError

        for const_name, (path, clz) in list(file_info_map.items()):
            try:
                dest.setcontents(path, source.getcontents(path))
            except ResourceNotFoundError:
                pass

    def setup_bundle(self, name):
        """Configure a bundle from existing sources"""
        from test import bundle_tests
        from os.path import dirname, join
        from ambry.library import new_library

        rc = self.get_rc()
        self.library = new_library(rc)

        self.db = self.library._db

        bundles = self.library.import_bundles(join(dirname(bundle_tests.__file__),  name), force = True, detach = True)

        self.assertEqual(1, len(bundles))

        return bundles.pop()

    def run_bundle(self, name):
        b = self.setup_bundle(name).cast_to_subclass()
        b.clean()
        b.sync_in()
        b.run()

    def test_ingest_basic(self):
        self.run_bundle('ingest.example.com/basic')

    def test_ingest_stages(self):
        self.run_bundle('ingest.example.com/stages')

    def test_ingest_headerstypes(self):
        self.run_bundle('ingest.example.com/headerstypes')