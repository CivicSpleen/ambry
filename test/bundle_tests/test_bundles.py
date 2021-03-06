"""
Created on Jun 22, 2012

@author: eric
"""
import os

from test.proto import TestBase


class Test(TestBase):

    def _my_import(self, cache_path):
        """ Imports bundle found by given path. """
        b = self.import_single_bundle(cache_path)
        b.clean_except_files()  # Clean objects, but leave the import files
        b.sync_objects_in()  # Sync from file records to objects.
        b.commit()
        return b

    def test_ingest_basic(self):
        bundle = self._my_import('ingest.example.com/basic')
        bundle.ingest()
        bundle.close()

    def test_ingest_stages(self):
        bundle = self._my_import('ingest.example.com/stages')
        bundle.ingest()
        bundle.close()

    def test_ingest_headerstypes(self):
        bundle = self._my_import('ingest.example.com/headerstypes')
        bundle.ingest()
        bundle.close()

    def test_ingest_variety(self):
        bundle = self._my_import('ingest.example.com/variety')
        bundle.ingest()
        bundle.close()

    def test_build_generators(self):
        bundle = self._my_import('build.example.com/generators')
        bundle.run()
        bundle.close()

    def test_build_casters(self):
        bundle = self._my_import('build.example.com/casters')

        bundle.run()
        bundle.close()

    def test_build_coverage(self):
        bundle = self._my_import('build.example.com/coverage')
        bundle.run()
        bundle.close()

    def test_plots(self):
        bundle = self._my_import('build.example.com/plot')

        bundle.build()
        bundle.close()

    def test_pipes_geoid(self):
        bundle = self._my_import('pipes.example.com/geoidpipes')

        bundle.build()
        bundle.close()

    def test_notebooks(self):
        bundle = self._my_import('misc.example.com/notebooks')

        bundle.build()
        bundle.close()
