# -*- coding: utf-8 -*-

from test.test_base import TestBase


class Test(TestBase):

    def test_headerstypes(self):

        b = self.import_single_bundle('ingest.example.com/headerstypes')
        b.ingest()

    def test_basic(self):
        b = self.import_single_bundle('ingest.example.com/basic')
        b.ingest()

    def test_stages(self):
        b = self.import_single_bundle('ingest.example.com/stages')

        b.run_stages()

    def test_casters(self):
        b = self.import_single_bundle('build.example.com/casters')
        b.ingest()
        b.source_schema()
        b.schema()
        b.build()

    def test_coverage(self):
        b = self.import_single_bundle('build.example.com/coverage')
        b.ingest()
        b.source_schema()
        b.schema()
        b.build()

    def test_generators(self):
        b = self.import_single_bundle('build.example.com/generators')
        b.run()

