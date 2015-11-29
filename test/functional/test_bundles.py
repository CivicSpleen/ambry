# -*- coding: utf-8 -*-

from test.test_base import TestBase


class Test(TestBase):

    def test_headerstypes(self):
        import unicodecsv as csv

        b = self.import_single_bundle('ingest.example.com/headerstypes')
        b.ingest()

    def test_basic(self):
        b = self.import_single_bundle('ingest.example.com/basic')
        b.ingest()

    def test_stages(self):
        b = self.import_single_bundle('ingest.example.com/stages')
        b.run()

    def test_casters(self):
        b = self.import_single_bundle('build.example.com/casters')
        b.run()
