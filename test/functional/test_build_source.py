# -*- coding: utf-8 -*-

from test.test_base import TestBase


class Test(TestBase):

    def test_bs_iter(self):
        import unicodecsv as csv

        b = self.import_single_bundle('ingest.example.com/headerstypes')

        return

        for f in b.build_source_files:
            print f.record.dict