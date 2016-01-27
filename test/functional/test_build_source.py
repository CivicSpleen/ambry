# -*- coding: utf-8 -*-

from test.test_base import TestBase
from ambry.orm.file import File


class Test(TestBase):

    def test_bs_iter(self):
        import unicodecsv as csv

        b = self.import_single_bundle('misc.example.com/notebooks')

        for f in b.build_source_files.list_records(File.BSFILE.BUILD):
            print f.record.dict