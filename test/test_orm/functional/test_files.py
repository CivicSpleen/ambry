# -*- coding: utf-8 -*-

from sqlalchemy.exc import IntegrityError

from ambry.orm.config import Config
from test.test_base import TestBase


class Test(TestBase):

    def test_basic(self):
        """Basic operations on datasets"""

        from ambry.orm.file import File

        db = self.new_database()
        ds = self.new_db_dataset(db, n=0)

        bs1 = ds.bsfile(File.BSFILE.BUILD)
        bs2 = ds.bsfile(File.BSFILE.SCHEMA)
        bs3 = ds.bsfile(File.BSFILE.BUILD)

        self.assertEqual(bs1.id, bs3.id)
        self.assertNotEqual(bs1.id, bs2.id)

        print bs1.id
        print bs2.id
        print bs3.id