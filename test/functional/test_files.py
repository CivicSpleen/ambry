# -*- coding: utf-8 -*-
from ambry.library import new_library
from ambry.orm.file import File

from test.test_base import TestBase

from test.test_orm.factories import ConfigFactory, DatasetFactory


class Test(TestBase):

    def test_basic(self):
        """Basic operations on datasets"""

        rc = self.get_rc()
        self.library = new_library(rc)
        ConfigFactory._meta.sqlalchemy_session = self.library.database.session
        DatasetFactory._meta.sqlalchemy_session = self.library.database.session

        ds = DatasetFactory()

        bs1 = ds.bsfile(File.BSFILE.BUILD)
        bs2 = ds.bsfile(File.BSFILE.SCHEMA)
        bs3 = ds.bsfile(File.BSFILE.BUILD)

        self.assertEqual(bs1.id, bs3.id)
        self.assertNotEqual(bs1.id, bs2.id)
