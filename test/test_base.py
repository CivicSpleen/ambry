"""
Created on Jun 22, 2012

@author: eric
"""

# Monkey patch logging, because I really don't understand logging

import unittest
from ambry.identity import *
import time, logging
import ambry.util
from ambry.util import install_test_logger

from ambry.orm import Database, Dataset

# ambry.util.get_logger = install_test_logger('/tmp/ambry-test.log')




class TestBase(unittest.TestCase):
    def setUp(self):
        from sqlalchemy import create_engine

        super(TestBase, self).setUp()

        self.dsn = 'sqlite://'  # Memory database

        # Make an array of dataset numbers, so we can refer to them with a single integer
        self.dn = [str(DatasetNumber(x, x)) for x in range(1, 10)]

        self._db = None

    def ds_params(self, n ):
        return dict(vid=self.dn[n], source='source', dataset='dataset')

    def new_dataset(self, n=1):
        return Dataset(**self.ds_params(n))

    def new_db_dataset(self, db=None, n=1):
        if db is None:
            db = self._db

        return db.new_dataset(**self.ds_params(n))

    def dump_database(self, table, db=None):

        if db is None:
            db = self._db

        for row in db.connection.execute("SELECT * FROM {}".format(table)):
            print row

    def new_database(self):
        db = Database(self.dsn)
        db.open()
        self._db = db
        return db

    def tearDown(self):
        pass
