"""
Created on Jun 22, 2012

@author: eric
"""

# Monkey patch logging, because I really don't understand logging

import unittest
from ambry.identity import *


from ambry.orm import Database, Dataset


class TestBase(unittest.TestCase):
    def setUp(self):
        from sqlalchemy import create_engine

        super(TestBase, self).setUp()

        self.dsn = 'sqlite://'  # Memory database

        # Make an array of dataset numbers, so we can refer to them with a single integer
        self.dn = [str(DatasetNumber(x, x)) for x in range(1, 10)]

        self.db = None

    def ds_params(self, n ):
        return dict(vid=self.dn[n], source='source', dataset='dataset')

    def get_rc(self, name='ambry.yaml'):
        from ambry.run import get_runconfig
        import os
        from test import bundlefiles

        def bf_dir(fn):
            return os.path.join(os.path.dirname(bundlefiles.__file__), fn)

        return get_runconfig(bf_dir('ambry.yaml'))

    def new_dataset(self, n=1):
        return Dataset(**self.ds_params(n))

    def new_db_dataset(self, db=None, n=1):
        if db is None:
            db = self.db

        return db.new_dataset(**self.ds_params(n))

    def copy_bundle_files(self, source, dest):
        from ambry.bundle.files import file_info_map
        from fs.errors import ResourceNotFoundError

        for const_name, (path, clz) in file_info_map.items():
            try:
                dest.setcontents(path, source.getcontents(path))
            except ResourceNotFoundError:
                pass

    def dump_database(self, table, db=None):

        if db is None:
            db = self.db

        for row in db.connection.execute("SELECT * FROM {}".format(table)):
            print row

    def new_database(self):
        db = Database(self.dsn)
        db.open()

        return db

    def tearDown(self):
        pass
