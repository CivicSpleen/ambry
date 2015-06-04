
import unittest
import tempfile
import uuid
from ambry.orm import Column
from ambry.orm import Partition
from ambry.orm import Table
from ambry.orm import Dataset
from ambry.orm import Config
from ambry.orm import File
from ambry.orm import Code
from ambry.orm import ColumnStat
from sqlalchemy.orm import sessionmaker
from ambry.identity import DatasetNumber, PartitionNumber
from sqlalchemy.exc import IntegrityError

class Test(unittest.TestCase):

    def setUp(self):
        from sqlalchemy import create_engine

        super(Test,self).setUp()

        self.dsn = 'sqlite://'

        # Make an array of dataset numbers, so we can refer to them with a single integer
        self.dn = [str(DatasetNumber(x, x)) for x in range(1, 10)]



    def new_dataset(self, n):
        return Dataset(vid=self.dn[n], source='source', dataset='dataset' )



    def new_partition(self,ds,n):

        t_vids = sorted(t.id_ for t in ds.tables)

        return Partition(ds, sequence_id=n, t_id=t_vids[n])

    def dump_database(self, db, table):
        import sys
        from subprocess import check_output

        for row in db.connection.execute("SELECT * FROM {}".format(table)):
            print row

    def test_table_basic(self):
        """Basic operations on datasets"""
        from ambry.orm.database import Database

        db = Database(self.dsn)
        db.open()

        ds = db.new_dataset(vid=self.dn[0], source='source', dataset='dataset')
        ds.add_table('table1')

        db.commit()

        t1 = db.dataset(ds.vid).table('table1')

        t1.add_column('col1', description='foobar')

        db.commit()

        self.dump_database(db,'columns')

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

