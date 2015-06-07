
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

        self.uuid = str(uuid.uuid4())
        self.tmpdir = tempfile.mkdtemp(self.uuid)

        self.delete_tmpdir = True

        #self.dsn = "sqlite:///{}/test.db".format(self.tmpdir)

        self.dsn = 'sqlite://'

        # Make an array of dataset numbers, so we can refer to them with a single integer
        self.dn = [str(DatasetNumber(x, x)) for x in range(1, 10)]

    def new_dataset(self, n):
        return Dataset(vid=self.dn[n], source='source', dataset='dataset' )

    def dump_database(self, db, table):
        import sys
        from subprocess import check_output

        for row in db.connection.execute("SELECT * FROM {}".format(table)):
            print row

    def test_dataset_basic(self):
        """Basic operations on datasets"""

        from ambry.orm.database import Database
        from ambry.orm.exc import ConflictError
        from ambry.identity import DatasetNumber

        db = Database(self.dsn)
        db.open()

        ##
        ## Creating and conflicts
        ##
        db.new_dataset(vid=self.dn[0], source='source', dataset='dataset')
        db.new_dataset(vid=self.dn[1], source='source', dataset='dataset')

        with self.assertRaises(ConflictError):
            db.new_dataset(vid=self.dn[0], source='source', dataset='dataset')

        dn = DatasetNumber(100)

        ##
        ## datasets() gets datasets, and latest give id instead of vid
        ##
        db.new_dataset(vid=str(dn.rev(5)), source='a', dataset='dataset')
        db.new_dataset(vid=str(dn.rev(1)), source='a', dataset='dataset')
        db.new_dataset(vid=str(dn.rev(3)), source='a', dataset='dataset')
        db.new_dataset(vid=str(dn.rev(4)), source='a', dataset='dataset')

        ds = db.dataset(str(dn.rev(5)))
        self.assertEquals(str(dn.rev(5)), ds.vid)

        ds = db.dataset(str(dn.rev(3)))
        self.assertEquals(str(dn.rev(3)), ds.vid)

        ds = db.dataset(str(dn.rev(None)))
        self.assertEquals(str(dn.rev(5)), ds.vid)

        db.new_dataset(vid=str(dn.rev(6)), source='a', dataset='dataset')

        ds = db.dataset(str(dn.rev(None)))
        self.assertEquals(str(dn.rev(6)), ds.vid)

        db.close()

    def test_config(self):

        from ambry.orm.database import Database

        db = Database(self.dsn)
        db.open()

        db.root_dataset.config.library.config.path = 'foobar'

        self.assertEqual('foobar',  db.root_dataset.config.library.config.path)

    def test_tables(self):
        from ambry.orm.database import Database

        db = Database(self.dsn)
        db.open()

        ds = db.new_dataset(vid=self.dn[0], source='source', dataset='dataset')

        ds.new_table('table1')
        ds.new_table('table2', description='table2', data=dict(a=1,b=2,c=3))
        ds.new_table('table3', description='table3')

        db.commit()

        t2 = ds.table('table2')
        t2.description = 'tablex'

        db.session.add(t2)
        db.commit()

        ds = db.dataset(ds.vid) # Refresh the memory object
        ds.new_table('table2', data=dict(b=22))
        ds.new_table('table3', description='table3-description')

        db.commit()

        self.assertEqual(22, db.dataset(ds.vid).table('table2').data['b'])
        self.assertEqual('table3-description', db.dataset(ds.vid).table('table3').description)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
