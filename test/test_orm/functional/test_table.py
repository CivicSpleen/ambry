
import unittest
from ambry.orm.database import Database
from ambry.orm.dataset import Dataset
from ambry.orm.partition import Partition
from ambry.identity import DatasetNumber


class Test(unittest.TestCase):

    def setUp(self):

        super(Test, self).setUp()

        self.dsn = 'sqlite://'

        # Make an array of dataset numbers, so we can refer to them with a single integer
        self.dn = [str(DatasetNumber(x, x)) for x in range(1, 10)]

    def new_dataset(self, n):
        return Dataset(vid=self.dn[n], source='source', dataset='dataset')

    def new_partition(self, ds, n):

        t_vids = sorted(t.id_ for t in ds.tables)

        return Partition(ds, sequence_id=n, t_id=t_vids[n])

    def dump_database(self, db, table):
        for row in db.connection.execute("SELECT * FROM {}".format(table)):
            print(row)

    def test_table_basic(self):
        """Basic operations on datasets"""

        db = Database(self.dsn)
        db.open()

        ds = db.new_dataset(vid=self.dn[0], source='source', dataset='dataset')
        ds.new_table('table1')

        db.commit()

        t1 = db.dataset(ds.vid).table('table1')

        t1.add_column('col1', description='foobar')

        db.commit()
        # uncomment to see database content.
        # self.dump_database(db, 'columns')

    def test_a_lot_of_tables(self):
        from contexttimer import Timer

        db = Database(self.dsn)
        db.open()

        ds = db.new_dataset(vid=self.dn[0], source='source', dataset='dataset')

        with Timer() as tr:
            for i in range(100):
                t = ds.new_table('table'+str(i))

                for j in range(10):
                    c = t.add_column("col"+str(j), datatype = 'integer')

            ds.commit()

        print len(ds.tables), len(ds.tables)/tr.elapsed

        self.assertEqual(100, len(ds.tables))

        for t in ds.tables:
            self.assertEqual(11, len(t.columns)) # 10 + id column

