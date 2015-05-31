
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

        self.dsn = "sqlite:///{}/test.db".format(self.tmpdir)

        # Make an array of dataset numbers, so we can refer to them with a single integer
        self.dn = [str(DatasetNumber(x, x)) for x in range(1, 10)]

    def create_db(self):
        """Create a database outside of the orm.database.Database object"""
        from sqlalchemy import create_engine

        self.engine = create_engine(self.dsn, echo=False)

        self.connection = self.engine.connect()

        self.Session = sessionmaker(bind=self.engine)

        self.session = self.Session()

        self.create_tables()

        self.delete_tmpdir = False



    def create_tables(self):
        tables = [Dataset,Config,Table,Column,File,Partition,Code,ColumnStat]

        for table in tables:
            table.__table__.create(bind=self.engine)

    def new_dataset(self, n):
        return Dataset(vid=self.dn[n], source='source', dataset='dataset' )

    def new_table(self, ds, n):
        t =  Table(ds, sequence_id = n, name = 'table{}'.format(n))

        for i in range(5):
            c = Column(sequence_id = i, name = 'column_{}_{}'.format(n,i), datatype = 'Integer')
            t.columns.append(c)

            for i in range(5):
                c.add_code(str(i), str(i))

        return t

    def new_partition(self,ds,n):

        t_vids = sorted(t.id_ for t in ds.tables)

        return Partition(ds, sequence_id=n, t_id=t_vids[n])

    def dump_datasets(self, db):
        import sys
        from subprocess import check_output

        print check_output('sqlite3 {} "SELECT * FROM datasets" '.format(db.path), stderr=sys.stderr, shell=True)

    def test_dataset_basic(self):
        """Basic operations on datasets"""

        from ambry.orm.database import Database
        from ambry.orm import ConflictError
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



    def x_test_basic(self):

        self.session.add(self.new_dataset(0))

        self.session.commit()

        self.session.add(self.new_dataset(0))

        with self.assertRaises(IntegrityError):
            self.session.commit()

        self.session.rollback()

        ds = self.session.query(Dataset).filter(Dataset.vid==self.dn[0]).one()

        for i in range(5):
            t = self.new_table(ds, i)
            self.session.add(t)

        self.session.commit()

        for i in range(5):
            t = self.new_partition(ds, i)
            self.session.add(t)

        self.session.commit()

        for p in ds.partitions:
            for c in  p.table.columns:
                p.add_stat(c.vid, dict(count=10, mean=5))


        self.session.commit()

        self.assertEquals(25, len(self.session.query(ColumnStat).all()))
        self.assertEquals(25, len(self.session.query(Column).all()))
        self.assertEquals(125, len(self.session.query(Code).all()))

        return

        self.session.delete(ds)

        self.session.commit()

        # Deleting the dataset should cascade to the columns
        self.assertEquals(0, len(self.session.query(ColumnStat).all()))
        self.assertEquals(0, len(self.session.query(Code).all()))
        self.assertEquals(0, len(self.session.query(Column).all()))










