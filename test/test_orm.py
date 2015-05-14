

from test_base import  TestBase
from ambry.orm import Column, Partition, Table, Dataset, Config, File,  Code, ColumnStat
from sqlalchemy.orm import sessionmaker
from ambry.identity import DatasetNumber, PartitionNumber
from sqlalchemy.exc import IntegrityError

class Test(TestBase):


    def setUp(self):
        from sqlalchemy import create_engine

        super(Test,self).setUp()

        self.dsn = "sqlite:///{}/test.db".format(self.tmpdir)

        self.engine = create_engine(self.dsn, echo=False)

        self.connection = self.engine.connect()

        self.Session = sessionmaker(bind=self.engine)

        self.session = self.Session()

        self.create_tables()

        self.delete_tmpdir = False

        self.dn = [ str(DatasetNumber(x, 5)) for x in range(1,5)]

    def create_tables(self):
        tables = [Dataset,Config,Table,Column,File,Partition,Code,ColumnStat]

        for table in tables:
            table.__table__.create(bind=self.engine)

    def new_dataset(self, n):
        return Dataset(vid=self.dn[n], source='source', dataset='dataset', creator='eric@busboom.org' )

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

        return Partition(ds, sequence_id=n, t_id = t_vids[n])

    def test_basic(self):

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










