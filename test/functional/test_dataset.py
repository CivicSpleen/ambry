# -*- coding: utf-8 -*-

from ambry.identity import DatasetNumber
from ambry.orm.exc import ConflictError

from test.test_base import ConfigDatabaseTestBase


class Test(ConfigDatabaseTestBase):

    def setUp(self):

        super(Test, self).setUp()

        # Make an array of dataset numbers, so we can refer to them with a single integer
        self.dn = [str(DatasetNumber(x, x)) for x in range(1, 10)]

    def test_dataset_basic(self):
        """Basic operations on datasets"""
        library = self.library()
        db = library.database

        # Creating and conflicts
        #
        db.new_dataset(vid=self.dn[0], source='source', dataset='dataset')
        db.new_dataset(vid=self.dn[1], source='source', dataset='dataset')

        with self.assertRaises(ConflictError):
            db.new_dataset(vid=self.dn[0], source='source', dataset='dataset')

        dn = DatasetNumber(100)

        # datasets() gets datasets, and latest give id instead of vid
        #
        db.new_dataset(vid=str(dn.rev(5)), source='a', dataset='dataset')
        db.new_dataset(vid=str(dn.rev(1)), source='a', dataset='dataset')
        db.new_dataset(vid=str(dn.rev(3)), source='a', dataset='dataset')
        db.new_dataset(vid=str(dn.rev(4)), source='a', dataset='dataset')

        ds = db.dataset(str(dn.rev(5)))
        self.assertEqual(str(dn.rev(5)), ds.vid)

        ds = db.dataset(str(dn.rev(3)))
        self.assertEqual(str(dn.rev(3)), ds.vid)

        ds = db.dataset(str(dn.rev(None)))
        self.assertEqual(str(dn.rev(5)), ds.vid)

        db.new_dataset(vid=str(dn.rev(6)), source='a', dataset='dataset')

        ds = db.dataset(str(dn.rev(None)))
        self.assertEqual(str(dn.rev(6)), ds.vid)

        db.close()

    def test_config(self):

        db = self.library().database

        db.root_dataset.config.library.config.path = 'foobar'

        self.assertEqual('foobar',  db.root_dataset.config.library.config.path)

    def test_tables(self):
        db = self.library().database
        ds = db.new_dataset(vid=self.dn[0], source='source', dataset='dataset')

        ds.new_table('table1')
        ds.new_table('table2', description='table2', data=dict(a=1, b=2, c=3))
        ds.new_table('table3', description='table3')

        db.commit()

        t2 = ds.table('table2')
        t2.description = 'tablex'

        db.session.add(t2)
        db.commit()

        ds = db.dataset(ds.vid)  # Refresh the memory object
        ds.new_table('table2', data=dict(b=22))
        ds.new_table('table3', description='table3-description')

        db.commit()

        self.assertEqual(22, db.dataset(ds.vid).table('table2').data['b'])
        self.assertEqual('table3-description', db.dataset(ds.vid).table('table3').description)

    def test_partitions(self):
        db = self.library().database

        ds = db.new_dataset(vid=self.dn[0], source='source', dataset='dataset')

        t = ds.new_table('table2')

        p1 = ds.new_partition(t, time=1)
        p2 = ds.new_partition(t, time=2)
        p3 = ds.new_partition(t, time=3)

        assert p1 != p2
        assert p2 != p3

        db.commit()

        self.assertEqual(3, len(ds.partitions))

        # partitions saved to db.
        result = db.connection.execute('SELECT p_vid FROM partitions;').fetchall()
        self.assertEqual(len(result), 3)
        flatten = [x[0] for x in result]
        self.assertIn(p1.vid, flatten)
        self.assertIn(p2.vid, flatten)
        self.assertIn(p3.vid, flatten)
