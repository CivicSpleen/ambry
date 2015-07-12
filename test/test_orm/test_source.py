
import unittest
from test.test_base import TestBase

class Test(TestBase):

    @unittest.skip('Need to attach dataset before can set table. ')
    def test_source_basic(self):
        """Basic operations on datasets"""

        db = self.new_database()
        ds = self.new_db_dataset(db)

        source = ds.new_source('st')

        st = source.source_table()

        for i, typ in enumerate([int,str,float]):
            st.add_column(i, str(i), typ)

        ds.commit()

        st = source.source_table()

        self.assertEqual(1, len(ds.sources))
        self.assertEqual(1, len(ds.source_tables))
        self.assertEqual(3, len(ds.source_columns))