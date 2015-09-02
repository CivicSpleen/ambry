# -*- coding: utf-8 -*-

from test.test_base import TestBase


class Test(TestBase):

    # @unittest.skip('Need to attach dataset before can set table. ')
    def test_source_basic(self):
        """Basic operations on datasets"""

        db = self.new_database()
        ds = self.new_db_dataset(db)

        source = ds.new_source('st')

        ds.commit()  # FIXME Should not have to do this.

        source = ds.source_file('st')
        st = source.source_table

        for i, typ in enumerate([int, str, float]):
            st.add_column(i, str(i), typ, dest_header=str(i)+'_'+str(i))

        ds.commit()

        st = source.source_table

        self.assertEqual(1, len(ds.sources))
        self.assertEqual(1, len(ds.source_tables))
        self.assertEqual(3, len(ds.source_columns))

        self.assertEqual({'1': '1_1', '0': '0_0', '2': '2_2'}, source.column_map)
        self.assertEqual({'1': 1, '0': 0, '2': 2}, source.column_index_map)

        #for s in ds.sources:
        #    print s.source_table
        #    print s.dest_table

        for i in range(10):
            source = ds.new_source("source"+str(i))
            ds.commit()
            t  = source.source_table
            self.assertEqual("source"+str(i), t.name)


