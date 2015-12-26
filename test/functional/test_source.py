# -*- coding: utf-8 -*-

from test.test_base import TestBase
from test.factories import DatasetFactory


class Test(TestBase):

    # @unittest.skip('Need to attach dataset before can set table. ')
    def test_source_basic(self):
        """Basic operations on datasets"""

        db = self.library().database
        DatasetFactory._meta.sqlalchemy_session = db.session
        dataset = DatasetFactory()

        source = dataset.new_source('st')

        db.commit()  # FIXME Should not have to do this.

        source = dataset.source_file('st')
        dataset._database = db
        st = source.source_table

        for i, typ in enumerate([int, str, float]):
            st.add_column(i, str(i), typ, dest_header=str(i)+'_'+str(i))

        db.commit()

        st = source.source_table

        self.assertEqual(1, len(dataset.sources))
        self.assertEqual(1, len(dataset.source_tables))
        self.assertEqual(3, len(dataset.source_columns))

        self.assertEqual({'1': '1_1', '0': '0_0', '2': '2_2'}, source.column_map)
        self.assertEqual({'1': 1, '0': 0, '2': 2}, source.column_index_map)

        for i in range(10):
            source = dataset.new_source('source' + str(i))
            db.commit()
            t = source.source_table
            self.assertEqual('source' + str(i), t.name)
