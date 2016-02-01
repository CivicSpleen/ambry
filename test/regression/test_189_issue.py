# -*- coding: utf-8 -*-
from test.test_base import TestBase


class Test(TestBase):

    def test_created_sqlite_table_name_contains_vid_only(self):
        bundle = self.import_single_bundle('ingest.example.com/basic')
        bundle.ingest()
        bundle.source_schema()
        bundle.schema()
        bundle.build()
        partition = bundle.library.datasets[0].partitions[0]
        warehouse = bundle.warehouse('temp1')
        warehouse.install(partition.vid)
        self.assertIn('sqlite', warehouse._backend._dsn)

        # First ensure query is working
        result = warehouse.query('SELECT * FROM {} LIMIT 1;'.format(partition.vid))
        expected_result = [(1, u'eb385c36-9298-4427-8925-fe09294dbd5f', 30, 99.7346915319786)]
        self.assertEqual(result, expected_result)

        # Now check created table. The name should match to partition.vid.
        cursor = warehouse._backend._connection.cursor()
        cursor.execute('select name from sqlite_master;')
        result = cursor.fetchall()
        self.assertEqual(partition.vid, result[0][0])
