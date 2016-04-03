# -*- coding: utf-8 -*-
from test.proto import TestBase

from ambry.mprlib.backends.postgresql import PostgreSQLBackend
from ambry_sources.med.postgresql import POSTGRES_PARTITION_SCHEMA_NAME


class SQLiteInspector(object):

    @staticmethod
    def assert_table(warehouse, partition):
        cursor = warehouse._backend._connection.cursor()
        cursor.execute('select name from sqlite_master;')
        result = cursor.fetchall()
        assert partition.vid == result[0][0]


class PostgreSQLInspector(object):

    @staticmethod
    def assert_table(warehouse, partition):
        relation = '{}.{}'.format(POSTGRES_PARTITION_SCHEMA_NAME, partition.vid)
        assert PostgreSQLBackend._relation_exists(
            warehouse._backend._connection,
            relation)


class Test(TestBase):

    def setUp(self):
        super(Test, self).setUp()
        if self._db_type == 'sqlite':
            self._inspector = SQLiteInspector
        elif self._db_type == 'postgres':
            self._inspector = PostgreSQLInspector
        else:
            raise Exception('Do not know inspector for {} database.'.format(self.dbname))

    def test_created_table_name_contains_vid_only(self):
        bundle = self.import_single_bundle('ingest.example.com/basic')
        bundle.ingest()
        bundle.source_schema()
        bundle.schema()
        bundle.build()
        partition = bundle.library.datasets[0].partitions[0]
        warehouse = bundle.warehouse('temp1')
        warehouse.install(partition.vid)

        # First ensure query is working
        expected_result = [(1, u'eb385c36-9298-4427-8925-fe09294dbd5f', 30, 99.7346915319786)]
        result = warehouse.query('SELECT * FROM {} LIMIT 1;'.format(partition.vid))
        if 'sqlite' in warehouse._backend._dsn:
            result = result.fetchall()
        self.assertEqual(result, expected_result)

        # Now check created table. The name should match to partition.vid.
        self._inspector.assert_table(warehouse, partition)
