# -*- coding: utf-8 -*-
import os

from sqlalchemy.orm import create_session
from sqlalchemy.pool import NullPool
from sqlalchemy import create_engine, Column as SAColumn, Integer, String

from ambry.util import AttrDict
from ambry.fdw.postgresql import add_partition, _table_name, _as_orm

from test.test_base import TestBase, PostgreSQLTestBase


TEST_FILES_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), '../', 'files'))


class Test(TestBase):

    def _get_fake_partition(self, vid):
        table = AttrDict(
            columns=[
                SAColumn('rowid', Integer, primary_key=True),
                SAColumn('col1', Integer),
                SAColumn('col2', String(8))])
        datafile = AttrDict(
            syspath=os.path.join(TEST_FILES_DIR, 'rowid_int_col1_int_col2_str_100_rows.msg'))
        partition = AttrDict(vid=vid, table=table, datafile=datafile)
        return partition

    def test_creates_table(self):
        # create fake partition.
        partition_vid = 'vid1'
        partition = self._get_fake_partition(partition_vid)

        # testing.
        try:
            postgres_test_db_dsn = PostgreSQLTestBase._create_postgres_test_db()['test_db_dsn']
            engine = create_engine(postgres_test_db_dsn, poolclass=NullPool)
            with engine.connect() as connection:
                add_partition(connection, partition)

                # select from virtual table.
                table_name = _table_name(partition)
                result = connection.execute('SELECT rowid, col1, col2 from {};'.format(table_name)).fetchall()
                self.assertEqual(len(result), 100)
                self.assertEqual(result[0], (0, 0, '0'))
                self.assertEqual(result[-1], (99, 99, '99'))
        finally:
            PostgreSQLTestBase._drop_postgres_test_db()

    def test_creates_many_tables(self):
        try:
            postgres_test_db_dsn = PostgreSQLTestBase._create_postgres_test_db()['test_db_dsn']
            engine = create_engine(postgres_test_db_dsn, poolclass=NullPool)
            with engine.connect() as connection:
                partitions = []
                for i in range(100):
                    partition_vid = 'vid_{}'.format(i)
                    partition = self._get_fake_partition(partition_vid)
                    add_partition(connection, partition)
                    partitions.append(partition)

                # check all tables and rows.
                for partition in partitions:
                    table_name = _table_name(partition)
                    query = 'SELECT * FROM {};'.format(table_name)
                    result = connection.execute(query).fetchall()
                    self.assertEqual(len(result), 100)
        finally:
            PostgreSQLTestBase._drop_postgres_test_db()

    def test_partition_row_orm(self):
        # create fake partition.
        partition_vid = 'vid1'
        partition = self._get_fake_partition(partition_vid)

        # testing.
        try:
            postgres_test_db_dsn = PostgreSQLTestBase._create_postgres_test_db()['test_db_dsn']
            engine = create_engine(postgres_test_db_dsn, poolclass=NullPool)
            with engine.connect() as connection:
                # create foreign table.
                add_partition(connection, partition)

                # create ORM and test it
                PartitionRow = _as_orm(connection, partition)
                session = create_session(bind=connection.engine)
                all_rows = session.query(PartitionRow).all()
                self.assertEqual(len(all_rows), 100)
                self.assertEqual(all_rows[0].rowid, 0)
                self.assertEqual(all_rows[0].col1, 0)
                self.assertEqual(all_rows[0].col2, '0')
        finally:
            PostgreSQLTestBase._drop_postgres_test_db()
