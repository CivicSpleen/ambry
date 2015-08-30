# -*- coding: utf-8 -*-
from datetime import date, datetime

from sqlalchemy.orm import create_session
from sqlalchemy.pool import NullPool
from sqlalchemy import create_engine

from ambry.warehouse.med.postgresql import add_partition, _table_name, _as_orm

from test.test_base import PostgreSQLTestBase
from test.test_warehouse.test_med.unit import BaseMEDTest


class Test(BaseMEDTest):

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

    def test_date_and_datetime(self):
        # create fake partition.
        partition_vid = 'vid1'
        partition = self._get_fake_datetime_partition(partition_vid)

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
                self.assertEqual(
                    result[0],
                    (0, date(2015, 8, 30), datetime(2015, 8, 30, 11, 41, 32, 977993)))
        finally:
            PostgreSQLTestBase._drop_postgres_test_db()
