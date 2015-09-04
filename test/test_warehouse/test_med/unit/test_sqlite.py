# -*- coding: utf-8 -*-
import os
import unittest

import apsw

from six import u, b

from sqlalchemy import create_engine
from sqlalchemy.orm import create_session

from ambry.warehouse.med.sqlite import add_partition, _as_orm, _table_name

from test.test_warehouse.test_med.unit import BaseMEDTest


class Test(BaseMEDTest):

    def test_1creates_virtual_table(self):
        partition_vid = 'vid1'
        partition = self._get_fake_partition(partition_vid)
        connection = apsw.Connection(':memory:')
        add_partition(connection, partition)

        # select from virtual table.
        cursor = connection.cursor()
        query = 'SELECT col1, col2 FROM {};'.format(_table_name(partition))
        result = cursor.execute(query).fetchall()
        self.assertEqual(len(result), 100)
        self.assertEqual(result[0], (0, b('0')))
        self.assertEqual(result[-1], (99, b('99')))

    def test_many_queries_on_one_partition(self):
        partition_vid = 'vid1'
        partition = self._get_fake_partition(partition_vid)
        connection = apsw.Connection(':memory:')
        add_partition(connection, partition)

        # select all from virtual table.
        cursor = connection.cursor()
        query = 'SELECT col1, col2 FROM {};'.format(_table_name(partition))
        result = cursor.execute(query).fetchall()
        self.assertEqual(len(result), 100)
        self.assertEqual(result[0], (0, b('0')))
        self.assertEqual(result[-1], (99, b('99')))

        # select first three records
        query = 'SELECT col1, col2 FROM {} LIMIT 3;'.format(_table_name(partition))
        result = cursor.execute(query).fetchall()
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], (0, b('0')))
        self.assertEqual(result[1], (1, b('1')))
        self.assertEqual(result[2], (2, b('2')))

        # select with filtering
        query = 'SELECT col1 FROM {} WHERE col1=\'1\';'.format(_table_name(partition))
        result = cursor.execute(query).fetchall()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], (1,))

    def test_creates_virtual_table_for_each_partition(self):
        partitions = []
        connection = apsw.Connection(':memory:')
        for i in range(1000):
            partition_vid = 'vid_{}'.format(i)
            partition = self._get_fake_partition(partition_vid)
            add_partition(connection, partition)
            partitions.append(partition)

        # check all tables and rows.
        cursor = connection.cursor()
        for partition in partitions:
            query = 'SELECT col1, col2 FROM {};'.format(_table_name(partition))
            result = cursor.execute(query).fetchall()
            self.assertEqual(len(result), 100)
            self.assertEqual(result[0], (0, b('0')))
            self.assertEqual(result[-1], (99, b('99')))

    @unittest.skip('sqlite module created by apsw is not visible by pysqlite.')
    def test_partition_row_orm(self):
        partition_vid = 'vid1'
        partition = self._get_fake_partition(partition_vid)
        # we can't use in memory database here because we are going to switch between backends and
        # each backend will use it own in memory database.
        # NOTE: I couldn't make shared in memory database to work.
        #    Details: https://www.sqlite.org/inmemorydb.html, look for 'shared'.
        DB_FILE = '/tmp/test_sqlite_vt1.sqlite'
        try:
            connection = apsw.Connection(DB_FILE)
            add_partition(connection, partition)
            connection.close()

            # create ORM and test it
            engine = create_engine('sqlite:///{}'.format(DB_FILE))
            with engine.connect() as conn:
                PartitionRow = _as_orm(conn, partition)
                session = create_session(bind=engine)
                all_rows = session.query(PartitionRow).all()
                self.assertEqual(len(all_rows), 100)
                self.assertEqual(all_rows[0].rowid, 0)
                self.assertEqual(all_rows[0].col1, 0)
                self.assertEqual(all_rows[0].col2, '0')
        finally:
            os.remove(DB_FILE)

    def test_date_and_datetime(self):
        partition_vid = 'vid1'
        partition = self._get_fake_datetime_partition(partition_vid)
        connection = apsw.Connection(':memory:')
        add_partition(connection, partition)

        # select from virtual table.
        cursor = connection.cursor()
        query = 'SELECT col1, col2 FROM {};'.format(_table_name(partition))
        result = cursor.execute(query).fetchall()
        self.assertEqual(len(result), 100)
        self.assertEqual(result[0], (u('2015-08-30'), u('2015-08-30T11:41:32.977993')))
