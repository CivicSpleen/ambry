# -*- coding: utf-8 -*-

import apsw

from ambry.util import AttrDict
from ambry.fdw.sqlite import add_partition

from test.test_base import TestBase


class Test(TestBase):
    def test_creates_virtual_table(self):
        partition_vid = 'vid1'
        partition = AttrDict(
            vid=partition_vid,
            columns=['rowid', 'col1', 'col2'],
            data=[[1, '1-1', '1-2'], [2, '2-1', '2-2']])
        connection = apsw.Connection(':memory:')
        add_partition(connection, partition)

        # select from virtual table.
        # Which 3 files are the biggest?
        cursor = connection.cursor()
        query = 'SELECT 1 FROM {};'.format(partition_vid)
        expected_result = [(1,), (1,)]
        result = cursor.execute(query).fetchall()
        self.assertEqual(result, expected_result)

    def test_creates_all_columns_of_the_partition(self):
        partition_vid = 'vid1'
        partition = AttrDict(
            vid=partition_vid,
            columns=['rowid', 'col1', 'col2'],
            data=[[1, '1-1', '1-2'], [2, '2-1', '2-2']])
        connection = apsw.Connection(':memory:')
        add_partition(connection, partition)

        cursor = connection.cursor()
        query = 'SELECT col1, col2 FROM {};'.format(partition_vid)
        expected_result = [('1-1', '1-2'), ('2-1', '2-2')]
        result = cursor.execute(query).fetchall()
        self.assertEqual(result, expected_result)

    def test_creates_many_virtual_tables(self):
        partitions = []
        connection = apsw.Connection(':memory:')
        for i in range(1000):
            partition_vid = 'vid_{}'.format(i)
            partition = AttrDict(
                vid=partition_vid,
                columns=['rowid', 'col{}_1'.format(i), 'col{}_2'.format(i)],
                data=[[1, '{}_1-1'.format(i), '{}_1-2'.format(i)],
                      [2, '{}_2-1'.format(i), '{}_2-2'.format(i)]])
            add_partition(connection, partition)
            partitions.append(partition)

        # check all tables and rows.
        cursor = connection.cursor()
        for partition in partitions:
            query = 'SELECT {} FROM {};'.format(', '.join(partition.columns[1:]), partition.vid)
            result = cursor.execute(query).fetchall()
            expected_result = [tuple(x[1:]) for x in partition.data]
            self.assertItemsEqual(result, expected_result)
