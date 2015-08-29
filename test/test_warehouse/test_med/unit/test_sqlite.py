# -*- coding: utf-8 -*-
import unittest

import apsw

from ambry.warehouse.med.sqlite import add_partition

from test.test_warehouse.test_med.unit import BaseMEDTest


class Test(BaseMEDTest):

    def test_creates_virtual_table(self):
        partition_vid = 'vid1'
        partition = self._get_fake_partition(partition_vid)
        connection = apsw.Connection(':memory:')
        add_partition(connection, partition)

        # select from virtual table.
        cursor = connection.cursor()
        query = 'SELECT col1, col2 FROM {};'.format(partition_vid)
        result = cursor.execute(query).fetchall()
        self.assertEqual(len(result), 100)
        self.assertEqual(result[0], (0, '0'))
        self.assertEqual(result[-1], (99, '99'))

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
            query = 'SELECT col1, col2 FROM {};'.format(partition.vid)
            result = cursor.execute(query).fetchall()
            self.assertEqual(len(result), 100)
            self.assertEqual(result[0], (0, '0'))
            self.assertEqual(result[-1], (99, '99'))

    @unittest.skip('Not implemented.')
    def test_partition_row_orm(self):
        # FIXME:
        pass
