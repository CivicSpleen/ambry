# -*- coding: utf-8 -*-
import os
import unittest

import xlrd

from ambry.etl.intuit import RowIntuiter

from contexttimer import Timer

class Test(unittest.TestCase):

    def test_basic(self):

        from ambry.etl.pipeline import RowProxy
        import string

        headers = list(string.ascii_letters)
        row = range(len(headers))

        rp = RowProxy(headers)

        rp.set_row(row)

        self.assertEqual(row, rp.get_row())
        self.assertEqual(headers,  rp.get_keys())
        self.assertEqual( dict(zip(headers, row)),  rp.dict)

        rp.a = 10
        rp['b'] = 20
        rp[2] = 30

        self.assertEquals(10, rp['a'])
        self.assertEquals(20, rp[1])
        self.assertEquals(30, rp.c)

        # Performance Test

        row_sum = 0

        expected_sum = 87498250000

        num_rows = 50000
        num_cols = len(headers)
        rows = []

        for i in range(num_rows):
            rows.append(list([c * i for c in range(num_cols)]))

        with Timer() as t:
            for row in rows:
                rp.set_row(row)
                row_sum += rp[1] + rp[2] + rp[3] + rp[26] + rp[38]

        print 'int    ',num_rows / t.elapsed

        self.assertEquals(expected_sum, row_sum)

        row_sum = 0

        with Timer() as t:
            for row in rows:
                rp.set_row(row)
                row_sum += rp['b'] + rp['c'] + rp['d'] + rp['A'] + rp['M']

        print 'str    ',num_rows/t.elapsed

        self.assertEquals(expected_sum, row_sum)

        row_sum = 0

        with Timer() as t:
            for row in rows:
                rp.set_row(row)
                row_sum += rp.b + rp.c + rp.d + rp.A + rp.M

        print 'attr   ',num_rows / t.elapsed

        row_sum = 0

        # Compare to constructing a dict every row
        with Timer() as t:
            for row in rows:
                rp = dict(zip(headers, row))
                row_sum += rp['b'] + rp['c'] + rp['d'] + rp['A'] + rp['M']

        print 'dict   ',num_rows/t.elapsed

        self.assertEquals(expected_sum, row_sum)

        row_sum = 0

        # Compare to array access
        with Timer() as t:
            for row in rows:
                row_sum += row[1] + row[2] + row[3] + row[26] + row[38]

        print 'array  ',num_rows / t.elapsed

        self.assertEquals(expected_sum, row_sum)

        row_sum = 0

        # Compare to constructed lambda access
        with Timer() as t:

            l = lambda row: [row[1],row[2],row[3],row[26],row[38]]

            for row in rows:
                row_sum += sum(l(row))

        print 'lambda ',num_rows / t.elapsed

        self.assertEquals(expected_sum, row_sum)



