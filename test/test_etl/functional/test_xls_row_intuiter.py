# -*- coding: utf-8 -*-
import os
import unittest

import xlrd

from ambry.etl.intuit import RowIntuiter

TEST_FILES_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'functional', 'files')


class Test(unittest.TestCase):

    def _get_source(self, file_name):
        """ Creates source pipe from xls wigh given file name and returns it."""

        class XlsSource(object):
            def __iter__(self):
                book = xlrd.open_workbook(os.path.join(TEST_FILES_DIR, file_name))
                sheet = book.sheet_by_index(0)
                num_cols = sheet.ncols
                for row_idx in range(0, sheet.nrows):
                    row = []
                    for col_idx in range(0, num_cols):
                        value = sheet.cell(row_idx, col_idx).value
                        if value == '':
                            # FIXME: Is it valid requirement?
                            # intuiter requires None's in the empty cells.
                            value = None
                        row.append(value)
                    yield row
        return XlsSource()

    def test_two_comments_two_headers_300_rows(self):
        file_name = 'two_comments_two_headers_300_data_rows.xls'

        p1 = RowIntuiter()
        p1.set_source_pipe(self._get_source(file_name))

        ret = list(p1)

        # contains valid rows
        self.assertEqual(len(ret), 300)
        self.assertEqual(ret[0][0], 1)
        self.assertEqual(ret[0][1], '0O0P01')
        self.assertEqual(ret[0][2], 1447)
        self.assertAlmostEqual(ret[0][3], 13.6176070904818)
        self.assertAlmostEqual(ret[0][4], 42.2481751825)
        self.assertAlmostEqual(ret[0][5], 8.272140707)

        # row intuiter accumulated proper comments
        self.assertEqual(
            p1.comments, ['Renter Costs', 'This is a header comment'])

        # row intuiter properly recognized header.
        self.assertEqual(
            p1.header,
            ['id', 'gvid', 'cost_gt_30', 'cost_gt_30_cv', 'cost_gt_30_pct', 'cost_gt_30_pct_cv'])

    def test_one_header_300_rows(self):
        file_name = 'one_header_300_data_rows.xls'

        p1 = RowIntuiter()
        p1.set_source_pipe(self._get_source(file_name))

        ret = list(p1)

        # contains valid rows
        self.assertEqual(len(ret), 300)
        self.assertEqual(ret[0][0], 1)
        self.assertEqual(ret[0][1], '0O0P01')
        self.assertEqual(ret[0][2], 1447)
        self.assertAlmostEqual(ret[0][3], 13.6176070904818)
        self.assertAlmostEqual(ret[0][4], 42.2481751825)
        self.assertAlmostEqual(ret[0][5], 8.272140707)

        # intuiter does not have comments
        self.assertEqual(p1.comments, [])

        # row intuiter properly recognized header.
        self.assertEqual(
            p1.header,
            ['id', 'gvid', 'cost_gt_30', 'cost_gt_30_cv', 'cost_gt_30_pct', 'cost_gt_30_pct_cv'])

    def test_one_header_300_rows_one_footer(self):
        file_name = 'one_header_300_row_one_footer.xls'

        p1 = RowIntuiter()
        p1.set_source_pipe(self._get_source(file_name))

        ret = list(p1)

        # contains valid rows
        self.assertEqual(len(ret), 300)
        self.assertEqual(ret[0][0], 1)
        self.assertEqual(ret[0][1], '0O0P01')
        self.assertEqual(ret[0][2], 1447)
        self.assertAlmostEqual(ret[0][3], 13.6176070904818)
        self.assertAlmostEqual(ret[0][4], 42.2481751825)
        self.assertAlmostEqual(ret[0][5], 8.272140707)

        # intuiter does not have comments
        self.assertEqual(p1.comments, [])

        # row intuiter properly recognized header.
        self.assertEqual(
            p1.header,
            ['id', 'gvid', 'cost_gt_30', 'cost_gt_30_cv', 'cost_gt_30_pct', 'cost_gt_30_pct_cv'])
