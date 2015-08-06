# -*- coding: utf-8 -*-
import os
import unittest

import xlrd

from ambry.etl.intuit import RowIntuiter


class Test(unittest.TestCase):

    def test_two_comments_two_headers_300_rows(self):
        TEST_FILES_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'functional', 'files')
        file_name = 'two_comments_two_headers_300_data_rows.xls'

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

        p1 = RowIntuiter()
        p1.set_source_pipe(XlsSource())

        ret = list(p1)

        # contains valid rows
        self.assertEquals(len(ret), 300)
        self.assertEquals(ret[0][0], 1)
        self.assertEquals(ret[0][1], '0O0P01')
        self.assertEquals(ret[0][2], 1447)

        # FIXME: How can we overcome float number round error?
        # FIXME: check float values too., 13.6176070905, 42.2481751825, 8.272140707])

        # row intuiter accumulated proper comments
        self.assertEquals(
            p1.comments, ['Renter Costs', 'This is a header comment'])

        # row intuiter properly recognized header.
        self.assertEquals(
            p1.header,
            ['id', 'gvid', 'cost_gt_30', 'cost_gt_30_cv', 'cost_gt_30_pct', 'cost_gt_30_pct_cv'])
