# -*- coding: utf-8 -*-
import unittest

import fudge

from ambry.etl.intuit import RowIntuiter, NoMatchError


class RowIntuiterTest(unittest.TestCase):

    # helpers
    def _get_source_pipe(self, rows=None):
        if not rows:
            rows = [
                ['elem1-1', 'elem1-2']]

        class FakeSourcePipe(object):
            def __iter__(self):
                for row in rows:
                    yield row
        return FakeSourcePipe()

    # _matches tests
    def test_returns_true_if_row_matches_to_pattern(self):
        # matches mean that all types of the elems in the row are in the pattern.
        pipe1 = RowIntuiter()
        pattern = [set([str])]
        row = ['test']
        self.assertTrue(pipe1._matches(row, pattern))

    def test_returns_false_if_row_does_not_match_to_pattern(self):
        # matches mean that all types of the elems in the row are in the pattern.
        pattern = [set([str])]
        row = [1]
        self.assertFalse(RowIntuiter()._matches(row, pattern))

    # _find_first_match_idx tests
    @fudge.patch(
        'ambry.etl.intuit.RowIntuiter._matches')
    def test_returns_first_match_idx(self, fake_matches):
        # prepare state
        data_pattern = [set([str, int, float])]
        rows = [
            ['header0', None, None],
            ['Header1', None, None]]

        rows.extend([[str(i), i, i + 0.1] for i in range(100)])

        fake_matches\
            .expects_call().returns(False)\
            .next_call().returns(False)\
            .next_call().returns(True)

        # testing
        first_line = RowIntuiter()._find_first_match_idx(rows, data_pattern)
        self.assertEqual(first_line, 2)
        self.assertEqual(rows[first_line], ['0', 0, 0.1])

    @fudge.patch(
        'ambry.etl.intuit.RowIntuiter._matches')
    def test_raises_NoMatchError_if_first_match_not_found(self, fake_matches):
        # prepare state
        data_pattern = [set([str, int, float])]
        rows = [
            ['Header1', None, None]]

        rows.extend([[str(i), i, i + 0.1] for i in range(100)])

        fake_matches.expects_call().returns(False)

        # testing
        with self.assertRaises(NoMatchError):
            RowIntuiter()._find_first_match_idx(rows, data_pattern)

    # _find_first_match_idx tests
    @fudge.patch(
        'ambry.etl.intuit.RowIntuiter._matches')
    def test_finds_last_match_idx(self, fake_matches):
        # prepare state
        data_pattern = [set([str, int, float])]
        rows = [
            ['header0', None, None],
            ['Header1', None, None]]

        rows.extend([[str(i), i, i + 0.1] for i in range(100)])

        fake_matches\
            .expects_call().returns(True)

        last_line = RowIntuiter()._find_last_match_idx(rows, data_pattern)
        assert last_line >= 0, '_find_last_match_idx should not return negative indexes.'
        self.assertEqual(rows[last_line], ['99', 99, 99.1])

    @fudge.patch(
        'ambry.etl.intuit.RowIntuiter._matches')
    def test_raises_NoMatchError_if_last_match_does_not_exist(self, fake_matches):
        # prepare state
        data_pattern = [set([str, int, float])]
        rows = [
            [None, None, None]]

        rows.extend([[str(i), i, i + 0.1] for i in range(100)])

        fake_matches\
            .expects_call().returns(False)\

        with self.assertRaises(NoMatchError):
            RowIntuiter()._find_last_match_idx(rows, data_pattern)

    # _find_header tests
    @fudge.patch(
        'ambry.etl.intuit.RowIntuiter._matches')
    def test_returns_first_row_matches_to_pattern(self, fake_matches):
        fake_matches.expects_call().returns(False)\
            .next_call().returns(True)

        pipe1 = RowIntuiter()
        rows = [
            ['Comment', None, None],
            ['header1-1', 'header1-2', 'header1-3'],
            ['header2-1', 'header2-2', 'header2-3']]

        header_pattern = [set([str, None]), set([str, None])]
        header = pipe1._find_header(rows, header_pattern)
        self.assertEqual(
            header,
            ['header1-1', 'header1-2', 'header1-3'])

    @fudge.patch(
        'ambry.etl.intuit.RowIntuiter._matches')
    def test_ignores_rows_with_low_match_ratio(self, fake_matches):
        fake_matches.expects_call().returns(False)\
            .next_call().returns(True)\
            .next_call().returns(True)

        pipe1 = RowIntuiter()
        rows = [
            ['Comment', None, None],
            ['header1-1', None, None],
            ['header2-1', 'header2-2', 'header2-3']]

        header_pattern = [set([str, None]), set([str, None])]
        header = pipe1._find_header(rows, header_pattern)
        self.assertEqual(
            header,
            ['header2-1', 'header2-2', 'header2-3'])

    # _find_comments tests
    @fudge.patch(
        'ambry.etl.intuit.RowIntuiter._matches')
    def test_joins_comments_to_list(self, fake_matches):
        fake_matches.expects_call().returns(False)\
            .next_call().returns(True)\
            .next_call().returns(True)\
            .next_call().returns(False)

        pipe1 = RowIntuiter()
        rows = [
            [None, None, None],
            ['Comment1', None, None],
            ['Comment2', None, None],
            ['header2-1', 'header2-2', 'header2-3']]

        comments_pattern = [set([str, None]), set([str, None]), set([None])]
        comments = pipe1._find_comments(rows, comments_pattern)
        self.assertEqual(
            comments,
            ['Comment1', 'Comment2'])

    # _get_patterns tests
    def test_returns_comments_pattern(self):
        rows = [
            ['Comment from row 0', None],
            ['Comment from row 1', None],
            ['Comment from row 2', None]
        ]
        # extend with data rows
        rows.extend([[str(i), i] for i in range(200)])
        pipe1 = RowIntuiter()
        comments_pattern, _, _ = pipe1._get_patterns(rows)
        self.assertEqual(len(comments_pattern), len(rows[0]),
                         'Comments pattern length has to match to columns amount.')
        self.assertEqual(comments_pattern, [set([str, None]), set([str, None])])

    def test_returns_header_pattern(self):
        # to get header pattern all patterns should be replaced with str|none.
        rows = [
            ['Header1', 'Header2'],
        ]
        # extend with data rows
        rows.extend([[str(i), i] for i in range(200)])

        pipe1 = RowIntuiter()
        _, header_pattern, _ = pipe1._get_patterns(rows)
        self.assertEqual(
            header_pattern,
            [set([str, None]), set([str, None])])

    def test_returns_data_pattern(self):
        rows = [
            ['header1', 'header2'],
        ]
        rows.extend([['s-{}'.format(i), float(i)] for i in range(100)])
        rows.extend([[i, float(i)] for i in range(100)])

        pipe1 = RowIntuiter()
        _, _, data_pattern = pipe1._get_patterns(rows)
        self.assertEqual(len(data_pattern), len(rows[0]), 'Pattern length has to match to columns amount.')
        self.assertEqual(data_pattern, [set([int, str]), set([float])])

    # __iter__ tests
    @fudge.patch(
        'ambry.etl.intuit.RowIntuiter._find_first_match_idx',
        'ambry.etl.intuit.RowIntuiter._find_last_match_idx')
    def test_generates_data_rows(self, fake_find_first, fake_find_last):
        rows = [
            ['Comment', None],
            ['header1', 'header2'],
        ]

        # extend with data rows
        rows.extend([['data{}-1'.format(i), 'data{}-2'.format(i)] for i in range(200)])

        first_line = rows.index(['data0-1', 'data0-2'])
        last_line = rows.index(['data199-1', 'data199-2'])
        fake_find_first.expects_call().returns(first_line)
        fake_find_last.expects_call().returns(last_line)
        p1 = RowIntuiter()
        p1.set_source_pipe(self._get_source_pipe(rows))

        ret = list(p1)
        self.assertEqual(len(ret), 200)

        # contains data rows
        self.assertEqual(['data0-1', 'data0-2'], ret[0])
        self.assertEqual(['data1-1', 'data1-2'], ret[1])
        self.assertEqual(['data2-1', 'data2-2'], ret[2])
        self.assertEqual(['data198-1', 'data198-2'], ret[198])
        self.assertEqual(['data199-1', 'data199-2'], ret[199])

        # does not contain comment and header
        self.assertNotIn(['Comment', None], ret)
        self.assertNotIn(['header1', 'header2'], ret)

    @fudge.patch(
        'ambry.etl.intuit.RowIntuiter._find_first_match_idx',
        'ambry.etl.intuit.RowIntuiter._find_last_match_idx')
    def test_does_not_generate_footer_if_footer_does_not_have_data(self, fake_find_first, fake_find_last):
        # This is the case when first chunk contains footer only.
        rows = []
        header = [
            ['Comment', None],
            ['header1', 'header2'],
        ]
        rows.extend(header)

        # extend with data rows
        data_rows_amount = RowIntuiter.FIRST_ROWS - len(header) + RowIntuiter.DATA_SAMPLE_SIZE + RowIntuiter.LAST_ROWS
        rows.extend([['data{}-1'.format(i), 'data{}-2'.format(i)] for i in range(data_rows_amount)])

        # extend with footer
        rows.extend([
            ['Footer1', None],
            ['Footer2', None],
            ['Footer3', None]])

        first_line_idx = rows.index(['data0-1', 'data0-2'])
        fake_find_first.expects_call().returns(first_line_idx)

        # first time last idx is -1 because rows data only passed.
        # second time it raises an error because given chunk does not have data.
        fake_find_last\
            .expects_call().returns(data_rows_amount + len(header) - 1)\
            .next_call().raises(NoMatchError)
        p1 = RowIntuiter()
        p1.set_source_pipe(self._get_source_pipe(rows))

        ret = list(p1)
        self.assertEqual(len(ret), data_rows_amount)

        # contains data rows
        self.assertEqual(['data0-1', 'data0-2'], ret[0])
        self.assertEqual(['data1-1', 'data1-2'], ret[1])
        self.assertEqual(['data2-1', 'data2-2'], ret[2])

        # does not contain comment and header
        self.assertNotIn(['Comment', None], ret)
        self.assertNotIn(['header1', 'header2'], ret)

        # does not contain footer
        self.assertNotIn(['Footer1', None], ret)
        self.assertNotIn(['Footer2', None], ret)

    @fudge.patch(
        'ambry.etl.intuit.RowIntuiter._find_first_match_idx',
        'ambry.etl.intuit.RowIntuiter._find_last_match_idx')
    def test_does_not_generate_footer_if_footer_have_data(self, fake_find_first, fake_find_last):
        # This is the case when first chunk contains some data and footer.
        rows = []

        header = [
            ['header1', 'header2'],
        ]
        rows.extend(header)

        # extend with data rows
        data_rows_amount = RowIntuiter.FIRST_ROWS - len(header) + RowIntuiter.DATA_SAMPLE_SIZE + RowIntuiter.LAST_ROWS + int(RowIntuiter.CHUNK_DATA_SIZE / 2)
        rows.extend([['data{}-1'.format(i), 'data{}-2'.format(i)] for i in range(data_rows_amount)])

        # extend with footer
        rows.extend([
            ['Footer1', None],
            ['Footer2', None],
            ['Footer3', None]])

        first_line_idx = rows.index(['data0-1', 'data0-2'])
        fake_find_first.expects_call().returns(first_line_idx)

        # first time last idx is idx of last elem because rows with data only passed.
        # second time it raises an error because given chunk does not have data.
        fake_find_last\
            .expects_call().returns(data_rows_amount + len(header) - 1)\
            .next_call().returns(int(RowIntuiter.CHUNK_DATA_SIZE / 2) - 1)
        p1 = RowIntuiter()
        p1.set_source_pipe(self._get_source_pipe(rows))

        ret = list(p1)
        self.assertEqual(len(ret), data_rows_amount)

        # contains data rows
        self.assertEqual(['data0-1', 'data0-2'], ret[0])
        self.assertEqual(['data1-1', 'data1-2'], ret[1])
        self.assertEqual(['data2-1', 'data2-2'], ret[2])
        self.assertEqual(
            ['data{}-1'.format(data_rows_amount - 1), 'data{}-2'.format(data_rows_amount - 1)],
            ret[data_rows_amount - 1])

        # does not contain header
        self.assertNotIn(['Comment', None], ret)
        self.assertNotIn(['header1', 'header2'], ret)

        # does not contain footer
        self.assertNotIn(['Footer1', None], ret)
        self.assertNotIn(['Footer2', None], ret)

    @fudge.patch(
        'ambry.etl.intuit.RowIntuiter._find_first_match_idx',
        'ambry.etl.intuit.RowIntuiter._find_last_match_idx')
    def test_generates_data_from_many_chunks(self, fake_find_first, fake_find_last):
        # This is the case when there are many chunks and last chunk contains data and footer.
        rows = []

        header = [
            ['header1', 'header2'],
        ]
        rows.extend(header)

        # extend with data rows
        data_rows_amount = \
            RowIntuiter.FIRST_ROWS - len(header)\
            + RowIntuiter.DATA_SAMPLE_SIZE + RowIntuiter.LAST_ROWS
        # add 3 chunks
        data_rows_amount += (RowIntuiter.CHUNK_DATA_SIZE * 3)

        # create last chunk with half fill.
        data_rows_amount += int(RowIntuiter.CHUNK_DATA_SIZE / 2)
        rows.extend([['data{}-1'.format(i), 'data{}-2'.format(i)] for i in range(data_rows_amount)])

        # extend with footer
        rows.extend([
            ['Footer1', None],
            ['Footer2', None],
            ['Footer3', None]])

        first_line_idx = rows.index(['data0-1', 'data0-2'])
        fake_find_first.expects_call().returns(first_line_idx)

        # first time last idx is idx of last elem because rows with data only passed.
        # second time it raises an error because given chunk does not have data.
        fake_find_last\
            .expects_call().returns(data_rows_amount + len(header) - 1)\
            .next_call().returns(int(RowIntuiter.CHUNK_DATA_SIZE / 2) - 1)
        p1 = RowIntuiter()
        p1.set_source_pipe(self._get_source_pipe(rows))

        ret = list(p1)
        self.assertEqual(len(ret), data_rows_amount)

        # contains data rows
        self.assertEqual(['data0-1', 'data0-2'], ret[0])
        self.assertEqual(['data1-1', 'data1-2'], ret[1])
        self.assertEqual(['data2-1', 'data2-2'], ret[2])
        self.assertEqual(
            ['data{}-1'.format(data_rows_amount - 1), 'data{}-2'.format(data_rows_amount - 1)],
            ret[data_rows_amount - 1])

        # does not contain header
        self.assertNotIn(['Comment', None], ret)
        self.assertNotIn(['header1', 'header2'], ret)

        # does not contain footer
        self.assertNotIn(['Footer1', None], ret)
        self.assertNotIn(['Footer2', None], ret)
