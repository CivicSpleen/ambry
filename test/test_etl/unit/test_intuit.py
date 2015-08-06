# -*- coding: utf-8 -*-
import unittest

import fudge

from ambry.etl.intuit import RowIntuiter


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
        pipe1 = RowIntuiter()
        pattern = [set([str])]
        row = [1]
        self.assertFalse(pipe1._matches(row, pattern))

    # _find_data_lines tests
    @fudge.patch(
        'ambry.etl.intuit.RowIntuiter._matches')
    def test_finds_first_line_in_the_header(self, fake_matches):
        # prepare state
        data_pattern = [set([str, int, float])]
        rows = [
            ['header0', None, None],
            ['Header1', None, None]]

        rows.extend([[str(i), i, i + 0.1] for i in range(100)])

        fake_matches\
            .expects_call().returns(False)\
            .next_call().returns(False)\
            .next_call().returns(True)\
            .next_call().returns(True)  # we need not empty last line.

        # testing
        pipe1 = RowIntuiter()
        first_line, last_line = pipe1._find_data_lines(rows, data_pattern)
        self.assertEquals(first_line, 2)
        self.assertEquals(rows[first_line], ['0', 0, 0.1])

    @fudge.patch(
        'ambry.etl.intuit.RowIntuiter._matches')
    def test_finds_last_line_in_the_footer(self, fake_matches):
        # prepare state
        data_pattern = [set([str, int, float])]
        rows = [
            ['header0', None, None],
            ['Header1', None, None]]

        rows.extend([[str(i), i, i + 0.1] for i in range(100)])

        fake_matches\
            .expects_call().returns(True)\
            .next_call().returns(False)\
            .next_call().returns(True)

        pipe1 = RowIntuiter()
        first_line, last_line = pipe1._find_data_lines(rows, data_pattern)
        self.assertEquals(rows[last_line], ['99', 99, 99.1])

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
        self.assertEquals(
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
        self.assertEquals(
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
        self.assertEquals(
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
        self.assertEquals(len(comments_pattern), len(rows[0]),
                          'Comments pattern length has to match to columns amount.')
        self.assertEquals(comments_pattern, [set([str, None]), set([str, None])])

    def test_returns_header_pattern(self):
        # to get header pattern all patterns should be replaced with str|none.
        rows = [
            ['Header1', 'Header2'],
        ]
        # extend with data rows
        rows.extend([[str(i), i] for i in range(200)])

        pipe1 = RowIntuiter()
        _, header_pattern, _ = pipe1._get_patterns(rows)
        self.assertEquals(
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
        self.assertEquals(len(data_pattern), len(rows[0]), 'Pattern length has to match to columns amount.')
        self.assertEquals(data_pattern, [set([int, str]), set([float])])

    # __iter__ tests
    @fudge.patch(
        'ambry.etl.intuit.RowIntuiter._find_data_lines')
    def test_generates_data_rows(self, fake_find):
        rows = [
            ['Comment', None],
            ['header1', 'header2'],
        ]

        # extend with data rows
        rows.extend([['data{}-1'.format(i), 'data{}-2'.format(i)] for i in range(201)])

        first_line = rows.index(['data0-1', 'data0-2'])
        last_line = rows.index(['data200-1', 'data200-2'])
        fake_find.expects_call().returns((first_line, last_line))
        p1 = RowIntuiter()
        p1.set_source_pipe(self._get_source_pipe(rows))

        ret = list(p1)
        self.assertEquals(len(ret), 200)

        # contains data rows
        self.assertIn(['data1-1', 'data1-2'], ret)
        self.assertIn(['data2-1', 'data2-2'], ret)
        self.assertIn(['data3-1', 'data3-2'], ret)

        # does not contain comment and header
        self.assertNotIn(['Comment', None], ret)
        self.assertNotIn(['header1', 'header2'], ret)
