"""

The RowGenerator reads a file and yields rows, handling simple headers in CSV
files, and complex headers with receeding comments in Excel files.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


class RowGenerator(object):
    file_name = None

    generator = None

    _header = None
    _unmangled_header = None
    footer = None
    data_start_line = 1
    data_end_line = None
    header_lines = [0]
    header_comment_lines = []
    header_mangler = None

    line_number = None

    put_row = None  # A row that was put back to be iterated over again.

    def __init__(self, file, data_start_line=None, data_end_line=None, header_lines=None, header_comment_lines=None,
                 header_mangler=None):
        """

        :param file:
        :param data_start_line: The line number ( zero indexed ) that is the first line of data
        :param data_end_line:  The line number of the last line of data.
        :param header_lines: A list of lines that are combined to be the header.
        :param header_comment_lines: A list of lines that are combined into a header comment
        :param header_mangler: A Function that transforms the header.
        :return:
        """

        self.file_name = file

        if data_start_line:
            self.data_start_line = data_start_line
        if data_end_line:
            self.data_end_line = data_end_line
        if header_lines:
            self.header_lines = header_lines
        if header_comment_lines:
            self.header_comment_lines = header_comment_lines

        if header_mangler:
            self.header_mangler = header_mangler

        self._raw_row_gen = None

        self.put_row = None

        self._header = None
        self._unmangled_header = None

    def add_intuition(self, data_start_line=None, data_end_line=None,
                      header_lines=None, header_comment_lines=None):

        if data_start_line:
            self.data_start_line = data_start_line
        if header_lines:
            self.header_lines = header_lines
        if header_comment_lines:
            self.header_comment_lines = header_comment_lines

    def _yield_rows(self):
        raise NotImplementedError

    @property
    def raw_row_gen(self):
        """Generate all rows from the underlying source, with no consideration
        for whether the row is data, header or comment """
        if self._raw_row_gen is None:
            self._raw_row_gen = self._yield_rows()
            self.line_number = 0

        return self._raw_row_gen

    def reset(self):
        self._raw_row_gen = None
        self.line_number = 0

    @property
    def header(self):

        if not self._header:
            self.get_header()

        return self._header

    @property
    def unmangled_header(self):
        if not self._unmangled_header:

            self.get_header()

        return self._unmangled_header

    def get_header(self):
        """Open the file and read the rows for the header and header comments. It leaves the iterator
        positioned on the first data row. """

        headers = []
        header_comments = []

        self.reset()
        row = None

        i = 0
        while self.line_number < self.data_start_line:

            row = self.raw_row_gen.next()

            if self.line_number in self.header_lines:
                headers.append([str(unicode(x).encode('ascii', 'ignore')) for x in row])

            if self.line_number in self.header_comment_lines:
                header_comments.append([str(unicode(x).encode('ascii', 'ignore')) for x in row])

            i += 1

            assert i < 8 or  self.line_number, 'The _yield_rows() method must increment self.line_number'


        self.put_row = row

        if self.line_number == self.data_start_line:
            # All of the header line are before this, so it is safe to construct the header now.

            if len(headers) > 1:

                # If there are gaps in the values in the first header line, extend them forward
                hl1 = []
                last = None
                for x in headers[0]:
                    if not x:
                        x = last
                    else:
                        last = x

                    hl1.append(x)

                headers[0] = hl1

                header = [' '.join(col_val.strip() if col_val else '' for col_val in col_set)
                          for col_set in zip(*headers)]
                header = [h.strip() for h in header]

            elif len(headers) > 0:
                header = headers.pop()

            else:
                header = []

            self._header = header

            self.header_comment = [' '.join(x) for x in zip(*header_comments)]

        if self.header_mangler:
            self._unmangled_header = list(self.header)
            self._header = self.header_mangler(self.header)
        else:
            self._unmangled_header = self._header = self.header


        return self._header

    def get_footer(self):

        if self.footer:
            return self.footer

        if not self.data_end_line:
            return None

        if self.line_number > self.data_end_line:
            self.reset()

        self.footer = []

        while True:
            try:
                row = self.raw_row_gen.next()
            except StopIteration:
                break

            if self.line_number < self.data_end_line:
                continue
            else:
                self.footer.append(' '.join(str(x) for x in row).strip())

        return self.footer

    def decode(self, v):
        """Decode a string into unicode"""
        return v

    def __iter__(self):
        """Generate rows for a source file. The source value must be specified in the sources config"""

        self.get_header()

        for row in self.raw_row_gen:

            if self.data_end_line and self.line_number >= self.data_end_line:
                break

            if self.put_row:
                yield self.put_row
                self.put_row = None

            yield row


class DelimitedRowGenerator(RowGenerator):
    delimiter = None

    def __init__(self, file, data_start_line=None, data_end_line=None, header_lines=None, header_comment_lines=None,
                 header_mangler=None, delimiter=','):

        super(DelimitedRowGenerator, self).__init__(
            file, data_start_line=data_start_line, data_end_line=data_end_line, header_lines=header_lines,
            header_comment_lines=header_comment_lines, header_mangler=header_mangler)

        self.delimiter = delimiter

    def get_csv_reader(self, f, sniff=False):
        # import unicodecsv as csv
        import csv

        if sniff:
            dialect = csv.Sniffer().sniff(f.read(5000))
            f.seek(0)
        else:
            dialect = None

        delimiter = self.delimiter if self.delimiter else ','

        return csv.reader(f, delimiter=delimiter, dialect=dialect)

    def _yield_rows(self):

        self.line_number = 0
        with open(self.file_name, 'rU') as f:
            for i, row in enumerate(self.get_csv_reader(f)):
                self.line_number = i
                yield row


class ExcelRowGenerator(RowGenerator):
    def __init__(self, file, data_start_line=None, data_end_line=None, header_lines=None, header_comment_lines=None,
                 header_mangler=None, segment=0):

        super(ExcelRowGenerator, self).__init__(file, data_start_line, data_end_line, header_lines,
                                                header_comment_lines, header_mangler)

        self.segment = segment
        self.workbook = None

    def make_datetime_caster(self):
        """Make a date caster function that can convert dates from this workbook. This is required
        because dates in Excel workbooks are stupid. """

        from xlrd import open_workbook

        wb = open_workbook(self.file_name)
        datemode = wb.datemode


        def excel_date(v):
            from xlrd import xldate_as_tuple
            import datetime

            try:
                year, month, day, hour, minute, second = xldate_as_tuple(float(v), datemode)
                return datetime.datetime(year, month, day, hour, minute, second)
            except ValueError:
                # Could be actually a string, not a float. Because Excel dates are completel broken.
                from  dateutil import parser
                try:
                    return parser.parse(v)
                except ValueError:
                    return None


        return excel_date


    def make_date_caster(self):
        """Make a date caster function that can convert dates from this workbook. This is required
        because dates in Excel workbooks are stupid. """

        from xlrd import open_workbook

        wb = open_workbook(self.file_name)
        datemode = wb.datemode

        def excel_date(v):
            from xlrd import xldate_as_tuple
            import datetime

            try:

                year, month, day, hour, minute, second = xldate_as_tuple(float(v), datemode)
                return datetime.date(year, month, day)
            except ValueError:
                # Could be actually a string, not a float. Because Excel dates are completel broken.
                from  dateutil import parser

                try:
                    return parser.parse(v).date()
                except ValueError:
                    return None

        return excel_date

    def _yield_rows(self):
        from xlrd import open_workbook
        from xlrd.biffh import XLRDError

        try:
            wb = open_workbook(self.file_name)
        except XLRDError:
            from zipfile import ZipFile
            # Usually b/c the .xls file is XML, but not zipped.

            self.file_name.replace('.xls', '.xml')

            wb = open_workbook(self.file_name)

        self.workbook = wb


        s = wb.sheets()[self.segment if self.segment else 0]

        for i in range(0, s.nrows):
            self.line_number = i
            yield self.srow_to_list(i, s)

    def srow_to_list(self, row_num, s):
        """Convert a sheet row to a list"""

        values = []

        for col in range(s.ncols):
            if self.decode:
                v = s.cell(row_num, col).value
                if isinstance(v, basestring):
                    v = self.decode(v)
                values.append(v)
            else:
                values.append(s.cell(row_num, col).value)

        return values

class GeneratorRowGenerator(RowGenerator):
    """A RowGenerator that is constructed on a function that returns a raw row generator function

    The first line generated must be the header
    """

    def __init__(self, raw_row_gen_f, header_mangler=None):
        """

        :param raw_row_gen_f: A raw row generator function
        :param header_mangler: A function to alter the list of header values
        :return:
        """

        self.rrg_f = raw_row_gen_f

        super(GeneratorRowGenerator, self).__init__(None, data_start_line=2, data_end_line=None, header_lines=[0],
                                                header_comment_lines=None, header_mangler=header_mangler)

    def _yield_rows(self):

        for i, line in enumerate(self.rrg_f()):
            self.line_number = i
            yield line



class RowSpecIntuiter(object):
    data_start_line = None
    data_end_line = None

    header = None
    header_comments = None

    lines = None

    def __init__(self, row_gen):
        from collections import defaultdict

        self.row_gen = row_gen

        self.header = []
        self.header_comments = []

        self.lines = []
        self.lengths = defaultdict(int)

        # Get the non-numm length of all of the rows, then find the midpoint between the min a max
        # We'll use that for the break point between comments and data / header.
        for i, row in enumerate(row_gen):
            if i > 100:
                break

            self.lines.append(row)

            lng = len(self.non_nulls(row))  # Number of non-nulls

            self.lengths[lng] += 1

        self.mid_length = (min(self.lengths.keys()) + max(self.lengths.keys())) / 2

    def non_nulls(self, row):
        """Return the non-empty values from a row"""
        return [col for col in row if bool(unicode(col).encode('ascii', 'replace').strip())]

    def is_data_line(self, i, row):
        """Return true if a line is a data row"""

        return self.header and len(self.non_nulls(row)) > self.mid_length

    def is_header_line(self, i, row):
        """Return true if a row is part of the header"""

        return not self.header and len(self.non_nulls(row)) > self.mid_length

    def is_header_comment_line(self, i, row):
        """Return true if a line is a header comment"""
        return not self.header  # All of the lines before the header

    def intuit(self):
        """Classify the rows of an excel file as header, comment and start of data

        Relies on definitions of methods of:
            is_data_line(i,row)
            is_header_line(i,row)
            is_header_comment_line(i,row)
        """

        self.row_gen.reset()
        for row in self.row_gen.raw_row_gen:

            i = self.row_gen.line_number

            is_dl = self.is_data_line(i, row)

            if self.data_end_line:
                continue
            elif not self.data_start_line:

                if is_dl:
                    self.data_start_line = i

                elif self.is_header_line(i, row):
                    self.header.append(i)

                elif self.is_header_comment_line(i, row):
                    self.header_comments.append(i)

            elif self.data_start_line and not self.data_end_line:
                if not is_dl:
                    self.data_end_line = i

        return dict(
            data_start_line=self.data_start_line,
            data_end_line=self.data_end_line,
            header_comment_lines=self.header_comments,
            header_lines=self.header
        )



