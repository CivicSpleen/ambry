"""

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

class RowGenerator(object):

    file_name = None

    generator = None
    column_map = None
    prefix_headers = ['id']
    header = None
    data_start_line = 1
    data_end_line = None
    header_lines = [0]
    header_comment_lines = []

    line_number = None

    put_row = None # A row that was put back to be iterated over again.


    def __init__(self, file, data_start_line = None, header_lines = None, header_comment_lines = None,
                 prefix_headers = None):
        """

        """

        self.file_name = file

        if data_start_line: self.data_start_line = data_start_line
        if header_lines: self.header_lines = header_lines
        if header_comment_lines: self.header_comment_lines = header_comment_lines
        if prefix_headers: self.prefix_headers = prefix_headers

        self._raw_row_gen = None

        self.put_row = None


    @property
    def raw_row_gen(self):
        """Generate all rows from the underlying source, with no consideration for wether the row is data, header
        or comment """
        if self._raw_row_gen is None:
            self._raw_row_gen = self._yield_rows()
            self.line_number = 0

        return self._raw_row_gen

    def reset(self):
        self._raw_row_gen = None
        self.line_number = 0

    def mangle_column_name(self, i, n):
        """
        Override this method to change the way that column names from the source are altered to
        become column names
        :param n:
        :return:
        """

        if not n:
            return 'column{}'.format(i)

        return n.strip()

    def get_header(self):
        """Open the file and read the rows for the header and header comments. It leaves the iterator
        positioned on the first data row. """

        if self.header:
            return self.header

        headers = []
        header_comments = []

        self.reset()
        row = None

        while self.line_number < self.data_start_line:

            row = self.raw_row_gen.next()

            if self.line_number in self.header_lines:

                headers.append([str(unicode(x).encode('ascii','ignore')) for x in row])

            if self.line_number in self.header_comment_lines:
                header_comments.append([str(unicode(x).encode('ascii', 'ignore')) for x in row])


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

                header = [' '.join(col_val if col_val else '' for col_val in col_set) for col_set in zip(*headers)]

            elif len(headers) > 0:
                header = headers.pop()

            else:
                header = []


            self.header = [ self.mangle_column_name(i,x) for i,x in enumerate(self.prefix_headers + header) ]

            if self.column_map:
                header = [ self.column_map.get(col,col) for col in header]

            self.header_comment = [' '.join(x) for x in zip(*header_comments)]


        return self.header

    def decode(self,v):
        """Decode a string into unicode"""
        return v


    def is_data_line(self, i,row):
        """Return true if a line is a data row"""
        return i >=1

    def is_header_line(self, i,row):
        """Return true if a l=row is part of the header"""
        return i == 0

    def is_header_comment_line(self, i,row):
        """Return true if a line is a header comment"""
        return False

    def intuit_row_spec(self):
        """Classify the rows of an excel file as header, comment and start of data

        Relies on definitions of methods of:
            is_data_line(i,row)
            is_header_line(i,row)
            is_header_comment_line(i,row)
        """

        data_start_line = None
        data_end_line = None
        header_lines = [3, 4]

        header = []
        header_comments = []


        self.reset()
        for row in self.raw_row_gen:

            i = self.line_number

            if not data_start_line:

                if self.is_data_line(i,row):
                    data_start_line = i

                elif self.is_header_comment_line(i,row):
                    header_comments.append(i)
                elif self.is_header_line(i,row):
                    header.append(i)

            elif data_start_line and not data_end_line:
                if not self.is_data_line(i, row):
                    data_end_line = i


        self.data_start_line=data_start_line
        self.data_end_line=data_end_line
        self.header_comment_lines=header_comments
        self.header_lines=header

        return dict(
            data_start_line=data_start_line,
            data_end_line=data_end_line,
            header_comment_lines=header_comments,
            header_lines=header
        )

    def __iter__(self):
        """Generate rows for a source file. The source value must be specified in the sources config"""


        self.get_header()

        pre = [None]*len(self.prefix_headers)

        for row in self.raw_row_gen:

            if self.put_row:
                yield pre + self.put_row
                self.put_row = None

            yield  pre + row


class DelimitedRowGenerator(RowGenerator):


    delimiter = None

    def __init__(self, file, data_start_line=None, header_lines=None, header_comment_lines=None, prefix_headers=None,
                 segment=0, delimiter = ','):

        super(DelimitedRowGenerator, self).__init__(file, data_start_line, header_lines, header_comment_lines,
                                                    prefix_headers, segment)

        self.delimiter = delimiter

    def get_csv_reader(self, f, sniff = False):
        import csv

        if sniff:
            dialect = csv.Sniffer().sniff(f.read(5000))
            f.seek(0)
        else:
            dialect = None

        delimiter = self.delimiter if self.delimiter else ','

        return csv.reader(f, delimiter = delimiter, dialect=dialect)

    def _yield_rows(self):

        self.line_number = 0
        with open(self.file_name,'rU') as f:
            for row in self.get_csv_reader(f):
                yield row
                self.line_number += 1

class ExcelRowGenerator(RowGenerator):




    def __init__(self, file, data_start_line=None, header_lines=None,
                 header_comment_lines=None, prefix_headers=None, segment = 0):

        super(ExcelRowGenerator, self).__init__(file, data_start_line, header_lines, header_comment_lines,
                                                prefix_headers)

        self.segment = segment

    def _yield_rows(self):
        from xlrd import open_workbook

        wb = open_workbook(self.file_name)

        self.workbook = wb

        s = wb.sheets()[self.segment]

        for i in range(0,s.nrows):
            yield self.srow_to_list(i, s)
            self.line_number += 1

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