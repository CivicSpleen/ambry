"""

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

class RowGenerator(object):

    file_name = None

    generator = None

    prefix_headers = ['id']
    header = None
    footer = None
    data_start_line = 1
    data_end_line = None
    header_lines = [0]
    header_comment_lines = []
    header_mangler = None

    line_number = None

    put_row = None # A row that was put back to be iterated over again.

    def __init__(self, file, data_start_line = None, data_end_line = None,
                 header_lines = None, header_comment_lines = None, prefix_headers = None,
                 header_mangler = None):
        """

        """

        self.file_name = file

        if data_start_line: self.data_start_line = data_start_line
        if data_end_line: self.data_end_line = data_end_line
        if header_lines: self.header_lines = header_lines
        if header_comment_lines: self.header_comment_lines = header_comment_lines
        if prefix_headers: self.prefix_headers = prefix_headers
        if header_mangler: self.header_mangler = header_mangler

        self._raw_row_gen = None

        self.put_row = None

    def add_intuition(self, data_start_line=None, data_end_line = None, header_lines=None, header_comment_lines=None,
                      prefix_headers=None):

        if data_start_line: self.data_start_line = data_start_line
        if header_lines: self.header_lines = header_lines
        if header_comment_lines: self.header_comment_lines = header_comment_lines
        if prefix_headers: self.prefix_headers = prefix_headers

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

            self.header =  self.prefix_headers + header

            self.header_comment = [' '.join(x) for x in zip(*header_comments)]

        if self.header_mangler:
            self.header = self.header_mangler(self.header)

        return self.header

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

    def decode(self,v):
        """Decode a string into unicode"""
        return v


    def __iter__(self):
        """Generate rows for a source file. The source value must be specified in the sources config"""

        self.get_header()

        pre = [None]*len(self.prefix_headers)

        for row in self.raw_row_gen:

            if self.data_end_line and self.line_number >= self.data_end_line:
                break

            if self.put_row:
                yield pre + self.put_row
                self.put_row = None

            yield  pre + row


class DelimitedRowGenerator(RowGenerator):


    delimiter = None

    def __init__(self, file, data_start_line=None, header_lines=None,
                 header_comment_lines=None, prefix_headers=None,
                 header_mangler=None, delimiter = ','):

        super(DelimitedRowGenerator, self).__init__(file, data_start_line, header_lines, header_comment_lines,
                                                    prefix_headers, header_mangler)

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
            for i, row in enumerate(self.get_csv_reader(f)):
                self.line_number = i
                yield row

class ExcelRowGenerator(RowGenerator):

    def __init__(self, file, data_start_line=None, data_end_line=None, header_lines=None,
                 header_comment_lines=None, prefix_headers=None, header_mangler=None, segment = 0):

        super(ExcelRowGenerator, self).__init__(file, data_start_line, data_end_line, header_lines,
                                                header_comment_lines, prefix_headers, header_mangler)

        self.segment = segment
        self.workbook = None

    def _yield_rows(self):
        from xlrd import open_workbook

        wb = open_workbook(self.file_name)

        self.workbook = wb

        s = wb.sheets()[self.segment if self.segment else 0]

        for i in range(0,s.nrows):
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

class RowSpecIntuiter(object):

    data_start_line = None
    data_end_line = None

    header = None
    header_comments = None

    def __init__(self, row_gen):

        self.row_gen = row_gen

        self.header = []
        self.header_comments = []


    def is_data_line(self, i, row):
        """Return true if a line is a data row"""
        return i >= 1

    def is_header_line(self, i, row):
        """Return true if a l=row is part of the header"""
        return i == 0

    def is_header_comment_line(self, i, row):
        """Return true if a line is a header comment"""
        return False

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

                elif self.is_header_comment_line(i, row):
                    self.header_comments.append(i)

                elif self.is_header_line(i, row):
                    self.header.append(i)

            elif self.data_start_line and not self.data_end_line:
                if not is_dl:

                    self.data_end_line = i


        return dict(
            data_start_line=self.data_start_line,
            data_end_line=self.data_end_line,
            header_comment_lines=self.header_comments,
            header_lines=self.header
        )



