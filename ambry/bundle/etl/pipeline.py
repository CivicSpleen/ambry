"""The RowGenerator reads a file and yields rows, handling simple headers in CSV
files, and complex headers with receeding comments in Excel files.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

class Pipe(object):

    _source_pipe = None
    _source = None

    segment = None # Set to the name of the segment

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, source_pipe):
        raise NotImplemented("Use set_source_pipe instead")

    @property
    def source_pipe(self):
        return self._source_pipe

    def set_source_pipe(self, source_pipe):
        self._source_pipe = source_pipe
        self._source = source_pipe.source if source_pipe else None

        return self



class AddHeader(Pipe):
    """Adds a header to a row file that doesn't have one, by returning the header for the first row. """

    def __init__(self, header):
        self._header = header


    def __iter__(self):

        yield self._header

        for row in self._source_pipe:
            yield row

class MapHeader(Pipe):
    def __init__(self, header_map):
        self._header_map = header_map


    def __iter__(self):

        rg = iter(self._source_pipe)

        yield [ self._header_map.get(c,c) for c in rg.next() ]

        for row in rg:
            yield row

class MangleHeader(Pipe):
    """"Alter the header with a function"""


    def mangle_column_name(self, i, n):
        """
        Override this method to change the way that column names from the source are altered to
        become column names in the schema. This method is called from :py:meth:`mangle_header` for each column in the
        header, and :py:meth:`mangle_header` is called from the RowGenerator, so it will alter the row both when the
        schema is being generated and when data are being inserted into the partition.

        Implement it in your bundle class to change the how columsn are converted from the source into database-friendly
        names

        :param i: Column number
        :param n: Original column name
        :return: A new column name
        """
        from ambry.orm import Column

        if not n:
            return 'column{}'.format(i)

        mn = Column.mangle_name(str(n).strip())

        return mn

    def mangle_header(self, header):

        return [self.mangle_column_name(i, n) for i, n in enumerate(header)]

    def __iter__(self):

        itr = iter(self.source_pipe)

        self.orig_header = itr.next()

        yield(self.mangle_header(self.orig_header))

        while True:
            yield itr.next()

class MergeHeader(Pipe):
    """Strips out the header comments and combines multiple header lines"""

    footer = None
    data_start_line = 1
    data_end_line = None
    header_lines = [0]
    header_comment_lines = []
    header_mangler = None

    headers = None
    header_comments = None
    footers = None

    initialized = False

    def init(self):
        """Deferred initialization b/c the object con be constructed without a valid source"""
        from itertools import chain

        def maybe_int(v):
            try:
                return int(v)
            except ValueError:
                return None

        if not self.initialized:

            self.data_start_line = 1
            self.data_end_line = None
            self.header_lines = [0]

            if self.source.start_line:
                self.data_start_line = self.source.start_line
            if self.source.end_line:
                self.data_end_line = self.source.end_line
            if self.source.header_lines:
                self.header_lines = map(maybe_int, self.source.header_lines)
            if self.source.comment_lines:
                self.header_comment_lines = map(maybe_int, self.source.comment_lines)

            max_header_line  = max(chain(self.header_comment_lines, self.header_lines))

            if self.data_start_line <= max_header_line:
                self.data_start_line = max_header_line + 1

            if not self.header_comment_lines:
                min_header_line = min(chain(self.header_lines))
                if min_header_line:
                    self.header_comment_lines = range(0,min_header_line)

            self.headers = []
            self.header_comments = []
            self.footers = []

            self.initialized = True

    def coalesce_headers(self):
        self.init()

        if len(self.headers) > 1:

            # If there are gaps in the values in the first header line, extend them forward
            hl1 = []
            last = None
            for x in self.headers[0]:
                if not x:
                    x = last
                else:
                    last = x

                hl1.append(x)

            self.headers[0] = hl1

            header = [' '.join(col_val.strip() if col_val else '' for col_val in col_set)
                      for col_set in zip(*self.headers)]
            header = [h.strip() for h in header]

            return header

        elif len(self.headers) > 0:
            return self.headers[0]

        else:
            return []

    def __iter__(self):
        self.init()


        if len(self.header_lines) == 1 and self.header_lines[0] == 0:
            # This is the normal case, with the header on line 0, so skip all of the
            # checks

            # NOTE, were also skiping the check on the data end line, which may sometimes be wrong.

            for row in self._source_pipe:
                yield row

        else:

            max_header_line = max(self.header_lines)

            for i, row in enumerate(self._source_pipe):

                if i < self.data_start_line:
                    if i in self.header_lines:
                        self.headers.append([str(unicode(x).encode('ascii', 'ignore')) for x in row])

                    if i in self.header_comment_lines:
                        self.header_comments.append([str(unicode(x).encode('ascii', 'ignore')) for x in row])

                    if i == max_header_line:
                        yield self.coalesce_headers()

                elif not self.data_end_line or i <= self.data_end_line:

                     yield row

                elif self.data_end_line and i>= self.data_end_line:
                    self.footers.append(row)


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
                    print 'END DL', is_dl, i, row
                    self.data_end_line = i

        return dict(
            data_start_line=self.data_start_line,
            data_end_line=self.data_end_line,
            header_comment_lines=self.header_comments,
            header_lines=self.header
        )

class PipelineSegment(list):

    def __init__(self, name, *args):
        list.__init__(self, args)

        self.name = name

    def append(self, x):
        self.insert(len(self),x)

    def insert(self, i, x):
        import inspect

        if inspect.isclass(x):
            x = x()

        if hasattr(x, 'segment'):
            x.segment = self
        super(PipelineSegment, self).insert(i, x)


from collections import OrderedDict, Mapping
class Pipeline(OrderedDict):
    """Hold a defined collection of PipelineGroups, and when called, coalesce them into a single pipeline """

    _groups_names = ['source', 'line_process', 'create_rows', 'coalesce_rows', 'mangle_header', 'normalize',
                     'remap_to_table', 'cast_columns', 'statistics', 'write_to_table']

    def __init__(self, *args, **kwargs):

        super(Pipeline, self).__init__(*args, **kwargs)

        for group_name in self._groups_names:

            gs = kwargs.get(group_name , [])
            if not isinstance(gs, (list, tuple)):
                gs = [gs]

            self[group_name] =  PipelineSegment(group_name, *gs)

    def __setitem__(self, k, v):
        super(Pipeline, self).__setitem__(k, v)

    def __getattr__(self, k):
        if not (k.startswith('__') or k.startswith('_OrderedDict__')):
            return self[k]
        else:
            return super(Pipeline, self).__getattr__(k)

    def __setattr__(self, k, v):
        if k.startswith('_OrderedDict__'):
            return super(Pipeline, self).__setattr__(k, v)

        self[k] = v

    def __call__(self, *args, **kwargs):

        chain = []

        # This is supposed to be an OrderedDict, but it doesn't seem to want to retain the ordering, so we force
        # it on output.

        for group_name in self._groups_names:
            chain += self[group_name]

        chain = list(args) + chain

        return reduce(lambda last, next: next.set_source_pipe(last), chain[1:], chain[0])

    def __str__(self):

        out = []
        for segment_name in self._groups_names:
            for pipe in self[segment_name]:
                out.append("-- {} {} ".format(segment_name, str(pipe)))

        return '\n'.join(out)

def augment_pipeline(pl, head_pipe = None, tail_pipe = None):
    """
    Augment the pipeline by adding a new pipe section to each stage that has one or more pipes. Can be used for debugging

    :param pl:
    :param DebugPipe:
    :return:
    """

    for k, v in pl.items():
        if len(v) > 0:
            if head_pipe and k != 'source': # Can't put anything before the source.
                v.insert(0,head_pipe)

            if tail_pipe:
                v.append(tail_pipe)
