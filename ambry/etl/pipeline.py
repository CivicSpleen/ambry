"""The RowGenerator reads a file and yields rows, handling simple headers in CSV
files, and complex headers with receeding comments in Excel files.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
from collections import OrderedDict

class PipelineError(Exception):
    def __init__(self,  pipe, *args, **kwargs):
        super(PipelineError, self).__init__(*args, **kwargs)
        self.pipe = pipe

        def __str__(self):
            from ambry.util import qualified_class_name

            return """
======================================
Pipeline Exception: PipelineError
Message:         {message}
Pipeline:        {pipeline_name}
Pipe:            {pipe_class}
Source:          {source_name}
Segment Headers: {headers}
-------------------------------------
Pipeline:
{pipeline}
""".format(message=self.message, pipeline_name=self.pipe.pipeline.name, pipeline = str(self.pipe.pipeline),
           pipe_class=qualified_class_name(self.pipe), source_name=self.source.name, headers=self.pipe.headers)

class MissingHeaderError(PipelineError):
    def __init__(self, pipe, header, table, *args, **kwargs):
        super(MissingHeaderError, self).__init__( pipe, *args, **kwargs)

        self.header = header
        self.table = table

    def __str__(self):
        from ambry.util import qualified_class_name
        return """
======================================
Pipeline Exception: MissingHeaderError
Message:         {message}
Pipeline:        {pipeline_name}
Pipe:            {pipe_class}
Source:          {source_name}
Segment Headers: {headers}
Missing Header:  {header}
-------------------------------------
{table_columns}
-------------------------------------
Pipeline:
{pipeline}
""".format(message = self.message, pipeline_name=self.pipe.pipeline.name, pipeline = str(self.pipe.pipeline),
           pipe_class = qualified_class_name(self.pipe), source_name = self.pipe.source.name,
           headers =  self.pipe.headers, header = self.header, table_name = self.table.name, table_columns = str(self.table))

class Pipe(object):
    """A step in the pipeline"""

    _source_pipe = None
    _source = None

    bundle = None
    partition = None # Set in the Pipeline
    segment = None # Set to the name of the segment
    pipeline = None  # Set to the name of the segment
    headers = None

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, source_pipe):
        raise NotImplemented("Use set_source_pipe instead")

    @property
    def source_pipe(self):
        assert bool(self._source_pipe)
        return self._source_pipe

    def set_source_pipe(self, source_pipe):
        self._source_pipe = source_pipe
        self._source = source_pipe.source if source_pipe and hasattr(source_pipe, 'source') else None

        return self

    def process_header(self, row):
        """Called to process the first row, the header. Must return the header,
        possibly modified. The returned header will be sent upstream"""
        self.headers = row
        return row

    def process_body(self, row):
        """Called to process each row in the body. Must return a row to be sent upstream"""
        return row

    def finish(self):
        """Called after the last row has been processed"""
        pass

    def __iter__(self):

        rg = iter(self._source_pipe)

        yield self.process_header(rg.next())

        for row in rg:
            row =  self.process_body(row)
            if row:
                yield row

        self.finish()

    def log(self, m):

        if self.bundle:
            self.bundle.logger.info(m)

    def error(self, m):

        if self.bundle:
            self.bundle.logger.error(m)

    def print_header(self):
        from ..util import qualified_class_name

        return qualified_class_name(self)

    def __str__(self):
        return self.print_header()

class Sink(Pipe):
    """A final stage pipe, which consumes its input and produces no output rows"""

    def __init__(self, count = None):
        self._count = count

    def run(self, count=None, *args, **kwargs):

        count = count if count else self._count

        for i, row in  enumerate(self._source_pipe):

            if count and i == count:
                break

class IterSource(Pipe):
    """Creates a source from an Iterator"""

    def __init__(self, iterator, header = None):
        self.iterator = iterator
        self.header = header

    def __iter__(self):

        itr = iter(self.iterator)

        if self.header:
            yield self.header
        else:
            # Create a header from the datatypes
            first = itr.next()
            yield [ type(e).__name__ for e in first]
            yield first

        for row in itr:
            yield row

class OnlySource(Pipe):
    """Only allow iteration on a named source. """

    def __init__(self, source_name):

        self.source_name = source_name

    def process_header(self, row):

        if self.source.name != self.source_name:
            raise StopIteration

        self.headers = row
        return row

class Head(Pipe):
    """ Pass-through only the first N rows
    """

    def __init__(self, count = 20):

        self.count = count

    def process_body(self, row):

        if self.count == 0:
            raise StopIteration

        self.count -= 1
        return row

class Sample(Pipe):
    """ Take a sample of rows, skipping rows exponentially to end at the est_length imput row, with
    count output rows.
    """

    def __init__(self, count = 20, skip = 5, est_length = 10000):

        from math import log, exp
        self.skip = float(skip)
        self.skip_factor = exp(log(est_length/self.skip)/(count-1))
        self.count = count
        self.i = 0

    def process_body(self, row):

        if self.count == 0:
            raise StopIteration

        if self.i % int(self.skip) == 0:
            self.count -= 1
            self.skip = self.skip * self.skip_factor

        else:
            row = None

        self.i += 1
        return row

class Ticker(Pipe):
    """ Ticks out 'H' and 'B' for header and rows.
    """

    def __init__(self, name = None):
        self._name = name

    def process_body(self, row):
        print self._name if self._name else 'B'
        return row

    def process_header(self, row):
        print '== {} {} =='.format(self.source.name, self._name if self._name else '')
        self.headers = row
        return row

class AddHeader(Pipe):
    """Adds a header to a row file that doesn't have one, by returning the header for the first row. """

    def __init__(self, header):
        self._header = header

    def __iter__(self):

        yield self._header

        for row in self._source_pipe:
            yield row

class MapHeader(Pipe):
    """Alter the header using a map"""
    def __init__(self, header_map):
        self._header_map = header_map

    def __iter__(self):

        rg = iter(self._source_pipe)

        self.headers =  [ self._header_map.get(c,c) for c in rg.next() ]

        yield self.headers

        for row in rg:
            yield row

class MapToSourceTable(Pipe):
    """Alter the header using the source_header and dest_header in the source table """
    def __init__(self, error_on_fail = True):

        self.error_on_fail = error_on_fail

    def process_header(self, row):

        m = { c.source_header:c.dest_header for c in self.source.source_table.columns }

        if self.error_on_fail:
            try:
                self.headers =  list([m[h] for h in row])
            except KeyError:
                for h in row:
                    if not h in m:
                        # pipe, header, table,
                        raise MissingHeaderError( self, h, self.source.source_table,
                                                  "Failed to find header in source_table ")

        else:
            self.headers = list([m.get(h, h) for h in row])

        return self.headers

class MangleHeader(Pipe):
    """"Alter the header so the values are well-formed, converting to alpjhanumerics and undersscores"""


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

        headers = itr.next()

        self.headers = self.mangle_header(headers)
        yield self.headers

        while True:
            yield itr.next()

class MergeHeader(Pipe):
    """Strips out the header comments and combines multiple header lines to emit a
    single header line"""

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
            self.i = 0

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

            for row in self._source_pipe:

                if self.i < self.data_start_line:
                    if self.i in self.header_lines:
                        self.headers.append([str(unicode(x).encode('ascii', 'ignore')) for x in row])

                    if self.i in self.header_comment_lines:
                        self.header_comments.append([str(unicode(x).encode('ascii', 'ignore')) for x in row])

                    if self.i == max_header_line:

                        yield self.coalesce_headers()

                elif not self.data_end_line or self.i <= self.data_end_line:
                     yield row

                elif self.data_end_line and self.i >= self.data_end_line:
                    self.footers.append(row)

                self.i += 1

    def __str__(self):
        from ..util import qualified_class_name

        return qualified_class_name(self) + ': header = {} '.format(','.join(str(e) for e in self.header_lines))

class AddDeleteExpand(Pipe):
    """Edit rows as they pass through """

    def __init__(self, add=[], delete=[], edit={}, expand={}):
        """

        :param add: List of blank columns to add, by header name, or dict of headers and functions to create the column value
        :param delete: List of headers names of columns to delete
        :param edit: Dict of header names and functions to alter the value.
        :return:
        """

        self.add = add
        self.delete = delete
        self.edit = edit
        self.expand = expand

        if isinstance(self.add, (list, tuple)):
            # Convert the list of headers into a sets of functins that
            # just produce None
            from collections import OrderedDict
            self.add = OrderedDict( (k,lambda e,r: None)  for k in self.add)

        self.edit_header = None
        self.edit_row = None
        self.edit_functions = None  # Turn dict lookup into list lookup

    def process_header(self, row):

        self.edit_functions = [None] * len(row)

        header_parts = []
        row_parts = []
        for i, h in enumerate(row):

            if h in self.delete:
                pass
            elif h in self.edit:
                self.edit_functions[i] = self.edit[h]
                row_parts.append('self.edit_functions[{i}](self,r[{i}])'.format(i=i))
                header_parts.append('r[{}]'.format(i))
            else:
                row_parts.append('r[{}]'.format(i))
                header_parts.append('r[{}]'.format(i))

        for f in self.add.values():
            self.edit_functions.append(f)
            i = len(self.edit_functions)-1
            assert self.edit_functions[i] == f
            row_parts.append('self.edit_functions[{i}](self,r)'.format(i=i))

        # The expansions get tacked onto the end, after the adds.
        header_expansions = []
        row_expanders = []  # The outputs of the expanders are combined, outputs must have same length as header_expansions
        self.expand_row = lambda e: []  # Null output

        for k, f in self.expand.items():
            self.edit_functions.append(f)
            i = len(self.edit_functions) - 1
            assert self.edit_functions[i] == f
            header_expansions += list(k)  # k must be a list or tauple or other iterable.
            row_expanders.append('self.edit_functions[{i}](self,r)'.format(i=i))

        if header_expansions:
            self.expand_row = eval("lambda r,self=self: ({})".format('+'.join(row_expanders)))

        # Maybe lookups in tuples is faster than in lists.
        self.edit_functions = tuple(self.edit_functions)

        header_extra = ["'{}'".format(e) for e in (self.add.keys()+header_expansions) ]

        # Build the single function to edit the header or row all at once
        self.edit_header = eval("lambda r: [{}]".format(',\n'.join(header_parts + header_extra)))
        self.edit_row = eval("lambda r,self=self: [{}]".format(',\n'.join(row_parts )))

        # Run it!
        self.header =  self.edit_header(row)
        return self.header

    def process_body(self, row):

        return self.edit_row(row)+self.expand_row(row)

class Add(AddDeleteExpand):

    def __init__(self, add):
        super(Add, self).__init__(add=add)

class Expand(AddDeleteExpand):

    def __init__(self,  expand):
        super(Expand, self).__init__(expand=expand)

class Delete(AddDeleteExpand):
    def __init__(self, delete):
        super(Delete, self).__init__(delete=delete)

class Edit(AddDeleteExpand):
    def __init__(self,  edit):
        super(Edit, self).__init__(edit=edit)

class RemoveBlankColumns(Pipe):
    """Remove columns that don't have a header"""

    def __init__(self):
        self.editor = None

    def process_header(self, row):

        header_parts = []
        for i, h in enumerate(row):
            if h.strip():
                header_parts.append('r[{}]'.format(i))

        if header_parts:
            self.editor = eval("lambda r: [{}]".format(','.join(header_parts)))
            self.headers = self.editor(row)
            return self.headers
        else:
            self.process_body = lambda self,row: row
            return row

    def process_body(self, row):
        return self.editor(row)

class Skip(Pipe):
    """Skip rows of a table that match a predicate """

    def __init__(self, pred, table = None, use_dict = True):
        """

        :param add: List of blank columns to add, by header name, or dict of headers and functions to create the column value
        :param delete: List of headers names of columns to delete
        :param edit: Dict of header names and functions to alter the value.
        :return:
        """

        self.pred = pred

        try:
            self.table = table.name
        except AttributeError:
            self.table = table

        self._use_dict = use_dict
        self._check = False

        self.skipped = 0
        self.passed = 0
        self.ignored = 0

    def process_header(self, row):

        # If there is no table specified, always run the predicate, but if the table
        # is specified, only run the predicate for that table.
        if self.table is None:
            self._check = True
        else:
            self._check = self.table == self.source.dest_table.name

        self.headers = row

        return row

    def __str__(self):

        return 'Skip. {} skipped, {} passed, {} ignored'.format(self.skipped, self.passed, self.ignored)

    def process_body(self, row):

        if not self._check:
            self.ignored += 1
            return row
        elif self.pred(dict(zip(self.headers,row)) if self._use_dict else row):
            self.skipped += 1
            return None
        else:
            self.passed += 1
            return row

class LogRate(Pipe):

    def __init__(self, output_f, N, message = None):
        from ambry.util import init_log_rate
        self.lr = init_log_rate(output_f,N, message)

    def process_body(self, row):
        self.lr()
        return row

class PrintRows(Pipe):
    """A Pipe that collects rows that pass through and displays them as a table when the pipeline is printed. """

    def __init__(self, count=10, columns=None, offset = None, print_at=None):
        self.columns = columns
        self.offset = offset
        self.count_inc = count
        self.count = count
        self.rows = []
        self.i = 1

        try:
            self.print_at_row = int(print_at)
            self.print_at_end = False

        except:
            self.print_at_row = None
            self.print_at_end = bool(print_at)

    def process_body(self, row):
        orig_row = list(row)

        if self.i < self.count:

            append_row = [self.i] + list(row)

            self.rows.append(append_row[self.offset:self.columns])

        if self.i == self.print_at_row:
            print str(self)


        self.i += 1

        return  orig_row

    def finish(self):

        if self.print_at_end:
            print str(self)

        # For multi-run pipes, the count is the number of rows per source.
        self.count += self.count_inc

    def process_header(self, row):
        self.headers = row

        return row

    def __str__(self):
        from ..util import qualified_class_name
        from tabulate import tabulate

        if self.rows:
            aug_header = ['0'] + ['#' + str(j) + ' ' + str(c) for j, c in enumerate(self.headers)]
            return (qualified_class_name(self)+
                    ' {} rows total\n'.format(self.i) +
                    tabulate(self.rows,aug_header[self.offset:self.columns] ,tablefmt="pipe") )
        else:
            return (qualified_class_name(self)+' 0 rows')

class PrintEvery(Pipe):
    """Print a row every N rows. Always prints the header. """

    def __init__(self, N=1):
        self.N = N
        self.i = 0

    def process_header(self, row):
        print 'Print Header: ',row
        self.headers = row
        return row

    def process_body(self, row):
        if self.i % self.N == 0:
            print 'Print Row   :',row
        self.i +=1
        return row

class MatchPredicate(Pipe):
    """Store rows that match a predicate. THe predicate is a function that takes the row as its
    sole parameter and returns true or false"""

    def __init__(self, pred):
        self._pred = pred
        self.i = 0
        self.matches = []

    def process_body(self, row):

        if self._pred(row):
            self.matches.append(row)

        return row

class Reduce(Pipe):
    """Like works like reduce() on the body rows, using the function f(accumulator,row) """

    def __init__(self, f, initializer=None):
        self._f = f
        self._initializer = initializer
        self.accumulator = None

    def __iter__(self):

        it = iter(self._source_pipe)

        # Yield the header
        self.headers =  next(it)
        yield self.headers

        if self._initializer is None:
            try:
                self.accumulator = self._f(None, next(it))
            except StopIteration:
                raise TypeError('reduce() of empty sequence with no initial value')
        else:
            self.accumulator = self._initializer

        for row in it:
            self.accumulator = self._f(self.accumulator, row)
            yield row

def make_table_map(table, headers):
    """"Create a function to map from rows with the structure of the headers to the structure of the table. """

    header_parts = {}
    for i,h in enumerate(headers):
        header_parts[h] = 'row[{}]'.format(i)

    body_code = 'lambda row: [{}]'.format(','.join(header_parts.get(c.name,'None') for c in table.columns ))
    header_code = 'lambda row: [{}]'.format(','.join(header_parts.get(c.name, "'{}'".format(c.name)) for c in table.columns))

    return eval(header_code), eval(body_code)

class SelectPartition(Pipe):
    """A Base class for adding a _pname column, which is used by the partition writer to select which
    partition a row is written to. By default, uses a partition name that consists of only the
     destination table of the source and a segment of the id of the source"""

    def __init__(self, select_f=None, as_dict=False):

        self._default = None
        self._as_dict = as_dict

        # Under the theory that removing an if is faster.
        if select_f:
            self.select_f = select_f
            self.process_body = self.process_body_select
        else:
            self.process_body = self.process_body_default


    def process_header(self, row):
        from ..identity import PartialPartitionName
        self._default = PartialPartitionName(table = self.source.dest_table_name, segment=self.source.id)
        self.headers = row + ['_pname']
        self._orig_headers = row
        return self.headers

    def process_body(self, row):
        """This method gets replaced by process_body_select() or process_body_default()"""
        raise NotImplemented("This function should be patched into nonexistence")

    def process_body_select(self, row):
        from ambry.identity import PartialPartitionName

        if self._as_dict:
            name = self.select_f(self.source, dict(zip(self.headers,row)))
        else:
            name = self.select_f(self.source, row)

        # Name must be a dict
        if not isinstance(name, PartialPartitionName):
            name = PartialPartitionName(**name)

        if not name.segment:
            name.segment = self.source.id

        if not name.table:
            name.table = self.source.dest_table_name

        return list(row) + [name]

    def process_body_default(self, row):
        return list(row) + [self._default]

class PartitionWriter(object):
    """Marker class so the partitions can be retrieved after the pipeline finishes
    """

class WriteToPartition(Pipe, PartitionWriter):
    """Writes to one of several partitions, depending on the contents of columns that selects a partition"""

    def __init__(self):
        """

        :param select_f: A function which takes  source and a row and returns a PartialPartitionName
        :return:
        """

        # The partitions are stored in both the data files and the partitions dicts, because
        # the _datafiles may have multiple copies of the same partition, and they all have to
        # be the same instance.
        self._datafiles = {} # Partitions associated with table mappers
        self._partitions = {} # Just the partitions.
        self._headers = {}
        self.headers = None
        self.p_name_index = None

        self.header_mapper, self.body_mapper = None, None

        self._start_time = None
        self._end_time = None
        self._count = 0

    def process_header(self, row):
        import time

        self.headers = row # Can't write until the first row tells us what the partition is

        if not '_pname' in row:
            raise PipelineError("Did not get a _pname header. The pipeline must insert a _pname value"
                                " to write to partitions ")

        self.p_name_index = row.index('_pname')

        self._headers[self.source.name] = row

        self._source_id = self.source.id

        self._start_time = time.time()

        return row

    def process_body(self, row):

        self._count += 1

        pname = row[self.p_name_index]

        if not pname.segment:
            pname.segment = self._source_id

        df_key = (self.source.name, pname)

        try:
            (p,  header_mapper, body_mapper) = self._datafiles[df_key]

        except KeyError:

            try:
                p  = self._partitions[pname]
            except KeyError:
                p = self.bundle.partitions.partition(pname)
                if not p:
                    from ..orm.partition import Partition

                    p = self.bundle.partitions.new_partition(pname, type=Partition.TYPE.SEGMENT )
                    p.clean()

                self._partitions[pname] = p

            header_mapper, body_mapper = make_table_map(p.table, self._headers[self.source.name])

            self._datafiles[df_key] = (p, header_mapper, body_mapper)

            p.datafile.insert_header(header_mapper(self.headers))

        p.datafile.insert_body(body_mapper(row))

        return row

    def finish(self):
        import time

        self._end_time = time.time()

        for key, (p, header_mapper, body_mapper) in self._datafiles.items():
            p.datafile.close()

    @property
    def rate(self):
        """Report the insertion rate in records per second"""
        import time

        end = self._end_time if self._end_time else time.time()

        return self._count / ( end  - self._start_time )

    @property
    def partitions(self):
        """Generate the partitions, so they can be manipulated after the pipeline completes"""
        for p in self._partitions.values():
            yield p

    def __str__(self):
        from ..util import qualified_class_name
        out = ""

        for p in self.partitions:
            out += str(p.identity.name) + "\n"

        return   qualified_class_name(self)  + "\n" + out

class PipelineSegment(list):

    def __init__(self, pipeline, name, *args):
        list.__init__(self)

        self.pipeline = pipeline
        self.name = name

        for p in args:
            assert not isinstance(p, (list, tuple))
            self.append(p)

    def __getitem__(self, k):

        import inspect

        # Index by class
        if inspect.isclass(k):

            matches = filter(lambda e: isinstance(e, k), self)

            if not matches:
                raise IndexError("No entry for class: {}".format(k))

            k = self.index(matches[0]) # Only return first index

        return super(PipelineSegment, self).__getitem__(k)

    def append(self, x):
        self.insert(len(self),x)
        return self

    def prepend(self, x):
        self.insert(0, x)
        return self

    def insert(self, i, x):
        import inspect

        assert not isinstance(x, (list, tuple))

        if inspect.isclass(x):
            x = x()

        if isinstance(x, Pipe):
            x.segment = self
            x.pipeline = self.pipeline

        assert not inspect.isclass(x)

        super(PipelineSegment, self).insert(i, x)

    @property
    def source(self):
        return self[0].source

class Pipeline(OrderedDict):
    """Hold a defined collection of PipelineGroups, and when called, coalesce them into a single pipeline """

    bundle = None
    name = None
    phase = None
    final = None

    _group_names = ['source', 'first', 'body', 'augment', 'last', 'store', 'final' ]

    def __init__(self, bundle = None,  *args, **kwargs):

        super(Pipeline, self).__init__()

        super(Pipeline, self).__setattr__('bundle', bundle)
        super(Pipeline, self).__setattr__('name', None)
        super(Pipeline, self).__setattr__('phase', None)
        super(Pipeline, self).__setattr__('final', [])

        for k, v in kwargs.items():
            if k not in self._group_names:
                raise IndexError("{} is not a valid pipeline section name".format(k))

        for group_name in self._group_names:
            gs = kwargs.get(group_name , [])
            if not isinstance(gs, (list, tuple)):
                gs = [gs]

            self.__setitem__(group_name, PipelineSegment(self, group_name, *gs))

        if args:
            self.__setitem__('body', PipelineSegment(self, 'body', *args))

    def _subset(self, subset):
        """Return a new pipeline with a subset of the sections"""
        kwargs = {}
        pl = Pipeline(bundle=self.bundle)
        for group_name, pl_segment in self.items():
            if group_name not in subset:
                continue
            pl[group_name] = pl_segment

        return pl

    def configure(self, pipe_config):
        """Configure from a dict"""

        # Create a context for evaluating the code for each pipeline. This removes the need
        # to qualify the class names with the module
        import ambry.etl
        eval_locals = dict(locals().items()  + ambry.etl.__dict__.items())

        replacements = {}

        def eval_pipe(pipe):
            if isinstance(pipe, basestring):
                try:
                    return eval(pipe, {}, eval_locals)
                except SyntaxError as e:
                    raise SyntaxError("SyntaxError while parsing pipe '{}' from metadata: {}"
                                      .format(pipe, e))
            else:
                return pipe

        for segment_name, pipes in pipe_config.items():
            if segment_name == 'final':
                # The 'final' segment is actually a list of names of BUndle methods to call afer the pipeline
                # completes
                super(Pipeline, self).__setattr__('final', pipes)
            elif segment_name == 'replace':
                for frm, to in pipes.items():
                    self.replace(eval_pipe(frm), eval_pipe(to))
            else:
                self[segment_name] = [eval_pipe(pipe) for pipe in pipes ]



    def replace(self,repl_class, replacement):
        """Replace a pipe segment, specified by its class, with another segment"""

        for segment_name, pipes in self.items():
            repl_pipes = []
            found = False
            for pipe in pipes:
                if isinstance(pipe, repl_class):
                    pipe = replacement
                    found = True

                repl_pipes.append(pipe)

            if found:
                found = False
                self[segment_name] = repl_pipes

    @property
    def file_name(self):

        return self.source.source.name

    def __setitem__(self, k, v):

        # If the caller tries to set a pipeline segment with a pipe, translte
        # the call to an append on the segment.

        if isinstance(v, (list, tuple)):
            v = list(filter(bool, v))

        empty_ps = PipelineSegment(self, k)

        if isinstance(v, Pipe) or ( isinstance(v, type) and issubclass(v, Pipe)):
            # Assignment from a pipe is appending
            self[k].append(v)
        elif v is None:
            # Assignment from None
            super(Pipeline, self).__setitem__(k, empty_ps)
        elif isinstance(v, (list, tuple) ) and not v  :
            # Assignment from empty list
            super(Pipeline, self).__setitem__(k, empty_ps)
        elif isinstance(v, PipelineSegment):
            super(Pipeline, self).__setitem__(k, v)
        elif isinstance(v, (list, tuple) ):
            # Assignment from a list
            super(Pipeline, self).__setitem__(k, PipelineSegment(self, k, *v))
        else:
            # This maybe should be an error?
            super(Pipeline, self).__setitem__(k, v)

        assert isinstance(self[k], PipelineSegment), "Unexpected type: {}".format(type(self[k]))

    def __getitem__(self, k):

        import inspect

        # Index by class. Looks through all of the segments for the first pipe with the given class
        if inspect.isclass(k):

            chain, last = self._collect()

            matches = filter(lambda e: isinstance(e, k), chain)

            if not matches:
                raise IndexError("No entry for class: {} in {}".format(k, chain))

            return matches[0]
        else:
            return super(Pipeline, self).__getitem__(k)

    def __getattr__(self, k):
        if not (k.startswith('__') or k.startswith('_OrderedDict__')):
            return self[k]
        else:
            return super(Pipeline, self).__getattr__(k)

    def __setattr__(self, k, v):
        if k.startswith('_OrderedDict__') or k in ('name', 'phase'):
            return super(Pipeline, self).__setattr__(k, v)

        self.__setitem__(k,v)

    def _collect(self):
        import inspect

        chain = []

        # This is supposed to be an OrderedDict, but it doesn't seem to want to retain the ordering, so we force
        # it on output.

        for group_name in self._group_names:

            assert isinstance(self[group_name], PipelineSegment)

            for p in self[group_name]:
                chain.append(p)

        if len(chain):
            last = chain[0]
            for p in chain[1:]:
                assert not inspect.isclass(p)
                p.set_source_pipe(last)
                last = p
        else:
            last = None

        for p in chain:
            p.bundle = self.bundle

        return chain, last

    def run(self, count=None, source_pipes = None):

        if source_pipes:
            for source_pipe in source_pipes:

                if self.bundle:
                    self.bundle.logger.info("Running source {} in a multi-source run".format(source_pipe.source.name))

                self['source'] = [source_pipe] # Setting as a scalar appends, as a list will replace.

                chain, last = self._collect()

                sink = Sink(count=count)
                sink.set_source_pipe(last)

                sink.run()

        else:
            chain, last = self._collect()

            sink = Sink(count=count)
            sink.set_source_pipe(last)

            sink.run()

        return self

    def iter(self):

        chain, last = self._collect()

        # Iterate over the last pipe, which will pull from all those before it.
        for row in last:
            yield row



    def __str__(self):

        out = []
        chain, last = self._collect()

        for pipe in chain:
            out.append((pipe.segment.name if hasattr(pipe,'segment') else '?')+': '+unicode(pipe))

        out.append('final: '+str(self.final))

        return 'Pipeline {}\n'.format(self.name if self.name else '')+'\n'.join(out)

    def headers_report(self):
        from ambry.util import qualified_class_name
        import tabulate

        out = []
        chain, last = self._collect()
        for pipe in chain:

            if not hasattr(pipe,'headers') or not pipe.headers:
                continue

            try:
                seg_name = pipe.segment.name if hasattr(pipe,'segment') else '?'
                v = [seg_name, qualified_class_name(pipe)]+[ str(e) for e in pipe.headers if e]
                out.append(v)

            except AttributeError:
                pass

        if not out:
            return None

        # Make all lines the same length
        ll = max( len(e) for e in out)
        for i in range(len(out)):
            if len(out[i])< ll:
                out[i] += ['']*(ll-len(out[i]))


        return tabulate.tabulate(out)


def augment_pipeline(pl, head_pipe = None, tail_pipe = None):
    """
    Augment the pipeline by adding a new pipe section to each stage that has one or more pipes. Can be used for debugging

    :param pl:
    :param DebugPipe:
    :return:
    """

    for k, v in pl.items():
        if v and len(v) > 0:
            if head_pipe and k != 'source': # Can't put anything before the source.
                v.insert(0,head_pipe)

            if tail_pipe:
                v.append(tail_pipe)




