"""The RowGenerator reads a file and yields rows, handling simple headers in CSV
files, and complex headers with receeding comments in Excel files.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

class PipelineError(Exception):
    pass

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
        self._source = source_pipe.source if source_pipe else None

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

class Sink(Pipe):
    """A final stage pipe, which consumes its input and produces no output rows"""

    def __init__(self, count = 1000):
        self._count = count

    def run(self, count=None, *args, **kwargs):

        count = count if count else self._count

        for i, row in  enumerate(self._source_pipe):

            if count and i == count:
                break

class Head(Pipe):
    """ Pass-throughg only the first N rows
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

class Edit(Pipe):
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
        self.count = count
        self.rows = []
        self.i = 0

        try:
            self.print_at_row = int(print_at)
            self.print_at_end = False
        except:
            self.print_at_row = None
            self.print_at_end = bool(print_at)


    def __iter__(self):

        for i, row in enumerate(self.source_pipe):
            orig_row = row[:]

            if i == 0:# Append thecolumn number to the row header
                row = [ '#'+str(j)+' '+c for j,c in enumerate(row )]

            self.i = i
            append_row = [i] + list(row)

            if i < self.count:

                self.rows.append(append_row[self.offset:self.columns])

            if i == self.print_at_row:
                print str(self)

            yield orig_row

        if self.print_at_end:
            print str(self)

    def __str__(self):
        from tabulate import tabulate
        from terminaltables import SingleTable

        # return  SingleTable([[ str(x) for x in row] for row in self.rows] ).table

        if self.rows:
            return 'print. {} rows total\n'.format(self.i) + tabulate(self.rows[1:],self.rows[0], tablefmt="pipe")

        else:
            return ''

class WriteToPartition(Pipe):
    """ """

    def __init__(self, **kwargs):

        self.kwargs = kwargs

    def init(self):
        from ..identity import PartialPartitionName

        if self.kwargs:
            pname = PartialPartitionName(**self.kwargs)
        else:
            pname = PartialPartitionName(table=self._source.dest_table_name)

        self.headers = None

        p = self.bundle.partitions.partition(pname)
        if not p:
            p = self.bundle.partitions.new_partition(pname)

        self.partition = p
        self.inserter = p.inserter()

    def process_header(self, row):
        self.init() # Delayed because the _soruce isn't set at init time.
        self.headers = row
        return row

    def process_body(self, row):

        self.inserter.insert(dict(zip(self.headers, row)))

        return row

    def finish(self):

        self.inserter.close()

    def __str__(self):
        return repr(self)

def make_partition_classifier(headers):
    """Create a function to extract the partition classification values from a row. These values are used to select a
    partition to write the row to """

    from ambry.identity import PartialPartitionName

    positions = []
    terms = []

    # Slow, but easy to understand ...
    for h_term, n_term in [  ('_p_'+term,term) for term, _, _ in  PartialPartitionName._name_parts ]:
        for i, header in enumerate(headers):
            if h_term == header:
                positions.append(i)
                terms.append(n_term)

    code = ('def f(row): name = PartialPartitionName(); {}; return name'
            .format(','.join('name.'+term for term in terms)+'='+','.join('row[{}]'.format(i) for i in positions )))

    exec code in locals()
    return locals()['f']

class WriteToSelectedPartition(Pipe):
    """Writes to one of several partitions, depending on the contents of columns that selects a partition"""

    def __init__(self):

        self.inserters = {}
        self.headers = None
        self.classifier = None

    def process_header(self, row):
        self.headers = row
        self.classifier = make_partition_classifier(row)
        return row

    def process_body(self, row):

        pname = self.classifier(row)

        inserter = self.inserters.get(str(pname), None)

        if inserter is None:
            p = self.bundle.partitions.partition(pname)
            if not p:
                p = self.bundle.partitions.new_partition(pname)
            inserter = p.inserter()
            self.inserters[str(pname)] =  inserter

        inserter.insert(dict(zip(self.headers, row)))

        return row

    def __str__(self):
        return repr(self)

class PipelineSegment(list):

    def __init__(self, pipeline, name, *args):
        list.__init__(self)

        self.pipeline = pipeline
        self.name = name

        for p in args:
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

        if inspect.isclass(x):
            x = x()

        if isinstance(x, Pipe):
            x.segment = self
            x.pipeline = self.pipeline

        super(PipelineSegment, self).insert(i, x)

    @property
    def source(self):
        return self[0].source

from collections import OrderedDict, Mapping
class Pipeline(OrderedDict):
    """Hold a defined collection of PipelineGroups, and when called, coalesce them into a single pipeline """

    bundle = None

    _source_groups =  [
                        'source',                   # The unadulterated source file
                        'source_first',             # For callers to hijack the start of the process
                        'source_row_intuit',        # Classify rows
                        'source_coalesce_rows',     # Combine rows into a header according to classification
                        'source_map_header',  # Alter column names to names used in final table
                        'source_type_intuit',       # Classify the types of columns
                        'source_last',
                        'write_source_schema'       # Create the source schema, one source table per source
                       ]

    _schema_groups = [
                        'source',
                        'schema_first',
                        'source_coalesce_rows',     # Combine rows into a header according to classification
                        'source_map_header',        # Alter column names to names used in final table
                        'dest_map_header',          # Change header names to be the same as used in the dest table
                        'dest_cast_columns',        # Run casters to convert values, maybe create code columns.
                        'dest_augment',             # Add dimension columns
                        'schema_last',
                        'write_dest_schema'         # Write the destinatino schema
                        ]

    _build_groups = [
                        'source',
                        'build_first',
                        'source_coalesce_rows',     # Combine rows into a header according to classification
                        'source_map_header',        # Alter column names to names used in final table
                        'dest_map_header',          # Change header names to be the same as used in the dest table
                        'dest_cast_columns',        # Run casters to convert values, maybe create code columns.
                        'dest_augment',             # Add dimension columns
                        'dest_statistics',          # Compute statistics
                        'build_last',               # For callers to hijack the end of the process
                        'write_to_table'            # Write the rows to the table.
                     ]

    _group_names = list(OrderedDict.fromkeys(_source_groups + _schema_groups + _build_groups)) # uniques, preserve order

    def __init__(self, bundle = None,  *args, **kwargs):

        super(Pipeline, self).__init__()

        super(Pipeline, self).__setattr__('bundle', bundle)

        for group_name in self._group_names:

            gs = kwargs.get(group_name , [])
            if not isinstance(gs, (list, tuple)):
                gs = [gs]

            self[group_name] = PipelineSegment(self, group_name, *gs)

    def _subset(self, subset):
        kwargs = {}
        pl = Pipeline(bundle=self.bundle)
        for group_name, pl_segment in self.items():
            if group_name not in subset:
                continue
            pl[group_name] = pl_segment

        return pl

    @property
    def source_phase(self):
        """Return a copy with only the PipeSegments that apply to the source phase"""

        return self._subset(self._source_groups)

    @property
    def schema_phase(self):
        """Return a copy with only the PipeSegments that apply to the schema phase"""

        return self._subset(self._schema_groups)

    @property
    def build_phase(self):
        """Return a copy with only the PipeSegments that apply to the build phase"""

        return self._subset(self._build_groups)


    @property
    def file_name(self):

        return self.source.source.name

    def __setitem__(self, k, v):

        # If the caller tries to set a pipeline segment with a pipe, translte
        # the call to an append on the segment.

        if isinstance(v, Pipe) or ( isinstance(v, type) and issubclass(v, Pipe)):
            self[k].append(v)
        else:
            if v is None or ( isinstance(v, list) and len(v) > 0 and v[0] is None ):
                super(Pipeline, self).__setitem__(k,PipelineSegment(self,k))
            else:
                super(Pipeline, self).__setitem__(k, v)

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
        if k.startswith('_OrderedDict__'):
            return super(Pipeline, self).__setattr__(k, v)

        self[k] = v

    def _collect(self):

        chain = []

        # This is supposed to be an OrderedDict, but it doesn't seem to want to retain the ordering, so we force
        # it on output.

        for group_name in self._group_names:
            for p in self[group_name]:
                chain.append(p)

        for p in chain[1:]:
            p.set_source_pipe(chain[0])

        last = reduce(lambda last, next: next.set_source_pipe(last), chain[1:], chain[0])

        for p in chain:
            p.bundle = self.bundle

        return chain, last

    def run(self, count=None):

        chain, last = self._collect()

        sink = Sink(count = count)
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
        for segment_name in self._group_names:

            for pipe in self[segment_name]:
                out.append(u"-- {} {} ".format(segment_name, unicode(pipe)))

        return '\n'.join(out)

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


