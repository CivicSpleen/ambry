"""The RowGenerator reads a file and yields rows, handling simple headers in CSV
files, and complex headers with receeding comments in Excel files.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

class Pipe(object):
    """A step in the pipeline"""

    _source_pipe = None
    _source = None

    partition = None # Set in the Pipeline
    segment = None # Set to the name of the segment
    pipeline = None  # Set to the name of the segment


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
        assert bool(self._source)
        return self

class Sink(Pipe):
    """A final stage pipe, which consumes its input and produces no output rows"""

    def __init__(self, count = 1000):
        self._count = count

    def run(self, count=None, *args, **kwargs):

        count = count if count else self._count

        for i, row in  enumerate(self._source_pipe):

            if count and i == count:
                break


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



class PrintRows(Pipe):
    """A Pipe that collects rows that pass through and displays them as a table when the pipeline is printed. """

    def __init__(self, count=10, columns=7, print_at=None):
        self.columns = columns
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
            self.i = i
            append_row = [i] + list(row)

            if i < self.count:
                self.rows.append(append_row[:self.columns])

            if i == self.print_at_row:
                print str(self)

            yield row

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

def make_partition_classifier(headers):
    """Create a function to extract the partition classification values from a row. These values are used to select a
    partition to write the row to """

    from ambry.identity import PartialPartitionName

    positions = []
    terms = []

    # Slow, but easy to understand ...
    for h_term, n_term in [  ('_p_'+term,term) for term, _, _ in  PartialPartitionName._name_parts ]:
        for i, header in headers:
            if h_term == header:
                positions.append(i)
                terms.append(n_term)

    code = ('f(row): name = PartialPartitionName(); {}; return name'
            .format(','.join('name.'+term for term in terms)+'='+','.join('row[{}]'.format() )))

    exec code
    return locals()['f']

class WriteToPartition(Pipe):
    """ """

    def __init__(self, partition):
        self.headers = None

        self.partition = partition
        self.inserter = self.partition.inserter()

    def __iter__(self):

        for i, row in enumerate(self.source_pipe):
            if i == 0:
                self.headers = row

            self.inserter.insert(row)

            yield row

    def __str__(self):
        return repr(self)

class WriteToSelectedPartition(Pipe):
    """Writes to one of several partitions, depending on the contents of columns that selects a partition"""

    def __init__(self):
        self.headers = None

        self.classifier = make_partition_classifier(self.headers)

        self.inserters = {}

    def __iter__(self):

        for i, row in enumerate(self.source_pipe):
            if i == 0:
                self.header = row
                yield row

            pname = self.classifier(row)

            inserter = self.inserter.get(pname, None)

            if inserter is None:
                p = self.bundle.partitions.new_partition(pname)
                inserter = p.inserter()
                self.inserters(pname, inserter)

            inserter.insert(row)

            yield row

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

    _groups_names = ['source',                  # The unadulterated dource file
                     'first',                   # For callers to hijack the start of the process
                     'source_row_intuit',       # (Meta only) Classify rows
                     'source_coalesce_rows',    # Combine rows into a header according to classification
                     'source_type_intuit',       # Classify the types of columns
                     'source_map_header',       # Alter column names to names used in final table
                     'dest_map_header',         # Change header names to be the same as used in the dest table
                     'dest_cast_columns',       # Run casters to convert values, maybe create code columns.
                     'dest_augment',            # Add dimension columns
                     'dest_statistics',         # Compute statistics
                     'last',                    # For callers to hijack the end of the process
                     'write_to_table'           # Write the rows to the table.
                     ]


    def __init__(self, bundle = None,  *args, **kwargs):

        super(Pipeline, self).__init__()

        super(Pipeline, self).__setattr__('bundle', bundle)

        for group_name in self._groups_names:

            gs = kwargs.get(group_name , [])
            if not isinstance(gs, (list, tuple)):
                gs = [gs]

            self[group_name] = PipelineSegment(self, group_name, *gs)

    @property
    def meta(self):
        """Return a copy with only the PipeSegments that apply to the meta phase"""

        exclude = [  'dest_map_header',  # Change header names to be the same as used in the dest table
                     'dest_cast_columns',  # Run casters to convert values, maybe create code columns.
                     'dest_augment',  # Add dimension columns
                     'dest_statistics',  # Compute statistics
                     'last',  # For callers to hijack the end of the process
                     'write_to_table'  # Write the rows to the table.
                     ]

        kwargs = {}
        pl = Pipeline(bundle = self.bundle)
        for group_name, pl_segment in self.items():
            if group_name in exclude:
                continue
            pl[group_name] = pl_segment

        return pl

    @property
    def build(self):
        """Return a copy with only the PipeSegments that apply to the build phase"""

        exclude = [ 'source_row_intuit',  'source_type_intuit'  ]

        kwargs = {}
        pl = Pipeline(bundle=self.bundle)
        for group_name, pl_segment in self.items():
            if group_name in exclude:
                continue
            pl[group_name] = pl_segment

        return pl

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

        for group_name in self._groups_names:
            for p in self[group_name]:
                chain.append(p)

        for p in chain[1:]:
            p.set_source_pipe(chain[0])

        last = reduce(lambda last, next: next.set_source_pipe(last), chain[1:], chain[0])

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
        for segment_name in self._groups_names:

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


