""" Bundle variants that directly load files with little additional processing.
"""


from  ambry.bundle import BuildBundle

class LoaderBundle(BuildBundle):
    prefix_headers = ['id']

    def mangle_column_name(self, n):
        """
        Override this method to change the way that column names from the source are altered to
        become column names
        :param n:
        :return:
        """

        return n

    @staticmethod
    def int_caster(v):
        """Remove commas from numbers and cast to int"""

        try:
            return int(v.replace(',', ''))
        except AttributeError:
            return v

    @staticmethod
    def real_caster(v):
        """Remove commas from numbers and cast to float"""

        try:
            return float(v.replace(',', ''))
        except AttributeError:
            return v

class CsvBundle(LoaderBundle):
    """A Bundle variant for loading CSV files"""

    delimiter = None


    def get_csv_reader(self, f, as_dict = False, sniff = False):
        import csv

        if sniff:
            dialect = csv.Sniffer().sniff(f.read(5000))
            f.seek(0)
        else:
            dialect = None

        delimiter= self.delimiter if self.delimiter else ','

        if as_dict:
            return csv.DictReader(f, delimiter = delimiter, dialect=dialect)
        else:
            return csv.reader(f, delimiter = delimiter, dialect=dialect)

    def get_source(self, source):
        """Get the source file. If the file does not end in a CSV file, replace it with a CSV extension
        and look in the source store cache """
        import os

        if not source:
            source = self.metadata.sources.keys()[0]

        fn = self.filesystem.download(source)

        if fn.endswith('.zip'):
            fn = self.filesystem.unzip(fn)

        # If the file we are given isn't actually a CSV file, we might have manually
        # converted it to a CSV and put it in the source store.
        if not fn.lower().endswith('.csv'):
            cache = self.filesystem.source_store

            if cache:
                bare_fn, ext = os.path.splitext(os.path.basename(fn))

                fn_ck = self.source_store_cache_key(bare_fn+".csv")

                if cache.has(fn_ck):
                    if not self.filesystem.download_cache.has(fn_ck):
                        with cache.get_stream(fn_ck) as s:

                            self.filesystem.download_cache.put(s, fn_ck )

                    return self.filesystem.download_cache.path(fn_ck)

        return fn

    def gen_rows(self, source, as_dict=False, prefix_headers = None):
        """
        Generate rows for a source file. The source value ust be specified in the sources config

        :param source:
        :param as_dict:
        :param prefix_headers: Number of None entries to put i nthe first of the row. For leaving space
        for columns that are added to the schema that are not in the original dataset.
        :return:
        """
        import csv

        if prefix_headers is None:
            prefix_headers = self.prefix_headers

        fn = self.get_source(source)

        self.log("Generating rows from {}".format(fn))

        with open(fn) as f:

            r = self.get_csv_reader(f, as_dict=as_dict)

            if as_dict:

                for row in r:
                    row['id'] = None
                    yield row
            else:
                # It might seem inefficient to return the header every time, but it really adds only a
                # fraction of a section for millions of rows.

                header = prefix_headers + r.next()

                header = [ x if x else "column{}".format(i) for i, x in enumerate(header)]

                for i, row in enumerate(r):

                    yield header, [None]*len(prefix_headers) + row

                    if self.run_args.test and i > 5000:
                        break


    def make_table_for_source(self, source_name):

        source = self.metadata.sources[source_name]

        table_name = source.table if source.table else source_name

        table_desc = source.description if source.description else "Table generated from {}".format(source.url)

        data = dict(source)
        del data['description']
        del data['url']

        table = self.schema.add_table(table_name, description=table_desc, data=data)

        return table

    def mangle_header(self, header):
        """Call mangle_column_name on each item in the header to produce a final header"""
        from collections import OrderedDict

        if isinstance(header, OrderedDict):
            return OrderedDict([(self.mangle_column_name(x), v) for x,v in header.items() if x])
        else:
            return OrderedDict([ (self.mangle_column_name(x),x) for x in header if x] )


    def meta(self):

        self.database.create()

        if not self.run_args.get('clean', None):
            self._prepare_load_schema()

        with self.session:
            for source_name, source in self.metadata.sources.items():

                if source.is_loadable is False:
                    continue

                table = self.make_table_for_source(source_name)

                header, row = self.gen_rows(source_name, as_dict=False).next()

                self.schema.update_from_iterator(table.name,
                                   header = self.mangle_header(header),
                                   iterator=self.gen_rows(source_name, as_dict=False),
                                   max_n=1000,
                                   logger=self.init_log_rate(500))

        return True

    def build_create_partition(self, source_name):
        """Create or find a partition based on the source"""
        # This ugliness is b/c get() doesn't take a 'default' arg.

        source = self.metadata.sources[source_name]

        try:
            table = source['table']

            if not table:
                table = source_name

        except:
            table = source_name

        assert bool(table)

        return self.partitions.find_or_new(table=table)


    def build(self):

        for source_name, source in self.metadata.sources.items():

            if source.is_loadable is False:
                continue


            p = self.build_create_partition(source_name)

            self.log("Loading source '{}' into partition '{}'".format(source_name, p.identity.name))

            lr = self.init_log_rate(print_rate = 5)

            columns = [c.name for c in p.table.columns ]
            header = [c.name for c in p.table.columns]


            mod_row = getattr(self, 'build_modify_row', False)

            with p.inserter() as ins:
                for _, row in self.gen_rows(source_name):
                    lr(str(p.identity.name))

                    d = dict(zip(header, row))

                    if mod_row:
                        mod_row(p, source, d)

                    ins.insert(d)

        return True

class ExcelBuildBundle(CsvBundle):

    workbook = None # So the derived classes can et to the workbook, esp for converting dates

    decode = False # Set to an encoding from which to decode all strings.

    def __init__(self, bundle_dir=None):
        '''
        '''

        super(ExcelBuildBundle, self).__init__(bundle_dir)

        if not self.metadata.build.requirements or not 'xlrd' in self.metadata.build.requirements:
            self.metadata.load_all()
            self.metadata.build.requirements.xlrd = 'xlrd'
            self.update_configuration()

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

    def get_wb_sheet(self, source, segment = None):

        if not source:
            source = self.metadata.sources.keys()[0]

        if segment:
            sheet_num = segment
        else:
            sheet_num = self.metadata.sources.get(source).segment
            sheet_num = 0 if not sheet_num else sheet_num

        return self.get_source(source), sheet_num

    def source_header(self, source, segment = None):
        from xlrd import open_workbook

        fn, sheet_num = self.get_wb_sheet(source, segment)

        with open(fn) as f:
            wb = open_workbook(fn)

            s = wb.sheets()[sheet_num]

            return self.srow_to_list(0, s)

    def gen_rows(self, source=None, as_dict=False, segment = None,   prefix_headers = None ):
        """Generate rows for a source file. The source value ust be specified in the sources config"""
        from xlrd import open_workbook

        if prefix_headers is None:
            prefix_headers = self.prefix_headers

        fn, sheet_num = self.get_wb_sheet(source, segment)

        self.log("Generate rows for: {}, sheet = {}".format(fn, sheet_num))

        header = self.source_header(source, segment) # if as_dict else None

        header = prefix_headers + header

        with open(fn) as f:

            wb = open_workbook(fn)

            self.workbook = wb

            s = wb.sheets()[sheet_num]

            for i in range(1,s.nrows):

                row = self.srow_to_list(i, s)

                if as_dict:
                    yield dict(zip(header, [None]*len(prefix_headers)  + row))
                else:
                    # It might seem inefficient to return the header every time, but it really adds only a
                    # fraction of a section for millions of rows.
                    yield header, [None]*len(prefix_headers)  + row

class TsvBuildBundle(CsvBundle):

    delimiter = '\t'

    def __init__(self, bundle_dir=None):
        '''
        '''

        super(TsvBuildBundle, self).__init__(bundle_dir)


    def get_source(self, source):
        """Get the source file. If the file does not end in a CSV file, replace it with a CSV extension
        and look in the source store cache """
        import os

        if not source:
            source = self.metadata.sources.keys()[0]

        fn = self.filesystem.download(source)

        if fn.endswith('.zip'):
            fn = self.filesystem.unzip(fn)

        return fn

class GeoBuildBundle(LoaderBundle):
    """A Bundle variant that loads zipped Shapefiles"""
    def __init__(self, bundle_dir=None):
        '''
        '''

        super(GeoBuildBundle, self).__init__(bundle_dir)


    def meta(self):
        from ambry.geo.sfschema import copy_schema

        self.database.create()

        self._prepare_load_schema()

        def log(x):
            self.log(x)

        for table, item in self.metadata.sources.items():

            with self.session:
                copy_schema(self.schema, table_name=table, path=item.url, logger=log)

        self.schema.write_schema()

        return True


    def build(self):

        for table, item in self.metadata.sources.items():
            self.log("Loading table {} from {}".format(table, item.url))

            # Set the source SRS, if it was not set in the input file
            if self.metadata.build.get('s_srs', False):
                s_srs = self.metadata.build.s_srs
            else:
                s_srs = None

            lr = self.init_log_rate(print_rate=10)

            p = self.partitions.new_geo_partition(table=table, shape_file=item.url, s_srs=s_srs, logger=lr)
            self.log("Loading table {}. Done".format(table))

        for p in self.partitions:
            print p.info

        return True