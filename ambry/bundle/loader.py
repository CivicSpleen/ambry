""" Bundle variants that directly load files with little additional processing.
"""


from  ambry.bundle import BuildBundle

class LoaderBundle(BuildBundle):

    prefix_headers = ['id']

    row_gen_ext_map = {
        'xlsm': 'xls',
        'xlsx': 'xls'
    }

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

    @staticmethod
    def test_type(type_, v):

        try:
            type_(v)
            return True
        except:
            return False

    def mangle_column_name(self, i, n):
        """
        Override this method to change the way that column names from the source are altered to
        become column names
        :param i: column number
        :param n: original column name
        :return:
        """
        from ambry.orm import Column

        if not n:
            return 'column{}'.format(i)

        return Column.mangle_name(n.strip())

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

    def row_gen_for_source(self, source_name):
        from os.path import dirname, split, splitext

        source = self.metadata.sources[source_name]

        fn = self.filesystem.download(source_name)

        base_dir, file_name  = split(fn)
        file_base, ext = splitext(file_name)

        if ext.startswith('.'):
            ext = ext[1:]

        ext = self.row_gen_ext_map.get(ext,ext)

        if source.row_spec.dict:
            rs = source.row_spec.dict
        else:
            rs = {}

        rs['segment'] = source.segment
        rs['prefix_headers'] = self.prefix_headers
        if ext == 'csv':
            from rowgen import DelimitedRowGenerator
            return DelimitedRowGenerator(fn, **rs)
        elif ext  == 'xls':
            from rowgen import ExcelRowGenerator
            return ExcelRowGenerator(fn, **rs)
        else:
            raise Exception("Unknown ext: {}".format(ext))

    def meta_for_source(self, source_name):

        with self.session:

            source = self.metadata.sources[source_name]

            if source.is_loadable is False:
                return

            table = self.make_table_for_source(source_name)

            header, row = self.gen_rows(source_name, as_dict=False).next()

            if self.col_map:
                header = [self.col_map.get(col, col) for col in header]

            self.schema.update_from_iterator(table.name,
                                             header=self.mangle_header(header),
                                             iterator=self.gen_rows(source_name, as_dict=False),
                                             max_n=1000,
                                             logger=self.init_log_rate(500))

    def meta(self):

        self.database.create()

        if not self.run_args.get('clean', None):
            self._prepare_load_schema()

        for source_name, source in self.metadata.sources.items():
            self.meta_for_source(source_name)

        return True

    def build_from_source(self, source_name):

        source = self.metadata.sources[source_name]

        if source.is_loadable is False:
            return

        p = self.build_create_partition(source_name)

        self.log("Loading source '{}' into partition '{}'".format(source_name, p.identity.name))

        lr = self.init_log_rate(print_rate=5)

        columns = [c.name for c in p.table.columns]

        row_gen =  self.row_gen_for_source(source_name)

        header = [ self.mangle_column_name(i,n) for i,n in enumerate(row_gen.get_header())]

        for col in header:
            if col not in columns:
                self.error("Header column {} not in table {} for source {}".format(col, p.table.name, source_name))

        with p.inserter() as ins:
            for row in row_gen:

                assert len(row) == len(header), '{} != {}'

                lr(str(p.identity.name))

                d = dict(zip(header, row))

                self.build_modify_row(row_gen, p, source, d)

                ins.insert(d)

    def build(self):
        for source_name in self.metadata.sources:
            self.build_from_source(source_name)
        return True

class CsvBundle(LoaderBundle):
    """A Bundle variant for loading CSV files"""

    delimiter = None
    col_map = None  # If set, maps column headers

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


    def make_table_for_source(self, source_name):

        source = self.metadata.sources[source_name]

        table_name = source.table if source.table else source_name

        table_desc = source.description if source.description else "Table generated from {}".format(source.url)

        data = dict(source)
        del data['description']
        del data['url']

        table = self.schema.add_table(table_name, description=table_desc, data=data)

        return table

class ExcelBuildBundle(CsvBundle):
    pass

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