""" Bundle variants that directly load files with little additional processing.
"""

from ambry.bundle import BuildBundle
from rowgen import RowSpecIntuiter


class LoaderBundle(BuildBundle):
    row_gen_ext_map = {
        'xlsm': 'xls',
        'xlsx': 'xls'
    }

    def __init__(self, bundle_dir=None):
        import os

        super(LoaderBundle, self).__init__(bundle_dir)

        self.col_map_fn = self.filesystem.path('meta', 'column_map.csv')

        # Load it
        if os.path.exists(self.col_map_fn):
            self.col_map = self.filesystem.read_csv(self.col_map_fn, key='header')
        else:
            self.col_map = {}

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
        become column names in the schema

        :param i: column number
        :param n: original column name
        :return:
        """
        from ambry.orm import Column

        if not n:
            return 'column{}'.format(i)

        mn = Column.mangle_name(n.strip())

        if mn in self.col_map:
            col = self.col_map[mn]['col']
            if col:
                return col
            else:
                return mn

        else:
            return mn

    def mangle_header(self, header):
        """Transform the header as it comes from the raw row generator into a column name"""

        return [self.mangle_column_name(i, n) for i, n in enumerate(header)]

    def build_create_partition(self, source_name):
        """Create or find a partition based on the source

        Will also load the source metadata into the partition, as a dict, under the key name of the source name.
        """

        source = self.metadata.sources[source_name]

        try:
            table = source['table']

            if not table:
                table = source_name

        except:
            table = source_name

        assert bool(table)

        kwargs = dict(table=table)

        if source.grain:
            kwargs['grain'] = source.grain

        p = self.partitions.find_or_new(table=table)

        with self.session:
            if 'source_data' not in p.record.data:
                p.record.data['source_data'] = {}
            p.record.data['source_data'][source_name] = source.dict

        return p

    def get_source(self, source):
        """Check for saved source"""
        import os

        # If the file we are given isn't actually a CSV file, we might have manually
        # converted it to a CSV and put it in the source store.
        if not fn.lower().endswith('.csv'):
            cache = self.filesystem.source_store

            if cache:
                bare_fn, ext = os.path.splitext(os.path.basename(fn))

                fn_ck = self.source_store_cache_key(bare_fn + ".csv")

                if cache.has(fn_ck):
                    if not self.filesystem.download_cache.has(fn_ck):
                        with cache.get_stream(fn_ck) as s:
                            self.filesystem.download_cache.put(s, fn_ck)

                    return self.filesystem.download_cache.path(fn_ck)

        return fn

    def row_gen_for_source(self, source_name):
        from os.path import dirname, split, splitext
        from ..dbexceptions import BuildError

        source = self.metadata.sources[source_name]

        dfn = fn = self.filesystem.download(source_name)

        if fn.endswith('.zip'):
            sub_file = source.file
            fn = self.filesystem.unzip(fn, regex=sub_file)

        base_dir, file_name = split(fn)
        file_base, ext = splitext(file_name)

        if source.filetype:
            ext = source.filetype
        else:
            if ext.startswith('.'):
                ext = ext[1:]

            ext = self.row_gen_ext_map.get(ext, ext)

        print '!!!!', ext, source.filetype

        if source.row_spec.dict:
            rs = source.row_spec.dict
        else:
            rs = {}

        if source.segment:
            rs['segment'] = source.segment

        rs['header_mangler'] = lambda header: self.mangle_header(header)

        if ext == 'csv':
            from rowgen import DelimitedRowGenerator

            return DelimitedRowGenerator(fn, **rs)
        elif ext == 'xls':
            from rowgen import ExcelRowGenerator
            return ExcelRowGenerator(fn, **rs)
        else:
            raise Exception("Unknown source file extension: '{}' for file '{}' from source {} "
                            .format(ext, file_name, source_name))

    def make_table_for_source(self, source_name):

        source = self.metadata.sources[source_name]

        table_name = source.table if source.table else source_name

        table_desc = source.description if source.description else "Table generated from {}".format(source.url)

        table = self.schema.add_table(table_name, description=table_desc)

        self.schema.add_column(table, 'id', datatype='integer', description=table_desc, is_primary_key=True)

        self.log("Created table {}".format(table.name))

        if source.grain:
            with self.session:
                if 'grain' in table.data and table.data['grain'] != source.grain:
                    raise BuildBundle("Table '{}' has grain '{}' conflicts with source '{}' grain of '{}'"
                                      .format(table_name, table.data['grain'], source_name, source.grain))

                table.data['grain'] = source.grain

        return self.schema.table(table_name)  # The session in 'if source.grain' may expire table, so refresh

    def meta_set_row_specs(self, row_intuitier_class=RowSpecIntuiter):
        """
        Run the row intuiter, which tries to figure out where the header and data lines are.

        :param row_intuitier_class: A RowSpecIntuiter class
        :return:
        """

        for source_name in self.metadata.sources:
            source = self.metadata.sources.get(source_name)

            rg = self.row_gen_for_source(source_name)

            ri = row_intuitier_class(rg).intuit()

            source.row_spec = ri

        self.metadata.write_to_dir()

    def meta(self):
        from collections import defaultdict
        from ..util.intuit import Intuiter
        import urllib2
        import unicodecsv as csv
        import os

        # A proto terms map, for setting grains
        pt = self.library.get('civicknowledge.com-proto-proto_terms').partition
        {r['name']: r['obj_number'] for r in pt.rows}

        self.database.create()

        tables = defaultdict(set)

        # First, load in the protoschema, to get prefix columns for each table.
        sf_path = self.filesystem.path('meta', self.PROTO_SCHEMA_FILE)

        if os.path.exists(sf_path):
            with open(sf_path, 'rbU') as f:
                self.schema.schema_from_file(f)

        if not self.run_args.get('clean', None):
            self._prepare_load_schema()

        # Collect all of the sources for each table, while also creating the tables
        for source_name, source in self.metadata.sources.items():

            if source.is_loadable is False:
                return

            table = self.make_table_for_source(source_name)
            tables[table.name].add(source_name)

        self.schema.write_schema()

        intuiters = defaultdict(Intuiter)

        # Intuit all of the tables

        for table_name, sources in tables.items():

            intuiter = intuiters[table_name]

            iterables = []

            for source_name in sources:

                try:
                    self.filesystem.download(source_name)
                except urllib2.HTTPError:
                    self.error("Failed to download url for source: {}".format(source_name))
                    continue

                self.log("Intuiting {} into {}".format(source_name, table_name))
                iterables.append(self.row_gen_for_source(source_name))

                rg = self.row_gen_for_source(source_name)

                intuiter.iterate(rg, 2000)

            self.schema.update_from_intuiter(table_name, intuiter)

            # Write the first 50 lines of the csv file, to see what the intuiter got from the
            # raw-row-gen
            with open(self.filesystem.build_path('{}-raw-rows.csv'.format(table_name)), 'w') as f:
                rg = self.row_gen_for_source(source_name)
                rrg = rg.raw_row_gen
                w = csv.writer(f)

                for i, row in enumerate(rrg):
                    if i > 50:
                        break

                    w.writerow(list(row))

            # Now write the first 50 lines from the row gen, after appliying the row spec
            with open(self.filesystem.build_path('{}-specd-rows.csv'.format(table_name)), 'w') as f:
                rg = self.row_gen_for_source(source_name)

                w = csv.writer(f)

                w.writerow(rg.header)

                for i, row in enumerate(rg):
                    if i > 50:
                        break

                    w.writerow(list(row))

            # Write an intuiter report, to review how the intuiter made it's decisions
            with open(self.filesystem.build_path('{}-intuit-report.csv'.format(table_name)), 'w') as f:
                w = csv.DictWriter(f, ("name length resolved_type has_codes count ints "
                                       "floats strs nones datetimes dates times strvals".split()))
                w.writeheader()
                for d in intuiter.dump():
                    w.writerow(d)

            # Load and update the column map
            # .. already loaded in the constructor

            # Update

            if os.path.exists(self.col_map_fn):
                col_map = self.filesystem.read_csv(self.col_map_fn, key='header')
            else:
                col_map = {}

            # Don't add the columns that are already mapped.
            mapped_domain = set(item['col'] for item in col_map.values())

            # TODO: do we need this loop here?
            for table_name, sources in tables.items():
                rg = self.row_gen_for_source(source_name)

                header = rg.header  # Also sets unmangled_header

                descs = [x.replace('\n', '; ') for x in (rg.unmangled_header if rg.unmangled_header else header)]

                for col_name, desc in zip(header, descs):
                    k = col_name.strip()

                    if k not in col_map and col_name not in mapped_domain:
                        col_map[k] = dict(header=k, col='')

            # Write back out
            with open(self.col_map_fn, 'w') as f:

                w = csv.DictWriter(f, fieldnames=['header', 'col'])
                w.writeheader()
                for k in sorted(col_map.keys()):
                    w.writerow(col_map[k])
        return True

    def build_modify_row(self, row_gen, p, source, row):
        """
        Modify a row just before being inserted into the partition

        :param row_gen: Row generator that created the row
        :param p: Partition the row will be inserted into
        :param source: Source record of the original data
        :param row: A dict of the row
        :return:
        """
        pass

    def build_from_source(self, source_name):

        source = self.metadata.sources[source_name]

        if source.is_loadable is False:
            return

        p = self.build_create_partition(source_name)

        self.log("Loading source '{}' into partition '{}'".format(source_name, str(p.identity.name)))

        lr = self.init_log_rate(print_rate=5)

        columns = [c.name for c in p.table.columns]

        row_gen = self.row_gen_for_source(source_name)

        header = row_gen.header

        for col in header:
            if col not in columns:
                self.error("Header column '{}' not in table {} for source {}"
                           .format(col, p.table.name, source_name))

        with p.inserter() as ins:
            for row in row_gen:
                assert len(row) == len(header), '{} != {}'

                lr(str(p.identity.name))

                d = dict(zip(header, row))

                self.build_modify_row(row_gen, p, source, d)

                errors = ins.insert(d)

                if errors:
                    self.error("Casting error for {}: {}".format(source_name, errors))

    def build(self):
        for source_name in self.metadata.sources:
            self.build_from_source(source_name)

            if self.run_args.test:
                break

        return True


class CsvBundle(LoaderBundle):
    """A Bundle variant for loading CSV files"""

    pass


class ExcelBuildBundle(CsvBundle):
    pass


class TsvBuildBundle(CsvBundle):
    delimiter = '\t'

    def __init__(self, bundle_dir=None):
        """
        """

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
        """
        """

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

        for source_name, source in self.metadata.sources.items():
            self.log("Loading table {} from {}".format(source_name, source.url))

            # Set the source SRS, if it was not set in the input file
            if self.metadata.build.get('s_srs', False):
                s_srs = self.metadata.build.s_srs
            else:
                s_srs = None

            lr = self.init_log_rate(print_rate=10)

            try:
                table = source['table']

                if not table:
                    table = source_name
            except:
                table = source_name

            p = self.partitions.new_geo_partition(table=table, shape_file=source.url, s_srs=s_srs, logger=lr)

            with self.session:
                if 'source_data' not in p.record.data:
                    p.record.data['source_data'] = {}

                p.record.data['source_data'][source_name] = source.dict

            self.log("Loading table {}. Done".format(source_name))

        for p in self.partitions:
            print p.info

        return True