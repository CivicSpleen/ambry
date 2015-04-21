"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""

from . import PartitionBase
from ..identity import PartitionIdentity, PartitionName
from ..database.partition import PartitionDb


class SqlitePartitionName(PartitionName):
    PATH_EXTENSION = '.db'
    FORMAT = 'db'


class SqlitePartitionIdentity(PartitionIdentity):
    _name_class = SqlitePartitionName


class SqlitePartition(PartitionBase):

    """Represents a bundle partition, part of the bundle data broken out in
    time, space, or by table."""

    _id_class = SqlitePartitionIdentity
    _db_class = PartitionDb

    def __init__(self, bundle, record, memory=False, **kwargs):

        super(SqlitePartition, self).__init__(bundle, record)
        self.memory = memory

    @property
    def database(self):
        if self._database is None:
            self._database = PartitionDb(
                self.bundle,
                self,
                base_path=self.path,
                memory=self.memory)
        return self._database

    def detach(self, name=None):
        return self.database.detach(name)

    def attach(self, id_, name=None):
        return self.database.attach(id_, name)

    def create_indexes(self, table=None):

        if not self.database.exists():
            self.create()

        if table is None:
            table = self.get_table()

        if isinstance(table, basestring):
            table = self.bundle.schema.table(table)

        for sql in self.bundle.schema.generate_indexes(table):
            self.database.connection.execute(sql)

    def drop_indexes(self, table=None):

        if not self.database.exists():
            self.create()

        if table is None:
            table = self.get_table()

        if not isinstance(table, basestring):
            table = table.name

        indexes = []

        for row in self.database.query("""SELECT name
            FROM sqlite_master WHERE type='index' AND tbl_name = '{}';""".format(table)):

            if row[0].startswith('sqlite_'):
                continue

            indexes.append(row[0])

        for index_name in indexes:

            print 'Drop', index_name

            self.database.connection.execute(
                "DROP INDEX {}".format(index_name))

    def create_with_tables(self, tables=None, clean=False):
        '''Create, or re-create,  the partition, possibly copying tables
        from the main bundle

        Args:
            tables. String or Array of Strings. Specifies the names of tables to
            copy from the main bundle.

            clean. If True, delete the database first. Defaults to true.

        '''

        if not tables:
            raise ValueError("'tables' cannot be empty")

        if not isinstance(tables, (list, set, tuple)):
            tables = [tables]
        else:
            tables = list(tables)

        if clean:
            self.database.delete()

        self.database.create()

        self.add_tables(tables)

    def add_tables(self, tables):

        for t in tables:
            if not t in self.database.inspector.get_table_names():
                _, table = self.bundle.schema.get_table_meta(t)
                table.create(bind=self.database.engine)

    def create(self):

        tables = self.data.get('tables', [])

        if tables:
            self.create_with_tables(tables=tables)
        else:
            self.database.create()

        # Closing becuase when creating a lot ot them, having more than 64 open will
        # cause the sqlite driver to return with 'unable to open database'
        # error
        self.close()

    def clean(self):
        """Delete all of the records in the tables declared for this
        oartition."""

        for table in self.data.get('tables', []):
            try:
                self.database.query("DELETE FROM {}".format(table))
            except:
                pass

        return self

    def finalize(self, force=False):

        if force or (not self.is_finalized and self.database.exists()):
            self.write_basic_stats()
            self.write_file()
            self.write_full_stats()
            self.compile_geo_coverage()
            self.compile_time_coverage()
            self.build_sample()

    def guess_lom(self, col, stats):
        """
        Guess the level of measurement for the column: Nominal, Ordinal, Interval, Ratio


        :param col:
        :param stats:
        :return:
        """

        if col.name == 'id' and (col.is_primary_key or col.fk_vid):
            return 'key'

        if col.type_is_text:
            return 'nom'

    def write_full_stats(self):
        """Write stats to the stats table.

        Dataset Id
        Column id
        Table Id
        Partition Id
        Count
        Mean, Std
        Min, 25, 50 75, Max
        # Uniques
        JSON of top 50 Unique values
        JSON of Histogram of 100 values, for Int and Real

        :return:

        """
        import pandas as pd
        import numpy as np
        import json

        df = self.pandas

        if df is None:
            return  # Usually b/c there are no records in the table.

        self.close()
        self.bundle.close()

        with self.bundle.session:
            table = self.record.table
            p = self.bundle.partitions.get(self.vid)

            all_cols = [c.name for c in table.columns]

            for row in df.describe().T.reset_index().to_dict(orient='records'):
                col_name = row['index']

                if col_name == 'id':
                    continue

                col = table.column(col_name)

                row['nuniques'] = df[col_name].dropna().nunique()

                h = np.histogram(df[col_name])

                row['hist'] = dict(values=zip(h[1], h[0]))

                del row['index']

                p.add_stat(col.vid, row)

                all_cols.remove(col_name)

            for col_name in all_cols:
                row = {}
                col = table.column(col_name)

                row['count'] = len(df[col_name])

                row['nuniques'] = df[col_name].dropna().nunique()

                if col.type_is_text():

                    row['uvalues'] = df[col_name].value_counts().sort(
                        inplace=False,
                        ascending=False)[
                        :100].to_dict()

                p.add_stat(col.vid, row)

    def write_basic_stats(self):
        """Record in the partition entry basic statistics for the partition's
        primary table."""
        from ..partitions import Partitions

        t = self.get_table()

        if not t:

            return

        if not t.primary_key:
            from ..dbexceptions import ConfigurationError

            raise ConfigurationError(
                "Table {} does not have a primary key; can't compute stats".format(
                    t.name))

        partition_s = self.database.session

        self.record.count = partition_s.execute(
            "SELECT COUNT(*) FROM {}".format(self.table.name)).scalar()
        self.record.min_key = partition_s.execute(
            "SELECT MIN({}) FROM {}".format(
                t.primary_key.name,
                self.table.name)).scalar()
        self.record.max_key = partition_s.execute(
            "SELECT MAX({}) FROM {}".format(
                t.primary_key.name,
                self.table.name)).scalar()

        with self.bundle.session as bundle_s:

            bundle_s.add(self.record)

            bundle_s.commit()

        self.set_state(Partitions.STATE.FINALIZED)

    def compile_geo_coverage(self):
        """Compile GVIDs for the geographic coverage and grain of the
        partition."""

        from geoid import civick
        from geoid.util import simplify

        p_s = self.database.session

        geo_cols = []
        table_name = self.table.name
        for c in self.table.columns:
            if 'gvid' in c.name:
                geo_cols.append(c.name)

        geoids = set()

        for gc in geo_cols:
            for row in p_s.execute("SELECT DISTINCT {} FROM {}".format(gc, table_name)):
                gvid = civick.GVid.parse(row[0])
                if gvid:
                    geoids.add(gvid)

        # If there is source data ( from the sources metadata in the build set in the loader in build_create_partition)
        # then use the time and space values as additional geo and time
        # information.

        extra_spaces = []

        if 'source_data' in self.record.data:
            for source_name, source in self.record.data['source_data'].items():
                if 'space' in source:
                    extra_spaces.append((source_name, source['space']))

        if self.identity.space:  # And from the partition name
            extra_spaces.append(('pname', self.identity.space))

        for source_name, space in extra_spaces:
            try:
                g = civick.GVid.parse(space)
            except KeyError:

                places = list(
                    self.bundle.library.search.search_identifiers(space))

                if not places:
                    from ..dbexceptions import BuildError
                    raise BuildError(
                        ("Failed to find space identifier '{}' in full text identifier search"
                         " for partition '{}' and source name '{}'") .format(
                            space, str(
                                self.identity), source_name))

                score, gvid, name = places[0]

                self.bundle.log(
                    "Resolving space '{}' from source '{}' to {}/{}". format(space, source_name, name, gvid))

                geoids.add(civick.GVid.parse(gvid))

        coverage, grain = simplify(geoids)

        # The first simplification may produce a set that can be simplified
        # again
        coverage, _ = simplify(coverage)

        # For geo_coverage, only includes the higher level summary levels,
        # counties, states, places and urban areas
        self.record.data['geo_coverage'] = sorted(
            [str(x) for x in coverage if bool(x) and x.sl in (40, 50, 60, 160, 400)])
        self.record.data['geo_grain'] = sorted([str(x) for x in grain])

        # Now add the geo and time coverage specified in the table. These values for space and time usually are specified
        # in the sources metadata, and are copied into the

        s = self.bundle.database.session
        s.merge(self.record)
        s.commit()

    def compile_time_coverage(self):
        from ambry.util.datestimes import expand_to_years

        date_cols = []
        years = set()
        table_name = self.table.name
        for c in self.table.columns:
            if 'year' in c.name:
                date_cols.append(c.name)

        p_s = self.database.session

        # From the table
        for dc in date_cols:
            for row in p_s.execute("SELECT DISTINCT {} FROM {}".format(dc, table_name)):
                years.add(row[0])

        # From the source
        # If there was a time value in the source that this partition was created from, then
        # add it to the years.
        if 'source_data' in self.record.data:
            for source_name, source in self.record.data['source_data'].items():
                if 'time' in source:
                    for year in expand_to_years(source['time']):
                        years.add(year)

        # From the partition name
        if self.identity.name.time:
            for year in expand_to_years(self.identity.name.time):
                years.add(year)

        self.record.data['time_coverage'] = list(years)

        s = self.bundle.database.session
        s.merge(self.record)
        s.commit()

    def build_sample(self):

        name = self.table.name

        count = int(
            self.database.connection.execute(
                'SELECT count(*) FROM "{}"'.format(name)).fetchone()[0])

        skip = count / 20

        if count > 100:
            sql = 'SELECT * FROM "{}" WHERE id % {} = 0 LIMIT 20'.format(
                name,
                skip)
        else:
            sql = 'SELECT * FROM "{}" LIMIT 20'.format(name)

        sample = []

        for j, row in enumerate(self.database.connection.execute(sql)):
            sample.append(row.values())

        self.record.data['sample'] = sample

        s = self.bundle.database.session
        s.merge(self.record)
        s.commit()

    def write_file(self):
        """Create a file entry in the bundle for the partition, storing the md5
        checksum and size."""

        import os
        from ..orm import File
        from ..util import md5_for_file
        from sqlalchemy.exc import IntegrityError

        self.database.close()

        statinfo = os.stat(self.database.path)

        f = File(path=self.identity.cache_key,
                 group='partition',
                 ref=self.identity.vid,
                 state='built',
                 type_='P',
                 hash=md5_for_file(self.database.path),
                 size=statinfo.st_size)

        with self.bundle.session as s:

            s.query(File).filter(File.path == self.identity.cache_key).delete()

            try:
                s.add(f)
                s.commit()
            except IntegrityError:
                s.rollback()
                s.merge(f)
                s.commit()

    @property
    def rows(self):
        """Run a select query to return all rows of the primary table."""

        if True:
            pk = self.get_table().primary_key.name
            return self.database.query(
                "SELECT * FROM {} ORDER BY {} ".format(self.get_table().name, pk))
        else:
            return self.database.query(
                "SELECT * FROM {}".format(self.get_table().name))

    @property
    def pandas(self):
        from sqlalchemy.exc import NoSuchColumnError

        pk = self.get_table().primary_key.name

        try:
            return self.select(
                "SELECT * FROM {}".format(self.get_table().name), index_col=pk).pandas
        except NoSuchColumnError:
            return self.select(
                "SELECT * FROM {}".format(self.get_table().name)).pandas
        except StopIteration:
            return None  # No records, so no dataframe.
            #raise Exception("Select failed: {}".format("SELECT * FROM {}".format(self.get_table().name)))

    @property
    def dict(self):

        table = self.table

        d = dict(
            stats={s.column.name: s.dict for s in self._stats},
            **self.record.dict
        )

        del d['table']
        d['table'] = table.nonull_col_dict,

        return d

    def query(self, *args, **kwargs):
        """Convience function for self.database.query()"""

        return self.database.query(*args, **kwargs)

    def select(self, sql=None, *args, **kwargs):
        """Run a query and return an object that allows the selected rows to be
        returned as a data object in numpy, pandas, petl or other forms."""
        from ..database.selector import RowSelector

        return RowSelector(self, sql, *args, **kwargs)

    def add_view(self, view_name):
        """Add a view specified in the configuration in the views.<viewname>
        dict."""
        from ..dbexceptions import ConfigurationError

        vd = self.bundle.metadata.views.get(view_name, None)

        if not vd:
            raise ConfigurationError(
                "Didn't file requested view in the configuration. "
                "Should have been at: views.{}".format(view_name))

        self.database.add_view(view_name, vd['sql'])

        self.bundle.log("Created view {}".format(view_name))

    def __repr__(self):
        return "<db partition: {}>".format(self.identity.vname)
