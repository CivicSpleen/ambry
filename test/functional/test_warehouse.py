# -*- coding: utf-8 -*-
from itertools import islice
import os
import stat
import unittest

from ambry_sources import MPRowsFile
from ambry_sources.sources import GeneratorSource, SourceSpec

from test.helpers import assert_sqlite_index, assert_ambry_sources, assert_postgres_index
from test.proto import TestBase


class Mixin(object):
    """ Requires successors provide _get_config method returning RunConfig instance. """

    # helpers

    def _assert_is_indexed(self, warehouse, partition, column):
        """ Raises AssertionError if column is not indexed. """

        if warehouse.dsn.startswith('sqlite'):
            assert_sqlite_index(warehouse._backend._connection, partition, column)
        else:
            assert_postgres_index(warehouse._backend._connection, partition, column)

    def test_query_mpr_with_auto_install(self):

        if isinstance(self, PostgreSQLTest):
            try:
                assert_ambry_sources('0.1.6')
            except AssertionError:
                self.skipTest('Need ambry_sources >= 0.1.6. Update your installation.')
            assert_shares_group(user='postgres')

        library = self.library()

        bundle = library.bundle('build.example.com-generators')
        bundle.ingest()
        bundle.source_schema()
        bundle.schema()
        bundle.build()

        partition = list(bundle.partitions)[0]
        self.assertTrue(os.path.exists(partition.datafile.syspath))

        warehouse = self.get_warehouse()

        try:
            # query partition.
            rows = list(islice(warehouse.query('SELECT * FROM {};'.format(partition.vid)), None, 3))

            self.assertEqual(0, rows[0][2])
            self.assertEqual(1, rows[1][2])
            self.assertEqual(2, rows[2][2])
        finally:
            bundle.progress.close()
            warehouse.close()
            library.database.close()

    def test_install_and_query_materialized_partition(self):
        # materialized view for postgres and readonly table for sqlite.

        if isinstance(self, PostgreSQLTest):
            try:
                assert_ambry_sources('0.1.6')
            except AssertionError:
                self.SkipTest('Need ambry_sources >= 0.1.6. Update your installation.')

        library = self.library()

        bundle = library.bundle('build.example.com-generators')
        bundle.ingest()
        bundle.source_schema()
        bundle.schema()
        bundle.build()

        partition = list(bundle.partitions)[0]
        self.assertTrue(os.path.exists(partition.datafile.syspath))

        warehouse = self.get_warehouse()

        try:
            # materialize partition (materialized view for postgres, readonly table for sqlite)
            warehouse.materialize(partition.vid)

            # query partition.
            rows = list(islice(warehouse.query('SELECT * FROM {};'.format(partition.vid)), None, 3))

            self.assertEqual(0, rows[0][2])
            self.assertEqual(1, rows[1][2])
            self.assertEqual(2, rows[2][2])

            self._assert_materialized(warehouse, partition)

        finally:
            bundle.progress.close()
            warehouse.close()
            library.database.close()

    def test_index_creation(self):
        if isinstance(self, PostgreSQLTest):
            try:
                assert_ambry_sources('0.1.6')
            except AssertionError:
                self.SkipTest('Need ambry_sources >= 0.1.6. Update your installation.')

        library = self.library()

        bundle = library.bundle('build.example.com-generators')
        bundle.ingest()
        bundle.source_schema()
        bundle.schema()
        bundle.build()

        partition = list(bundle.partitions)[0]
        self.assertTrue(os.path.exists(partition.datafile.syspath))

        warehouse = self.get_warehouse()

        try:

            # Index creation requires materialized tables.
            warehouse.materialize(partition.vid)

            column = 'int'
            assert column in partition.table.header

            # Create indexes
            warehouse.index(partition.vid, column)

            # query partition.
            self._assert_is_indexed(warehouse, partition, column)

            # query indexed data
            # rows = warehouse.query('SELECT {} FROM {} LIMIT 1;'.format(partition.vid, column))
            rows = warehouse \
                .query(
                    'SELECT {} FROM {} ORDER BY int ASC LIMIT 2;'
                    .format(column, partition.vid)) \
                .fetchall()
            self.assertEqual(rows, [(0,), (0,)])
        finally:
            bundle.progress.close()
            warehouse.close()
            library.database.close()

    @unittest.skip('This test needs a bundle that has multiple partitions of the same table')
    def test_table_install_and_query(self):
        try:
            assert_ambry_sources('0.1.8')
        except AssertionError:
            self.SkipTest('Need ambry_sources >= 0.1.8. Update your installation.')

        library = self.library()

        bundle = library.bundle('build.example.com-generators')
        bundle.ingest()
        bundle.source_schema()
        bundle.schema()
        bundle.build()
        partition1 = list(bundle.partitions)[0]
        partition2 = list(bundle.partitions)[1]
        self.assertTrue(os.path.exists(partition1.datafile.syspath))
        self.assertTrue(os.path.exists(partition2.datafile.syspath))

        warehouse = self.get_warehouse()

        try:
            partition1._datafile = _get_datafile(bundle.build_fs, partition1.cache_key)
            partition2._datafile = _get_datafile(bundle.build_fs, partition2.cache_key, rows=[[3, 3], [4, 4]])

            # Install table
            library.warehouse.install(table1.vid)

            # query all partitions
            rows = library.warehouse.query('SELECT col1, col2 FROM {};'.format(table1.vid))

            # We need to sort rows before check because the order of the table partitions is unknown.
            self.assertEqual(sorted(rows), sorted([(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)]))
        finally:
            bundle.progress.close()
            library.warehouse.close()
            library.database.close()

    @unittest.skip('This test needs a bundle that has multiple partitions of the same table')
    def test_query_with_union(self):
        if isinstance(self, PostgreSQLTest):
            try:
                assert_ambry_sources('0.1.6')
            except AssertionError:
                self.SkipTest('Need ambry_sources >= 0.1.6. Update your installation.')
        else:
            # sqlite tests
            try:
                assert_ambry_sources('0.1.8')
            except AssertionError:
                self.skipTest('Need ambry_sources >= 0.1.8. Update your installation.')

        library = self.library()

        bundle = library.bundle('build.example.com-generators')
        bundle.ingest()
        bundle.source_schema()
        bundle.schema()
        bundle.build()
        partition1 = list(bundle.partitions)[0]
        partition2 = list(bundle.partitions)[1]
        self.assertTrue(os.path.exists(partition1.datafile.syspath))
        self.assertTrue(os.path.exists(partition2.datafile.syspath))

        try:

            # execute nested query.
            query = '''
                SELECT col1, col2 FROM {}
                UNION
                SELECT col1, col2 FROM {};'''\
                .format(partition1.vid, partition2.vid)
            rows = library.warehouse.query(query)

            # We need to sort rows before check because the order of the table partitions is unknown.
            self.assertEqual(sorted(rows), sorted([(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)]))
        finally:
            bundle.progress.close()
            library.warehouse.close()
            library.database.close()


class InMemorySQLiteTest(TestBase, Mixin):

    def setUp(self):
        super(InMemorySQLiteTest, self).setUp()
        if self._db_type != 'sqlite':
            self.skipTest('SQLite tests are disabled.')

    def get_warehouse(self):
        return self.library().warehouse(dsn='sqlite://')

    def _assert_materialized(self, warehouse, partition):
        # Drop partition datafile and check again. So the data can only come from materialization.
        partition.datafile.remove()
        self.assertFalse(partition.datafile.exists)
        rows = list(islice(warehouse.query('SELECT * FROM {};'.format(partition.vid)), None, 3))

        self.assertEqual(0, rows[0][2])
        self.assertEqual(1, rows[1][2])
        self.assertEqual(2, rows[2][2])


class FileSQLiteTest(TestBase, Mixin):

    def setUp(self):
        super(FileSQLiteTest, self).setUp()
        if self._db_type != 'sqlite':
            self.skipTest('SQLite tests are disabled.')

    def get_warehouse(self):
        return self.library().warehouse()

    def _assert_materialized(self, warehouse, partition):

        # Re-open the database through SQLalchemy, which won't have the module installed,
        # so the data can only come from materialization

        rows = list(islice(warehouse.engine.execute('SELECT * FROM {};'.format(partition.vid)), None, 3))

        self.assertEqual(0, rows[0][2])
        self.assertEqual(1, rows[1][2])
        self.assertEqual(2, rows[2][2])


class PostgreSQLTest(TestBase, Mixin):

    def setUp(self):
        super(PostgreSQLTest, self).setUp()
        if self._db_type != 'postgres':
            self.skipTest('PostgreSQL tests are disabled.')

    def get_warehouse(self):
        return self.library().warehouse()

    def _assert_materialized(self, warehouse, partition):
        # Drop partition datafile and check again. So the data can only come from materialization.
        rows = list(islice(warehouse.query('SELECT * FROM {};'.format(partition.vid)), None, 3))

        self.assertEqual(0, rows[0][2])
        self.assertEqual(1, rows[1][2])
        self.assertEqual(2, rows[2][2])


def assert_shares_group(user=''):
    """ Checks that the given user shares group with user who executes tests.

    Args:
        user (str): system username

    Raises:
        AssertionError: if given user is not the member of the tests executor group.

    """
    assert user, 'user is required attribute.'
    import getpass
    import grp
    import pwd
    current_user_group_id = pwd.getpwnam(getpass.getuser()).pw_gid
    current_user_group = grp.getgrgid(current_user_group_id).gr_name

    other_user_groups = [g.gr_name for g in grp.getgrall() if user in g.gr_mem]
    if current_user_group not in other_user_groups:
        details_link = 'https://github.com/CivicKnowledge/ambry_sources#making-mpr-files-readable-by-postgres-user'
        raise AssertionError(
            'This test requires postgres user to be in the {} group.\n'
            'Hint: see {} for details.'.format(current_user_group, details_link))


def is_group_readable(filepath):
    """ Returns True if given file is group readable, otherwise returns False.

    Args:
        filepath (str):

    """
    st = os.stat(filepath)
    return bool(st.st_mode & stat.S_IRGRP)


def get_perm(filepath):
    return stat.S_IMODE(os.lstat(filepath)[stat.ST_MODE])


def _get_generator_source(rows=None):
    if not rows:
        rows = [
            [0, 0],
            [1, 1],
            [2, 2]]

    def gen(rows=rows):
        # generate header
        yield ['col1', 'col2']

        # generate some rows
        for row in rows:
            yield row
    return GeneratorSource(SourceSpec('foobar'), gen())


class BundleWarehouse(TestBase):

    def test_bundle_warehouse_install(self):

        b = self.import_single_bundle('build.example.com/casters')
        b.ingest()
        b.source_schema()
        b.schema()
        b.build()

        wh = b.warehouse('test')

        wh.clean()

        self.assertEqual(0, len(wh.list()))

        self.assertEqual('p00casters004003', wh.install('build.example.com-casters-integers'))
        self.assertEqual('p00casters002003', wh.install('build.example.com-casters-simple_stats'))
        self.assertEqual('p00casters006003', wh.materialize('build.example.com-casters-simple'))

        self.assertEqual(3, len(wh.list()))

    def test_bundle_warehouse_query(self):
        l = self.library()

        b = self.import_single_bundle('build.example.com/casters')
        b.ingest()
        b.source_schema()
        b.schema()
        b.build()

        wh = b.warehouse('test')
        wh.clean()

        self.assertEqual(0, len(wh.list()))

        self.assertEqual(20, sum(1 for row in wh.query('SELECT * FROM p00casters004003;')))
        self.assertEqual(6000, sum(1 for row in wh.query('SELECT * FROM p00casters006003;')))

        p = l.partition('p00casters004003')

        self.assertEqual(20, sum(1 for row in wh.query('SELECT * FROM {};'.format(p.vname))))
        self.assertEqual(20, sum(1 for row in wh.query('SELECT * FROM {};'.format(p.name))))

        self.assertEqual(3, len(wh.list()))

    def test_library_warehouse_query(self):
        l = self.library()

        b = l.bundle('build.example.com-casters')
        wh = l.warehouse()
        wh.clean()

        self.assertEqual(0, len(wh.list()))

        self.assertEqual(20, sum(1 for row in wh.query('SELECT * FROM p00casters004003;')))
        self.assertEqual(6000, sum(1 for row in wh.query('SELECT * FROM p00casters006003;')))
        self.assertEqual(4000, sum(1 for row in wh.query('SELECT * FROM pERJQxWUVb005001;')))

        self.assertEqual(3, len(wh.list()))

    def test_library_build_from_sql(self):

        l = self.library()

        b = l.bundle('build.example.com-sql')
        wh = l.warehouse()
        wh.clean()

        for source in ['use_select', 'use_view']:

            b.ingest(sources=[source])

            b.source_schema(sources=[source])

            b.schema(sources=[source])

            b.build(sources=[source])

        self.assertEqual(20, sum(1 for _ in b.partition(table='use_select')))

        self.assertEqual(20, sum(1 for _ in b.partition(table='use_view')))


def _get_datafile(fs, path, rows=None):
    datafile = MPRowsFile(fs, path)
    datafile.load_rows(_get_generator_source(rows=rows))
    return datafile
