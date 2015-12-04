# -*- coding: utf-8 -*-
import os
import stat
import unittest

from semantic_version import Version

from test.factories import PartitionFactory, TableFactory

import ambry_sources
from ambry_sources import MPRowsFile
from ambry_sources.med import sqlite as sqlite_med
from ambry_sources.med import postgresql as postgres_med
from ambry_sources.sources import GeneratorSource, SourceSpec

from test.test_base import TestBase, PostgreSQLTestBase

AMBRY_SOURCES_VERSION = getattr(ambry_sources, '__version__', None) or ambry_sources.__meta__.__version__


class Mixin(object):
    """ Requires successors to inherit from TestBase and provide _get_library method. """

    # helpers

    def _assert_is_indexed(self, warehouse, partition, column):
        ''' Raises AssertionError if column is not indexed. '''
        raise NotImplementedError('Override the method and provide db specific index check.')

    def _get_generator_source(self, rows=None):
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

    def test_query_mpr_with_auto_install(self):
        if isinstance(self, PostgreSQLTest):
            if Version(AMBRY_SOURCES_VERSION) < Version('0.1.6'):
                self.skipTest('Need ambry_sources >= 0.1.6. Update your installation.')
            assert_shares_group(user='postgres')
        library = self._get_library()

        # FIXME: Find the way how to initialize bundle with partitions and drop partition creation.
        bundle = self.setup_bundle(
            'simple', source_url='temp://', build_url='temp://', library=library)
        PartitionFactory._meta.sqlalchemy_session = bundle.dataset.session
        partition1 = PartitionFactory(dataset=bundle.dataset)
        bundle.wrap_partition(partition1)

        try:
            datafile = MPRowsFile(bundle.build_fs, partition1.cache_key)
            datafile.load_rows(self._get_generator_source())
            partition1._datafile = datafile
            rows = library.warehouse.query('SELECT * FROM {};'.format(partition1.vid))
            self.assertEqual(rows, [(0, 0), (1, 1), (2, 2)])
        finally:
            library.warehouse.close()
            library.database.close()

    def test_install_and_query_materialized_table(self):
        # materialized view for postgres and readonly table for sqlite.
        if isinstance(self, PostgreSQLTest):
            _assert_valid_ambry_sources()

        library = self._get_library()

        # FIXME: Find the way how to initialize bundle with partitions and drop partition creation.
        bundle = self.setup_bundle(
            'simple', source_url='temp://', build_url='temp://', library=library)
        PartitionFactory._meta.sqlalchemy_session = bundle.dataset.session
        partition1 = PartitionFactory(dataset=bundle.dataset)
        bundle.wrap_partition(partition1)

        try:
            datafile = MPRowsFile(bundle.build_fs, partition1.cache_key)
            datafile.load_rows(self._get_generator_source())
            partition1._datafile = datafile

            # materialize partition (materialized view for postgres, readonly table for sqlite)
            library.warehouse.materialize(partition1.vid)

            # query partition.
            rows = library.warehouse.query('SELECT * FROM {};'.format(partition1.vid))

            # now drop the *.mpr file and check again. Query should return the same data.
            #
            syspath = datafile.syspath
            os.remove(syspath)
            self.assertFalse(os.path.exists(syspath))
            rows = library.warehouse.query('SELECT * FROM {};'.format(partition1.vid))
            self.assertEqual(rows, [(0, 0), (1, 1), (2, 2)])
        finally:
            library.warehouse.close()
            library.database.close()

    def test_index_creation(self):
        if isinstance(self, PostgreSQLTest):
            _assert_valid_ambry_sources()

        library = self._get_library()

        # FIXME: Find the way how to initialize bundle with partitions and drop partition creation.
        bundle = self.setup_bundle(
            'simple', source_url='temp://', build_url='temp://', library=library)
        PartitionFactory._meta.sqlalchemy_session = bundle.dataset.session
        partition1 = PartitionFactory(dataset=bundle.dataset)
        bundle.wrap_partition(partition1)

        try:
            datafile = MPRowsFile(bundle.build_fs, partition1.cache_key)
            datafile.load_rows(self._get_generator_source())
            partition1._datafile = datafile

            # Index creation requires materialized tables.
            library.warehouse.materialize(partition1.vid)

            # Create indexes
            library.warehouse.index(partition1.vid, ['col1', 'col2'])

            # query partition.
            self._assert_is_indexed(library.warehouse, partition1, 'col1')
            self._assert_is_indexed(library.warehouse, partition1, 'col2')

            # query indexed data
            rows = library.warehouse.query('SELECT col1, col2 FROM {};'.format(partition1.vid))
            self.assertEqual(rows, [(0, 0), (1, 1), (2, 2)])
        finally:
            library.warehouse.close()
            # FIXME: Use library.warehouse.close() only.
            library.database.close()

    def test_table_query(self):
        if isinstance(self, PostgreSQLTest):
            _assert_valid_ambry_sources()
        else:
            # sqlite tests
            if Version(AMBRY_SOURCES_VERSION) < Version('0.1.8'):
                self.skipTest('Need ambry_sources >= 0.1.8. Update your installation.')

        library = self._get_library()

        # FIXME: Find the way how to initialize bundle with partitions and drop partition creation.
        bundle = self.setup_bundle(
            'simple', source_url='temp://', build_url='temp://', library=library)
        PartitionFactory._meta.sqlalchemy_session = bundle.dataset.session
        TableFactory._meta.sqlalchemy_session = bundle.dataset.session

        table1 = TableFactory(dataset=bundle.dataset)
        partition1 = PartitionFactory(dataset=bundle.dataset, table=table1, segment=1)
        partition2 = PartitionFactory(dataset=bundle.dataset, table=table1, segment=2)
        bundle.wrap_partition(partition1)
        bundle.wrap_partition(partition2)

        try:
            datafile1 = MPRowsFile(bundle.build_fs, partition1.cache_key)
            datafile1.load_rows(self._get_generator_source())
            partition1._datafile = datafile1

            datafile2 = MPRowsFile(bundle.build_fs, partition2.cache_key)
            datafile2.load_rows(self._get_generator_source(rows=[[3, 3], [4, 4]]))
            partition2._datafile = datafile2

            # Install table
            library.warehouse.install(table1.vid)

            # query all partitions
            rows = library.warehouse.query('SELECT col1, col2 FROM {};'.format(table1.vid))

            # We need to sort rows before check because the order of the table partitions is unknown.
            self.assertEqual(sorted(rows), sorted([(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)]))
        finally:
            library.warehouse.close()
            # FIXME: Use library.warehouse.close() only.
            library.database.close()


class InMemorySQLiteTest(TestBase, Mixin):

    @classmethod
    def get_rc(cls):
        rc = TestBase.get_rc()
        # use file database for library for that test case.
        cls._real_warehouse_database = rc.library.get('warehouse')
        cls._real_test_database = rc.library.database
        rc.library.warehouse = 'sqlite://'
        rc.library.database = 'sqlite://'
        return rc

    def _get_library(self):
        library = self.library()

        # assert it is in-memory database.
        assert library.config.library.warehouse == 'sqlite://'

        return library

    def _assert_is_indexed(self, warehouse, partition, column):
        _assert_sqlite_index(warehouse, partition, column)


class FileSQLiteTest(TestBase, Mixin):

    @classmethod
    def setUpClass(cls):
        TestBase.setUpClass()
        cls._warehouse_db = 'sqlite:////tmp/test-warehouse.db'

    def tearDown(self):
        super(self.__class__, self).tearDown()
        os.remove(self._warehouse_db.replace('sqlite:///', ''))

    @classmethod
    def get_rc(cls):
        rc = TestBase.get_rc()
        # use file database for library for that test case.
        if not rc.library.warehouse == cls._warehouse_db:
            cls._real_warehouse_database = rc.library.database
            rc.library.warehouse = cls._warehouse_db
            rc.library.database = cls._warehouse_db  # It's ok to use the same db file for that test case.
        return rc

    @classmethod
    def tearDownClass(cls):
        rc = TestBase.get_rc()
        if rc.library.database != cls._real_warehouse_database:
            # restore database
            rc.library.database = cls._real_warehouse_database

    def _get_library(self):
        library = self.library()

        # assert it is file database.
        assert library.database.exists()
        return library

    def _assert_is_indexed(self, warehouse, partition, column):
        _assert_sqlite_index(warehouse, partition, column)


class PostgreSQLTest(PostgreSQLTestBase, Mixin):

    @classmethod
    def get_rc(cls):
        rc = TestBase.get_rc()
        # replace database with file database.
        cls._real_warehouse_database = rc.library.database
        rc.library.warehouse = cls.postgres_test_db_data['test_db_dsn']
        rc.library.database = cls.postgres_test_db_data['test_db_dsn']  # It's ok to use the same database.
        return rc

    @classmethod
    def tearDownClass(cls):
        rc = TestBase.get_rc()
        real_warehouse_database = getattr(cls, '_real_warehouse_database', None)
        if real_warehouse_database and rc.library.database != real_warehouse_database:
            # restore database
            rc.library.database = real_warehouse_database
        PostgreSQLTestBase.tearDownClass()

    def _get_library(self):
        library = self.library()

        # assert it is file database.
        assert library.database.exists()
        return library

    def _assert_is_indexed(self, warehouse, partition, column):
        table = postgres_med.table_name(partition.vid) + '_v'
        with warehouse._backend._connection.cursor() as cursor:
            # Sometimes postgres may not use index although index exists.
            # See https://wiki.postgresql.org/wiki/FAQ#Why_are_my_queries_slow.3F_Why_don.27t_they_use_my_indexes.3F
            # and http://stackoverflow.com/questions/9475778/postgresql-query-not-using-index-in-production
            # for details.
            # So, force postgres always to use existing indexes.
            cursor.execute('SET enable_seqscan TO \'off\';')
            query = 'EXPLAIN SELECT * FROM {} WHERE {} > 1 and {} < 3;'.format(table, column, column)
            cursor.execute(query)
            result = cursor.fetchall()
            self.assertIn('Index Scan', result[0][0])


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


def _assert_valid_ambry_sources():
    if Version(AMBRY_SOURCES_VERSION) < Version('0.1.6'):
        raise unittest.SkipTest('Need ambry_sources >= 0.1.6. Update your installation.')


def _assert_sqlite_index(warehouse, partition, column):
    table = sqlite_med.table_name(partition.vid) + '_v'
    cursor = warehouse._backend._connection.cursor()
    query = 'EXPLAIN QUERY PLAN SELECT * FROM {} WHERE {} > 1;'.format(table, column)
    result = cursor.execute(query).fetchall()
    assert 'USING INDEX' in result[0][-1]
