# -*- coding: utf-8 -*-
import os

import pytest

import apsw
from apsw import SQLError

from pysqlite2.dbapi2 import OperationalError

from fs.opener import fsopendir

try:
    from ambry_sources.med.sqlite import install_mpr_module
except ImportError as exc:
    from semantic_version import Version
    import ambry_sources
    ambry_sources_version = getattr(ambry_sources, '__version__', None) or ambry_sources.__meta__.__version__
    if Version(ambry_sources_version) < Version('0.1.10'):
        raise ImportError(
            '{}. You need to update ambry_sources to >= 0.1.10.'
            .format(exc))
    else:
        raise

from ambry.library.warehouse.backends.postgresql import PostgreSQLBackend

from test.test_base import TestBase
from test.helpers import assert_sqlite_index


class InspectorBase(object):
    """ Set of asserts to test database state. """

    @classmethod
    def assert_sql_saved(cls, bundle):
        """ Finds file record in the library and matches it agains bundle.sql content. """
        # Content of the File record should match to bundle.sql file content.
        file_record = [x for x in bundle.dataset.files if x.path == 'bundle.sql'][0]
        assert file_record.unpacked_contents == bundle._source_fs.getcontents('bundle.sql')

    @classmethod
    def assert_table_created(cls, library, table):
        raise NotImplementedError

    @classmethod
    def assert_view_created(cls, library, view):
        raise NotImplementedError

    @classmethod
    def assert_materialized_view_created(cls, library, mat_view):
        raise NotImplementedError

    @classmethod
    def assert_index(cls, library, partition, column):
        raise NotImplementedError


class SQLiteInspector(InspectorBase):
    """ Set of asserts to test sqlite state. """

    @classmethod
    def assert_table_created(cls, library, table):
        """ Looks for given table in the library. If not found raises AssertionError. """
        try:
            table_rows = library.database.connection.execute('SELECT col1, col2 FROM table1;').fetchall()
            assert table_rows == []
        except OperationalError as exc:
            if 'no such table' in str(exc):
                raise AssertionError('table1 was not created.')
            else:
                raise

    @classmethod
    def assert_view_created(cls, library, view):
        """ Looks for given view in the library. If not found raises AssertionError. """

        # keep apsw imports here to prevent break if apsw is not installed.
        connection = None
        try:
            # We have to use apsw because pysqlite does not support virtual tables.
            dsn = library.database.dsn.replace('sqlite:///', '')
            connection = apsw.Connection(dsn)

            # add mod_partition to allow query on mpr through view.
            install_mpr_module(connection)

            # get data from mpr through view.
            cursor = connection.cursor()
            rows_from_view = cursor.execute('SELECT s1_id, s2_id FROM {};'.format(view)).fetchall()
            assert rows_from_view == [(1, 1)]
        except SQLError as exc:
            if 'no such table' in str(exc):
                raise AssertionError('{} view was not created.'.format(view))
            else:
                raise
        finally:
            if connection:
                connection.close()

    @classmethod
    def assert_materialized_view_created(cls, library, view):
        """ Looks for given materialied view in the library. If not found or is not materialized
            raises AssertionError.
        """
        try:
            table_rows = library.database.connection\
                .execute('SELECT s1_id, s2_id FROM {};'.format(view))\
                .fetchall()
            assert table_rows == [(1, 1)]
        except OperationalError as exc:
            if 'no such table' in str(exc):
                raise AssertionError('{} materialized view was not created.'.format(view))
            else:
                raise

    @classmethod
    def assert_index(cls, library, partition, column):
        assert_sqlite_index(library.database.engine.raw_connection(), partition, 'id')


class PostgreSQLInspector(InspectorBase):
    """ Set of asserts to test postgresql state. """

    @classmethod
    def assert_table_created(cls, library, table):
        relation = '{}.{}'.format('ambrylib', table)
        assert PostgreSQLBackend._relation_exists(library.database.engine.raw_connection(), relation)


class BundleSQLTest(TestBase):
    """ Test bundle.sql handling. """

    def setUp(self):
        super(BundleSQLTest, self).setUp()
        if self.dbname == 'sqlite':
            self.inspector = SQLiteInspector
        elif self.dbname == 'postgres':
            self.inspector = PostgreSQLInspector
        else:
            raise Exception('Do not know inspector for {} database.'.format(self.dbname))

    @pytest.mark.slow
    def test_bundle_sql(self):
        """ Tests view creation from sql file. """

        library = self.library()
        if not library.database.exists():
            raise Exception(
                'The test requires file database. {} looks like in-memory db.'
                .format(library.database.dsn))

        # First load 'simple' dataset because simple_with_sql dataset requires it.
        test_root = fsopendir('temp://')
        test_root.makedir('build')
        test_root.makedir('source')
        simple_bundle = self.setup_bundle(
            'simple', source_url=test_root.getsyspath('source'),
            build_url=test_root.getsyspath('build'), library=library)

        # simple_bundle.run(force=True)
        simple_bundle.sync_in()
        simple_bundle.ingest(force=True)
        simple_bundle.schema()
        simple_bundle.build()

        # now load simple_with_sql bundle.
        test_root.makedir('build1')
        test_root.makedir('source1')
        sql_bundle = self.setup_bundle(
            'simple_with_sql', source_url=test_root.getsyspath('source1'),
            build_url=test_root.getsyspath('build1'), library=library)

        # load files to library database (as File records)
        sql_bundle.sync_in()

        # create mpr file with source rows.
        sql_bundle.ingest(force=True)

        # create schema of the tables
        sql_bundle.schema()

        # now build - this should create table, view, materialized view and indexes from the sql
        # and load source with data from the view.
        sql_bundle.build()

        # check the final state.
        self.inspector.assert_sql_saved(sql_bundle)
        self.inspector.assert_table_created(library, 'table1')
        #self.inspector.assert_view_created(library, 'view1')
        #self.inspector.assert_materialized_view_created(library, 'materialized_view1')
        #self.inspector.assert_index(
        #    library,
        #    simple_bundle.partition('example.com-simple-simple'),
        #    'id')
