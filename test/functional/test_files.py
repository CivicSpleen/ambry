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

from test.test_base import TestBase


class Test(TestBase):

    @classmethod
    def setUpClass(cls):
        TestBase.setUpClass()
        cls._db = 'sqlite:////tmp/test-files-ambry-1.db'
        try:
            os.remove(cls._db.replace('sqlite:///', ''))
        except OSError:
            pass

    def tearDown(self):
        super(self.__class__, self).tearDown()
        try:
            os.remove(self._db.replace('sqlite:///', ''))
        except OSError:
            pass

    @pytest.mark.slow
    def test_bundle_sql(self):
        """ Tests view creation from sql file. """
        # Replace config because user may set memory database in the config, but test requires file database.
        rc = TestBase.get_rc()
        rc.library.database = self._db

        library = self.library(config=rc)

        # First load 'simple' dataset because simple_with_sql dataset requires it.
        test_root = fsopendir('temp://')
        test_root.makedir('build')
        test_root.makedir('source')
        simple_bundle = self.setup_bundle(
            'simple', source_url=test_root.getsyspath('source'),
            build_url=test_root.getsyspath('build'), library=library)

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
        self._assert_sql_saved(sql_bundle)
        self._assert_table_created(library, 'table1')
        self._assert_view_created(library, 'view1')
        self._assert_materialized_view_created(library, 'materialized_view1')

    def _assert_table_created(self, library, table):
        """ Looks for given table in the library. If not found raises AssertionError. """
        try:
            table_rows = library.database.connection.execute('SELECT col1, col2 FROM table1;').fetchall()
            self.assertEqual(table_rows, [])
        except OperationalError as exc:
            if 'no such table' in str(exc):
                raise AssertionError('table1 was not created.')
            else:
                raise

    def _assert_view_created(self, library, view):
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
            self.assertEqual(rows_from_view, [(1, 1)])
        except SQLError as exc:
            if 'no such table' in str(exc):
                raise AssertionError('{} view was not created.'.format(view))
            else:
                raise
        finally:
            if connection:
                connection.close()

    def _assert_materialized_view_created(self, library, view):
        """ Looks for given materialied view in the library. If not found or is not materialized
            raises AssertionError.
        """
        try:
            table_rows = library.database.connection\
                .execute('SELECT s1_id, s2_id FROM {};'.format(view))\
                .fetchall()
            self.assertEqual(table_rows, [(1, 1)])
        except OperationalError as exc:
            if 'no such table' in str(exc):
                raise AssertionError('{} materialized view was not created.'.format(view))
            else:
                raise

    def _assert_sql_saved(self, bundle):
        """ Finds file record in the library and matches it agains bundle.sql content. """
        # Content of the File record should match to bundle.sql file content.
        file_record = [x for x in bundle.dataset.files if x.path == 'bundle.sql'][0]
        self.assertEqual(
            file_record.unpacked_contents,
            bundle._source_fs.getcontents('bundle.sql'))
