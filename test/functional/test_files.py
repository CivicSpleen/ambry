# -*- coding: utf-8 -*-
import os

from pysqlite2.dbapi2 import OperationalError

from fs.opener import fsopendir

from ambry_sources.med.sqlite import install_mpr_module

from test.test_base import TestBase


class Test(TestBase):

    @classmethod
    def setUpClass(cls):
        TestBase.setUpClass()
        cls._db = 'sqlite:////tmp/test-files.db'

    @classmethod
    def get_rc(cls):
        rc = TestBase.get_rc()
        rc.library.database = cls._db
        return rc

    def tearDown(self):
        super(self.__class__, self).tearDown()
        os.remove(self._db.replace('sqlite:///', ''))

    def test_sql_create_table(self):
        """ Tests table creation from sql file. """
        library = self.library()

        # First load 'simple' dataset because simple_with_sql dataset requires it.
        test_root = fsopendir('temp://')
        test_root.makedir('build')
        test_root.makedir('source')
        test_root.makedir('build1')
        test_root.makedir('source1')
        simple_bundle = self.setup_bundle(
            'simple', source_url=test_root.getsyspath('source'),
            build_url=test_root.getsyspath('build'), library=library)

        simple_bundle.sync_in()
        simple_bundle.ingest()
        simple_bundle.dest_schema()
        simple_bundle.build()

        # now load simple_with_sql bundle.
        sql_bundle = self.setup_bundle(
            'simple_with_sql', source_url=test_root.getsyspath('source1'),
            build_url=test_root.getsyspath('build1'), library=library)
        # load files to library database (as File records)
        sql_bundle.sync_in()
        # Content of the File record should match to bundle.sql file content.
        file_record = [x for x in sql_bundle.dataset.files if x.path == 'bundle.sql'][0]
        self.assertEqual(
            file_record.unpacked_contents,
            sql_bundle._source_fs.getcontents('bundle.sql'))

        # create mpr
        sql_bundle.ingest()

        # create schema of the tables
        sql_bundle.dest_schema()

        # now build - this should create view from the sql and load source with data from the view.
        sql_bundle.build()

        # Assert table created.
        #

        try:
            table_rows = library.database.connection.execute('SELECT col1, col2 FROM table1;').fetchall()
            self.assertEqual(table_rows, [])
        except OperationalError as exc:
            if 'no such table' in str(exc):
                raise AssertionError('table1 was not created.')
            else:
                raise

        # Assert table for source created.
        # FIXME:

    # FIXME: Add slow mark.
    def test_view_from_sql(self):
        """ Tests view creation from sql file. """
        library = self.library()

        # First load 'simple' dataset because simple_with_sql dataset requires it.
        test_root = fsopendir('temp://')
        test_root.makedir('build')
        test_root.makedir('source')
        test_root.makedir('build1')
        test_root.makedir('source1')
        simple_bundle = self.setup_bundle(
            'simple', source_url=test_root.getsyspath('source'),
            build_url=test_root.getsyspath('build'), library=library)

        simple_bundle.sync_in()
        simple_bundle.ingest()
        simple_bundle.dest_schema()
        simple_bundle.build()

        # now load simple_with_sql bundle.
        sql_bundle = self.setup_bundle(
            'simple_with_sql', source_url=test_root.getsyspath('source1'),
            build_url=test_root.getsyspath('build1'), library=library)
        # load files to library database (as File records)
        sql_bundle.sync_in()
        # Content of the File record should match to bundle.sql file content.
        file_record = [x for x in sql_bundle.dataset.files if x.path == 'bundle.sql'][0]
        self.assertEqual(
            file_record.unpacked_contents,
            sql_bundle._source_fs.getcontents('bundle.sql'))

        # create mpr
        sql_bundle.ingest()

        # create schema of the tables
        sql_bundle.dest_schema()

        # now build - this should create view from the sql and load source with data from the view.
        sql_bundle.build()

        # Assert view created.
        #

        # keep apsw imports here to prevent break if apsw is not installed.
        import apsw
        from apsw import SQLError
        # FIXME: check version of the ambry_sources.
        connection = None
        try:
            # We have to use apsw because pysqlite does not supper virtual tables.
            dsn = library.database.dsn.replace('sqlite:///', '')
            connection = apsw.Connection(dsn)
            # add mod_partition to allow query on mpr
            install_mpr_module(connection)
            cursor = connection.cursor()
            rows_from_view = cursor.execute('SELECT s1_id, s2_id FROM view1;').fetchall()
            self.assertEqual(rows_from_view, [(1, 1)])
        except SQLError as exc:
            if 'no such table' in str(exc):
                raise AssertionError('view1 view was not created.')
            else:
                raise
        finally:
            if connection:
                connection.close()

        # Source from view referenced in sources.csv of 'simple_with_sql' bundle created.
        # FIXME:

    def _test_sql_create_materialized_view(self):
        """ Tests materialized view creation from sql file. """
        # FIXME: Not ready.
        library = self.library()

        # First load 'simple' dataset because simple_with_sql dataset requires it.
        test_root = fsopendir('temp://')
        test_root.makedir('build')
        test_root.makedir('source')
        test_root.makedir('build1')
        test_root.makedir('source1')
        simple_bundle = self.setup_bundle(
            'simple', source_url=test_root.getsyspath('source'),
            build_url=test_root.getsyspath('build'), library=library)

        simple_bundle.sync_in()
        simple_bundle.ingest()
        simple_bundle.dest_schema()
        simple_bundle.build()

        # now load simple_with_sql bundle.
        sql_bundle = self.setup_bundle(
            'simple_with_sql', source_url=test_root.getsyspath('source1'),
            build_url=test_root.getsyspath('build1'), library=library)
        # load files to library database (as File records)
        sql_bundle.sync_in()
        # Content of the File record should match to bundle.sql file content.
        file_record = [x for x in sql_bundle.dataset.files if x.path == 'bundle.sql'][0]
        self.assertEqual(
            file_record.unpacked_contents,
            sql_bundle._source_fs.getcontents('bundle.sql'))

        # create mpr
        sql_bundle.ingest()

        # create schema of the tables
        sql_bundle.dest_schema()

        # now build - this should create view from the sql and load source with data from the view.
        sql_bundle.build()

        # Assert view created.
        #

        try:
            table_rows = library.database.connection\
                .execute('SELECT s1_id, s2_id FROM materialized_view1;')\
                .fetchall()
            self.assertEqual(table_rows, [])
        except OperationalError as exc:
            if 'no such table' in str(exc):
                raise AssertionError('table1 was not created.')
            else:
                raise

        # Assert table for source created.
        # FIXME:
