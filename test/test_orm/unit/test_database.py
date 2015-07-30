# -*- coding: utf-8 -*-
from urlparse import urlparse
import unittest

from ambry.run import get_runconfig

import fudge

from sqlalchemy import create_engine

from ambry.orm.database import get_stored_version, _validate_version, _migration_required, SCHEMA_VERSION,\
    _update_version, _is_missed, migrate
from ambry.orm.exc import DatabaseError, DatabaseMissingError

MISSING_POSTGRES_CONFIG_MSG = 'PostgreSQL is not configured properly. Add postgresql section to the library section.'


class BaseDatabaseTest(unittest.TestCase):

    def setUp(self):
        conf = get_runconfig()
        if 'postgresql' in conf.dict['library']:
            dsn = conf.dict['library']['postgresql']['database']
            parsed_url = urlparse(dsn)
            db_name = parsed_url.path.replace('/', '')
            self.postgres_dsn = parsed_url._replace(path='postgres').geturl()
            self.postgres_test_db = '{}_'.format(db_name)
            self.postgres_test_dsn = parsed_url._replace(path=self.postgres_test_db).geturl()
        else:
            self.postgres_dsn = None
            self.postgres_test_dsn = None
            self.postgres_test_db = None

    def tearDown(self):
        # drop test database
        if getattr(self, '_active_pg_connection', None):
            self._active_pg_connection.execute('rollback')
            self._active_pg_connection.detach()
            self._active_pg_connection.close()
            self._active_pg_connection = None

            # droop test database;
            engine = create_engine(self.postgres_dsn)
            connection = engine.connect()
            connection.execute('commit')
            connection.execute('DROP DATABASE {};'.format(self.postgres_test_db))
            connection.execute('commit')
            connection.close()

    def pg_connection(self):
        # creates test database and returns postgres connection to that database.
        postgres_user = 'ambry'
        if not self.postgres_dsn:
            raise Exception(MISSING_POSTGRES_CONFIG_MSG)

        # connect to postgres database because we need to create database for tests.
        engine = create_engine(self.postgres_dsn)
        connection = engine.connect()

        # we have to close opened transaction.
        connection.execute('commit')

        # drop test database created by previuos run (control + c case).
        # connection.execute('DROP DATABASE {};'.format(self.postgres_test_db))
        # connection.execute('commit')

        # create test database
        query = 'CREATE DATABASE {} OWNER {} template template0 encoding \'UTF8\';'\
            .format(self.postgres_test_db, postgres_user)
        connection.execute(query)
        connection.execute('commit')
        connection.close()

        # now create connection for tests.
        self.pg_engine = create_engine(self.postgres_test_dsn)
        pg_connection = self.pg_engine.connect()
        self._active_pg_connection = pg_connection
        return pg_connection


class GetVersionTest(BaseDatabaseTest):

    def test_returns_user_version_from_sqlite_pragma(self):
        engine = create_engine('sqlite://')
        connection = engine.connect()
        connection.execute('PRAGMA user_version = 22')
        version = get_stored_version(connection)
        self.assertEquals(version, 22)

    def test_returns_user_version_from_postgres_table(self):
        if not self.postgres_dsn:
            # FIXME: it seems failing is better choice here.
            raise unittest.SkipTest(MISSING_POSTGRES_CONFIG_MSG)

        pg_connection = self.pg_connection()

        create_table_query = '''
            CREATE TABLE user_version (
                version INTEGER NOT NULL); '''

        pg_connection.execute(create_table_query)
        pg_connection.execute('INSERT INTO user_version VALUES (22);')
        pg_connection.execute('commit')
        version = get_stored_version(pg_connection)
        self.assertEquals(version, 22)


class ValidateVersionTest(unittest.TestCase):

    @fudge.patch('ambry.orm.database.get_stored_version')
    def test_raises_database_error_if_db_version_is_between_10_100(self, fake_get):
        fake_get.expects_call().returns(88)
        engine = create_engine('sqlite://')
        connection = engine.connect()
        with self.assertRaises(DatabaseError):
            _validate_version(connection)

    @fudge.patch(
        'ambry.orm.database.get_stored_version',
        'ambry.orm.database._migration_required',
        'ambry.orm.database.migrate')
    def test_runs_migrations(self, fake_get, fake_required, fake_migrate):
        fake_get.expects_call().returns(100)
        fake_required.expects_call().returns(True)
        fake_migrate.expects_call()

        engine = create_engine('sqlite://')
        connection = engine.connect()
        _validate_version(connection)


class MigrationRequiredTest(unittest.TestCase):

    def setUp(self):
        engine = create_engine('sqlite://')
        self.connection = engine.connect()

    @fudge.patch('ambry.orm.database.get_stored_version')
    def test_raises_assertion_error_if_user_has_old_code(self, fake_get):
        fake_get.expects_call().returns(SCHEMA_VERSION + 10)
        with self.assertRaises(AssertionError):
            _migration_required(self.connection)

    @fudge.patch('ambry.orm.database.get_stored_version')
    def test_returns_true_if_stored_version_is_less_than_actual(self, fake_get):
        fake_get.expects_call().returns(SCHEMA_VERSION - 1)
        self.assertTrue(_migration_required(self.connection))

    @fudge.patch('ambry.orm.database.get_stored_version')
    def test_returns_false_if_stored_version_equals_to_actual(self, fake_get):
        fake_get.expects_call().returns(SCHEMA_VERSION)
        self.assertFalse(_migration_required(self.connection))


class UpdateVersionTest(BaseDatabaseTest):

    def setUp(self):
        super(self.__class__, self).setUp()
        engine = create_engine('sqlite://')
        self.sqlite_connection = engine.connect()

    def test_updates_user_version_sqlite_pragma(self):
        _update_version(self.sqlite_connection, 122)
        stored_version = self.sqlite_connection.execute('PRAGMA user_version').fetchone()[0]
        self.assertEquals(stored_version, 122)

    def test_creates_user_version_postgresql_table(self):
        if not self.postgres_dsn:
            # FIXME: it seems failing is better choice here.
            raise unittest.SkipTest(MISSING_POSTGRES_CONFIG_MSG)
        pg_connection = self.pg_connection()
        _update_version(pg_connection, 123)
        version = pg_connection.execute('SELECT version FROM user_version;').fetchone()[0]
        self.assertEquals(version, 123)

    def test_updates_user_version_postgresql_table(self):
        if not self.postgres_dsn:
            # FIXME: it seems failing is better choice here.
            raise unittest.SkipTest(MISSING_POSTGRES_CONFIG_MSG)
        pg_connection = self.pg_connection()
        create_table_query = '''
            CREATE TABLE user_version (
                version INTEGER NOT NULL); '''

        pg_connection.execute(create_table_query)
        pg_connection.execute('INSERT INTO user_version VALUES (22);')
        pg_connection.execute('commit')

        _update_version(pg_connection, 123)
        version = pg_connection.execute('SELECT version FROM user_version;').fetchone()[0]
        self.assertEquals(version, 123)

    def test_raises_DatabaseMissingError_error(self):
        with fudge.patched_context(self.sqlite_connection.engine, 'name', 'foo'):
            with self.assertRaises(DatabaseMissingError):
                _update_version(self.sqlite_connection, 1)


class IsMissedTest(unittest.TestCase):

    def setUp(self):
        engine = create_engine('sqlite://')
        self.connection = engine.connect()

    @fudge.patch(
        'ambry.orm.database.get_stored_version')
    def test_returns_true_if_migration_is_not_applied(self, fake_stored):
        fake_stored.expects_call().returns(1)
        self.assertTrue(_is_missed(self.connection, 2))

    @fudge.patch(
        'ambry.orm.database.get_stored_version')
    def test_returns_false_if_migration_applied(self, fake_stored):
        fake_stored.expects_call().returns(2)
        self.assertFalse(_is_missed(self.connection, 2))


class MigrateTest(unittest.TestCase):

    def setUp(self):
        engine = create_engine('sqlite://')
        self.connection = engine.connect()

    @fudge.patch(
        'ambry.orm.database._is_missed',
        'ambry.orm.migrations.0100_init.Migration.migrate')
    def test_runs_missed_migration_and_changes_version(self, fake_is_missed, fake_migrate):
        fake_is_missed.expects_call().returns(True)
        fake_migrate.expects_call()
        migrate(self.connection)
        stored_version = self.connection.execute('PRAGMA user_version').fetchone()[0]
        self.assertEquals(stored_version, 100)

    @fudge.patch(
        'ambry.orm.database._is_missed',
        'ambry.orm.migrations.0100_init.Migration.migrate')
    def test_does_not_change_version_if_migration_failed(self, fake_is_missed, fake_migrate):
        fake_is_missed.expects_call().returns(True)
        fake_migrate.expects_call().raises(Exception('My fake exception'))
        self.connection.execute('PRAGMA user_version = 22')
        with self.assertRaises(Exception):
            migrate(self.connection)
        stored_version = self.connection.execute('PRAGMA user_version').fetchone()[0]
        self.assertEquals(stored_version, 22)
