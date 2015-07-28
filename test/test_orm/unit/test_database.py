# -*- coding: utf-8 -*-
import unittest

import fudge

from sqlalchemy import create_engine

from ambry.orm.database import get_stored_version, _validate_version, _migration_required, SCHEMA_VERSION,\
    _update_version, _is_missed, migrate
from ambry.orm.exc import DatabaseError, DatabaseMissingError


class GetVersionTest(unittest.TestCase):
    def test_returns_user_version_from_sqlite_pragma(self):
        engine = create_engine('sqlite://')
        connection = engine.connect()
        connection.execute('PRAGMA user_version = 22')
        version = get_stored_version(connection)
        self.assertEquals(version, 22)

    def test_returns_user_version_from_postgres_table(self):
        # FIXME:
        pass


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


class UpdateVersionTest(unittest.TestCase):

    def setUp(self):
        engine = create_engine('sqlite://')
        self.connection = engine.connect()

    def test_updates_user_version_sqlite_pragma(self):
        _update_version(self.connection, 122)
        stored_version = self.connection.execute('PRAGMA user_version').fetchone()[0]
        self.assertEquals(stored_version, 122)

    def test_updates_user_version_postgresql_table(self):
        # FIXME:
        pass

    def test_raises_DatabaseMissingError_error(self):
        with fudge.patched_context(self.connection.engine, 'driver', 'foo'):
            with self.assertRaises(DatabaseMissingError):
                _update_version(self.connection, 1)


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
