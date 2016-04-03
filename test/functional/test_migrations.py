# -*- coding: utf-8 -*-

import pkgutil
import os
import unittest

from sqlalchemy.pool import NullPool

try:
    # py2, mock is external lib.
    from mock import patch
except ImportError:
    # py3, mock is included
    from unittest.mock import patch

from ambry.orm import database, migrations
from ambry.orm.database import Database, POSTGRES_SCHEMA_NAME

from test.proto import TestBase


class MigrationTest(TestBase):

    def setUp(self):
        super(self.__class__, self).setUp()
        # TODO (kazbek): Use library.database instead of own sqlite file.
        self.sqlite_db_file = '/tmp/test_migration.db'

    def tearDown(self):
        super(self.__class__, self).tearDown()
        if os.path.exists(self.sqlite_db_file):
            os.remove(self.sqlite_db_file)

    def test_validate_migrations_order(self):
        # raises AssertionError if migrations are not ordered properly.
        # This prevents developers from wrong migrations commits.
        prefix = migrations.__name__ + '.'
        all_migrations = []
        for importer, modname, ispkg in pkgutil.iter_modules(migrations.__path__, prefix):
            version = int(modname.split('.')[-1].split('_')[0])
            all_migrations.append((version, modname))

        if len(all_migrations) <= 1:
            raise unittest.SkipTest('There are no enough migrations to validate ordering.')

        for i in range(1, len(all_migrations)):
            previous = all_migrations[i - 1]
            current = all_migrations[i]
            error_msg = '{} migration number violates ordering. Expecting {} migration. '\
                'See migrations documentation for details.'.format(current, previous[0] + 1)
            self.assertEqual(current[0] - previous[0], 1, error_msg)

    @patch('ambry.orm.database._get_all_migrations')
    def test_applies_new_migration_to_sqlite_database(self, fake_get):
        if self._db_type != 'sqlite':
            self.skipTest('SQLite tests are disabled.')

        # replace real migrations with tests migrations.

        test_migrations = [
            (100, 'test.functional.migrations.0100_init'),
            (101, 'test.functional.migrations.0101_add_column'),
            (102, 'test.functional.migrations.0102_create_table'),
            (103, 'test.functional.migrations.0103_not_ready')  # that should not apply
        ]

        fake_get.return_value = test_migrations

        # create database with initial schema
        with patch.object(database, 'SCHEMA_VERSION', 100):
            db = Database('sqlite:///{}'.format(self.sqlite_db_file))
            db.create_tables()
            db.close()

        # switch version and reconnect. Now both migrations should apply.
        with patch.object(database, 'SCHEMA_VERSION', 102):
            db = Database('sqlite:///{}'.format(self.sqlite_db_file))
            try:
                # check column created by migration 101.
                db.connection.execute('SELECT column1 FROM datasets;').fetchall()

                # check table created by migration 102.
                db.connection.execute('SELECT column1 FROM table1;').fetchall()

                # db version changed to 102
                self.assertEqual(db.connection.execute('PRAGMA user_version').fetchone()[0], 102)
            finally:
                db.close()

    @patch('ambry.orm.database._get_all_migrations')
    def test_applies_new_migration_to_postgresql_database(self, fake_get):
        if self._db_type != 'postgres':
            self.skipTest('Postgres tests are disabled.')
        # replace real migrations with tests migrations.
        test_migrations = [
            (100, 'test.test_orm.functional.migrations.0100_init'),
            (101, 'test.test_orm.functional.migrations.0101_add_column'),
            (102, 'test.test_orm.functional.migrations.0102_create_table'),
            (103, 'test.test_orm.functional.migrations.0103_not_ready')  # that should not apply
        ]

        fake_get.return_value = test_migrations

        # create postgresql db
        postgres_test_db_dsn = self.config.library.database
        # PostgreSQLTestBase._create_postgres_test_db(get_runconfig())['test_db_dsn']

        # populate database with initial schema
        with patch.object(database, 'SCHEMA_VERSION', 100):
            db = Database(postgres_test_db_dsn, engine_kwargs={'poolclass': NullPool})
            db.create()
            db.close()

        # switch version and reconnect. Now both migrations should apply.
        with patch.object(database, 'SCHEMA_VERSION', 102):
            db = Database(postgres_test_db_dsn, engine_kwargs={'poolclass': NullPool})
            try:
                # check column created by migration 101.
                db.connection\
                    .execute('SELECT column1 FROM {}.datasets;'.format(POSTGRES_SCHEMA_NAME))\
                    .fetchall()

                # check table created by migration 102.
                db.connection\
                    .execute('SELECT column1 FROM {}.table1;'.format(POSTGRES_SCHEMA_NAME))\
                    .fetchall()

                # db version changed to 102
                db_version = db.connection\
                    .execute('SELECT version FROM {}.user_version;'.format(POSTGRES_SCHEMA_NAME))\
                    .fetchone()[0]
                self.assertEqual(db_version, 102)
            finally:
                db.close()
