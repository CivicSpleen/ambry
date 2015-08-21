# -*- coding: utf-8 -*-
import unittest
import fudge

import os
import pkgutil

from sqlalchemy.pool import NullPool

from ambry.orm.database import Database
from ambry.orm import database
from ambry.orm import migrations
from ambry.run import get_runconfig

from test.test_base import TestBase, PostgreSQLTestBase


class MigrationTest(TestBase):

    def setUp(self):
        super(self.__class__, self).setUp()
        self.sqlite_db_file = 'test_migration.db'

    def tearDown(self):
        super(self.__class__, self).tearDown()
        if os.path.exists(self.sqlite_db_file):
            os.remove(self.sqlite_db_file)

    def test_validate_migrations_order(self):
        # raises AssertionError if migrations are not ordered properly.
        # This is helper for developer to prevent wrong migrations commits.
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

    @fudge.patch(
        'ambry.orm.database._get_all_migrations')
    def test_applies_new_migration_to_sqlite_database(self, fake_get):
        # replace real migrations with tests migrations.

        test_migrations = [
            (100, 'test.test_orm.functional.migrations.0100_init'),
            (101, 'test.test_orm.functional.migrations.0101_add_column'),
            (102, 'test.test_orm.functional.migrations.0102_create_table'),
            (103, 'test.test_orm.functional.migrations.0103_not_ready')  # that should not apply
        ]

        fake_get.expects_call().returns(test_migrations)

        # create database with initial schema
        with fudge.patched_context(database, 'SCHEMA_VERSION', 100):
            db = Database('sqlite:///{}'.format(self.sqlite_db_file))
            db.create_tables()
            db.close()

        # switch version and reconnect. Now both migrations should apply.
        with fudge.patched_context(database, 'SCHEMA_VERSION', 102):
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

    @fudge.patch(
        'ambry.orm.database._get_all_migrations')
    def test_applies_new_migration_to_postgresql_database(self, fake_get):
        # replace real migrations with tests migrations.
        test_migrations = [
            (100, 'test.test_orm.functional.migrations.0100_init'),
            (101, 'test.test_orm.functional.migrations.0101_add_column'),
            (102, 'test.test_orm.functional.migrations.0102_create_table'),
            (103, 'test.test_orm.functional.migrations.0103_not_ready')  # that should not apply
        ]

        fake_get.expects_call().returns(test_migrations)

        # create postgresql db
        try:
            postgres_test_db_dsn = PostgreSQLTestBase._create_postgres_test_db(get_runconfig())['test_db_dsn']

            # populate database with initial schema
            with fudge.patched_context(database, 'SCHEMA_VERSION', 100):
                db = Database(postgres_test_db_dsn, engine_kwargs={'poolclass': NullPool})
                db.create()
                db.close()

            # switch version and reconnect. Now both migrations should apply.
            with fudge.patched_context(database, 'SCHEMA_VERSION', 102):
                db = Database(postgres_test_db_dsn, engine_kwargs={'poolclass': NullPool})
                try:
                    # check column created by migration 101.
                    db.connection.execute('SELECT column1 FROM datasets;').fetchall()

                    # check table created by migration 102.
                    db.connection.execute('SELECT column1 FROM table1;').fetchall()

                    # db version changed to 102
                    self.assertEqual(
                        db.connection.execute('SELECT version FROM user_version;').fetchone()[0],
                        102)
                finally:
                    db.close()
        finally:
            PostgreSQLTestBase._drop_postgres_test_db()
