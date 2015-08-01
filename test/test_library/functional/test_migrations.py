# -*- coding: utf-8 -*-
import unittest
import fudge

import os
import pkgutil

from ambry.orm.database import Database
from ambry.orm import database
from ambry.orm import migrations


class MigrationTest(unittest.TestCase):

    def setUp(self):
        self.sqlite_db_file = 'test_migration.db'

    def tearDown(self):
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
            self.assertEquals(current[0] - previous[0], 1, error_msg)

    @fudge.patch(
        'ambry.orm.database._get_all_migrations')
    def test_apllies_new_migration_to_sqlite_database(self, fake_get):
        # replace real migrations with tests migrations.

        test_migrations = [
            (100, 'test.test_library.functional.migrations.0100_init'),
            (101, 'test.test_library.functional.migrations.0101_add_column'),
            (102, 'test.test_library.functional.migrations.0102_create_table'),
            (103, 'test.test_library.functional.migrations.0103_not_ready')  # that should not apply
        ]

        fake_get.expects_call().returns(test_migrations)

        # create database with initial schema
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
                self.assertEquals(db.connection.execute('PRAGMA user_version').fetchone()[0], 102)
            finally:
                db.close()

    def test_applies_new_migration_to_postgresql_database(self):
        pass
