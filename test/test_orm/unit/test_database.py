# -*- coding: utf-8 -*-
import os
import unittest

import fudge

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection as SQLAlchemyConnection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import ProgrammingError, OperationalError


from ambry.orm.database import get_stored_version, _validate_version, _migration_required, SCHEMA_VERSION,\
    _update_version, _is_missed, migrate
from ambry.orm.exc import DatabaseError, DatabaseMissingError

from ambry.orm.database import Database, ROOT_CONFIG_NAME_V, ROOT_CONFIG_NAME
from ambry.orm.dataset import Dataset

from test.test_orm.factories import DatasetFactory, TableFactory,\
    ColumnFactory, PartitionFactory

from test.test_library.asserts import assert_spec

from test.test_orm.base import BasePostgreSQLTest, MISSING_POSTGRES_CONFIG_MSG


class DatabaseTest(unittest.TestCase):

    def setUp(self):
        self.sqlite_db = Database('sqlite://')
        self.sqlite_db.create()
        fudge.clear_expectations()
        fudge.clear_calls()

    def tearDown(self):
        fudge.clear_expectations()
        fudge.clear_calls()

    # helpers
    def _assert_exists(self, model_class, **filter_kwargs):
        query = self.query(model_class)\
            .filter_by(**filter_kwargs)
        assert query.first() is not None

    def _assert_does_not_exist(self, model_class, **filter_kwargs):
        query = self.query(model_class)\
            .filter_by(**filter_kwargs)
        assert query.first() is None

    def test_initializes_path_and_driver(self):
        dsn = 'postgresql+psycopg2://ambry:secret@127.0.0.1/exampledb'
        db = Database(dsn)
        self.assertEqual(db.path, '/exampledb')
        self.assertEqual(db.driver, 'postgresql+psycopg2')

    # .create tests
    def test_creates_new_database(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(self.sqlite_db._create_path, ['self'])
        assert_spec(self.sqlite_db.exists, ['self'])
        assert_spec(self.sqlite_db.create_tables, ['self'])
        assert_spec(self.sqlite_db._add_config_root, ['self'])

        # prepare state
        self.sqlite_db.exists = fudge.Fake('exists').expects_call().returns(False)
        self.sqlite_db._create_path = fudge.Fake('_create_path').expects_call()
        self.sqlite_db.create_tables = fudge.Fake('create_tables').expects_call()
        ret = self.sqlite_db.create()
        self.assertTrue(ret)
        fudge.verify()

    def test_returns_false_if_database_exists(self):

        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(self.sqlite_db.exists, ['self'])

        # prepare state
        with fudge.patched_context(self.sqlite_db, 'exists', fudge.Fake('exists').expects_call().returns(True)):
            ret = self.sqlite_db.create()
            self.assertFalse(ret)
        fudge.verify()

    # ._create_path tests
    def test_makes_database_directory(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(os.makedirs, ['name', 'mode'])
        assert_spec(os.path.exists, ['path'])

        # prepare state
        fake_makedirs = fudge.Fake('makedirs').expects_call()
        fake_exists = fudge.Fake('exists')\
            .expects_call()\
            .returns(False)\
            .next_call()\
            .returns(True)

        # test
        with fudge.patched_context(os, 'makedirs', fake_makedirs):
            with fudge.patched_context(os.path, 'exists', fake_exists):
                db = Database('sqlite:///test_database1.db')
                db._create_path()
        fudge.verify()

    def test_ignores_exception_if_makedirs_failed(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(os.makedirs, ['name', 'mode'])

        fake_makedirs = fudge.Fake('makedirs')\
            .expects_call()\
            .raises(Exception('My fake exception'))

        fake_exists = fudge.Fake('exists')\
            .expects_call()\
            .returns(False)\
            .next_call()\
            .returns(True)

        # test
        with fudge.patched_context(os, 'makedirs', fake_makedirs):
            with fudge.patched_context(os.path, 'exists', fake_exists):
                db = Database('sqlite:///test_database1.db')
                db._create_path()
        fudge.verify()

    def test_raises_exception_if_dir_does_not_exists_after_creation_attempt(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(os.makedirs, ['name', 'mode'])
        assert_spec(os.path.exists, ['path'])

        # prepare state
        fake_makedirs = fudge.Fake('makedirs')\
            .expects_call()

        fake_exists = fudge.Fake('exists')\
            .expects_call()\
            .returns(False)\
            .next_call()\
            .returns(False)

        # test
        with fudge.patched_context(os, 'makedirs', fake_makedirs):
            with fudge.patched_context(os.path, 'exists', fake_exists):
                try:
                    db = Database('sqlite:///test_database1.db')
                    db._create_path()
                except Exception as exc:
                    self.assertIn('Couldn\'t create directory', exc.message)
        fudge.verify()

    # .exists tests
    def test_sqlite_database_does_not_exists_if_file_not_found(self):
        db = Database('sqlite://no-such-file.db')
        self.assertFalse(db.exists())

    def test_returns_false_if_datasets_table_does_not_exist(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(self.sqlite_db.connection.execute, ['self', 'object'])

        # prepare state
        statement = 'select 1'
        params = []

        self.sqlite_db.connection.execute = fudge.Fake()\
            .expects_call()\
            .raises(ProgrammingError(statement, params, 'orig'))

        # testing.
        ret = self.sqlite_db.exists()
        self.assertFalse(ret)

    @fudge.patch(
        'ambry.orm.database.os.path.exists')
    def test_returns_true_if_root_config_dataset_exists(self, fake_path_exists):
        # prepare state
        fake_path_exists.expects_call().returns(True)

        # testing.
        self.assertTrue(self.sqlite_db.exists())

    def test_returns_false_if_root_config_dataset_does_not_exist(self):
        self.sqlite_db.connection.execute('DELETE FROM datasets;')
        self.sqlite_db.commit()
        query = "SELECT * FROM datasets WHERE d_vid = '{}' ".format(ROOT_CONFIG_NAME_V)
        self.assertIsNone(self.sqlite_db.connection.execute(query).fetchone())
        self.assertFalse(self.sqlite_db.exists())

    # engine() tests.
    @fudge.patch(
        'ambry.orm.database.create_engine',
        'ambry.orm.database._validate_version')
    def test_engine_creates_and_caches_sqlalchemy_engine(self, fake_create, fake_validate):
        fake_validate.expects_call()
        engine_stub = fudge.Fake().is_a_stub()
        fake_create.expects_call()\
            .returns(engine_stub)
        db = Database('sqlite://')
        self.assertEqual(db.engine, engine_stub)
        self.assertEqual(db._engine, engine_stub)

    @fudge.patch(
        'ambry.orm.database.create_engine',
        'ambry.orm.database.event',
        'ambry.orm.database._validate_version')
    def test_listens_to_connect_signal_for_sqlite_driver(self, fake_create,
                                                         fake_event, fake_validate):
        fake_validate.expects_call()
        fake_event.provides('listen')
        engine_stub = fudge.Fake().is_a_stub()
        fake_create.expects_call()\
            .returns(engine_stub)
        Database('sqlite://').engine

    # connection tests.
    def test_connection_creates_and_caches_new_sqlalchemy_connection(self):
        db = Database('sqlite://')
        self.assertIsInstance(db.connection, SQLAlchemyConnection)
        self.assertIsInstance(db._connection, SQLAlchemyConnection)

    def test_connection_sets_search_path_to_library_for_postgres(self):
        fake_connection = fudge.Fake('connection')\
            .provides('execute')\
            .with_args('SET search_path TO library')\
            .expects_call()

        fake_engine = fudge.Fake()\
            .provides('connect')\
            .returns(fake_connection)

        dsn = 'postgresql+psycopg2://ambry:secret@127.0.0.1/exampledb'
        db = Database(dsn)
        db._engine = fake_engine
        self.assertEqual(db.connection, fake_connection)

    def test_connection_sets_path_to_library_for_postgis(self):
        fake_connection = fudge.Fake('connection')\
            .provides('execute')\
            .with_args('SET search_path TO library')\
            .expects_call()

        fake_engine = fudge.Fake()\
            .provides('connect')\
            .returns(fake_connection)

        dsn = 'postgis+psycopg2://ambry:secret@127.0.0.1/exampledb'
        db = Database(dsn)
        db._engine = fake_engine
        self.assertEqual(db.connection, fake_connection)

    # .session tests # FIXME:

    # .open tests # FIXME:

    # .close tests
    def test_closes_session_and_connection(self):
        db = Database('sqlite://')
        with fudge.patched_context(db.session, 'close', fudge.Fake('session.close').expects_call()):
            with fudge.patched_context(db.connection, 'close', fudge.Fake('connection.close').expects_call()):
                db.close()
        fudge.verify()
        self.assertIsNone(db._session)
        self.assertIsNone(db._connection)

    # .commit tests
    def test_commit_commits_session(self):
        db = Database('sqlite://')

        # init engine and session
        db.session

        with fudge.patched_context(db._session, 'commit', fudge.Fake().expects_call()):
            db.commit()
        fudge.verify()

    def test_commit_raises_session_commit_exception(self):
        db = Database('sqlite://')

        # init engine and session
        db.session

        with fudge.patched_context(db._session, 'commit', fudge.Fake().expects_call().raises(ValueError)):
            with self.assertRaises(ValueError):
                db.commit()

        fudge.verify()

    # .rollback tests
    def test_rollbacks_session(self):
        db = Database('sqlite://')

        # init engine and session
        db.session

        with fudge.patched_context(db._session, 'rollback', fudge.Fake('session.rollback').expects_call()):
            db.rollback()
        fudge.verify()

    # .clean tests FIXME:

    # .drop tests FIXME:

    # .metadata tests FIXME:

    # .inspector tests
    def test_contains_engine_inspector(self):
        db = Database('sqlite://')
        self.assertIsInstance(db.inspector, Inspector)
        self.assertEqual(db.engine, db.inspector.engine)

    # .clone tests
    def test_clone_returns_new_instance(self):
        db = Database('sqlite://')
        new_db = db.clone()
        self.assertNotEqual(db, new_db)
        self.assertEqual(db.dsn, new_db.dsn)

    # .create_tables test
    def test_ignores_OperationalError_while_droping(self):
        db = Database('sqlite://')
        fake_drop = fudge.Fake()\
            .expects_call()\
            .raises(OperationalError('select 1;', [], 'a'))
        with fudge.patched_context(db, 'drop', fake_drop):
            db.create_tables()

    def test_creates_dataset_table(self):
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session

        # Now all tables are created. Can we use ORM to create datasets?
        DatasetFactory()
        self.sqlite_db.commit()

    def test_creates_table_table(self):
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        TableFactory._meta.sqlalchemy_session = self.sqlite_db.session

        # Now all tables are created. Can we use ORM to create datasets?
        ds1 = DatasetFactory()
        self.sqlite_db.commit()
        TableFactory(dataset=ds1)
        self.sqlite_db.commit()

    def test_creates_column_table(self):
        ColumnFactory._meta.sqlalchemy_session = self.sqlite_db.session

        # Now all tables are created. Can we use ORM to create columns?

        ColumnFactory()
        self.sqlite_db.commit()

    def test_creates_partition_table(self):
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        PartitionFactory._meta.sqlalchemy_session = self.sqlite_db.session

        ds1 = DatasetFactory()
        PartitionFactory(dataset=ds1)
        self.sqlite_db.commit()

    # ._add_config_root tests
    def test_creates_new_root_config(self):
        # prepare state
        db = Database('sqlite://')
        db.enable_delete = True

        # prevent _add_root_config call from create_tables
        with fudge.patched_context(db, '_add_config_root', fudge.Fake().is_a_stub()):
            db.create_tables()
        query = db.session.query
        datasets = query(Dataset).all()
        self.assertEqual(len(datasets), 0)

        # testing
        # No call real _add_config_root and check result of the call.
        db._add_config_root()
        datasets = query(Dataset).all()
        self.assertEqual(len(datasets), 1)
        self.assertEqual(datasets[0].name, ROOT_CONFIG_NAME)
        self.assertEqual(datasets[0].vname, ROOT_CONFIG_NAME_V)

    def test_closes_session_if_root_config_exists(self):

        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(self.sqlite_db.close_session, ['self'])

        # testing
        with fudge.patched_context(self.sqlite_db, 'close_session', fudge.Fake('close_session').expects_call()):
            self.sqlite_db._add_config_root()
        fudge.verify()

    # .new_dataset test FIXME:

    # .root_dataset tests FIXME:

    # .dataset tests FIXME:

    # .datasets tests
    def test_returns_list_with_all_datasets(self):
        # prepare state
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session

        ds1 = DatasetFactory()
        ds2 = DatasetFactory()
        ds3 = DatasetFactory()

        # testing
        ret = self.sqlite_db.datasets
        self.assertIsInstance(ret, list)
        self.assertEqual(len(ret), 3)
        self.assertIn(ds1, ret)
        self.assertIn(ds2, ret)
        self.assertIn(ds3, ret)

    # .remove_dataset test
    def test_removes_dataset(self):

        # prepare state.
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        ds1 = DatasetFactory()
        self.sqlite_db.session.commit()
        ds1_vid = ds1.vid

        # testing
        self.sqlite_db.remove_dataset(ds1)
        self.assertEqual(
            self.sqlite_db.session.query(Dataset).filter_by(vid=ds1_vid).all(),
            [],
            'Dataset was not removed.')


class GetVersionTest(BasePostgreSQLTest):

    def test_returns_user_version_from_sqlite_pragma(self):
        engine = create_engine('sqlite://')
        connection = engine.connect()
        connection.execute('PRAGMA user_version = 22')
        version = get_stored_version(connection)
        self.assertEqual(version, 22)

    def test_returns_user_version_from_postgres_table(self):
        if not self.postgres_dsn:
            # FIXME: it seems failing is better choice here.
            raise unittest.SkipTest(MISSING_POSTGRES_CONFIG_MSG)

        pg_connection = self.pg_connection()

        pg_connection.execute("CREATE SCHEMA ambrylib")

        create_table_query = '''
            CREATE TABLE ambrylib.user_version (
                version INTEGER NOT NULL); '''

        pg_connection.execute(create_table_query)
        pg_connection.execute('INSERT INTO ambrylib.user_version VALUES (22);')
        pg_connection.execute('commit')
        version = get_stored_version(pg_connection)
        self.assertEqual(version, 22)


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


class UpdateVersionTest(BasePostgreSQLTest):

    def setUp(self):
        super(self.__class__, self).setUp()
        engine = create_engine('sqlite://')
        self.sqlite_connection = engine.connect()

    def test_updates_user_version_sqlite_pragma(self):
        _update_version(self.sqlite_connection, 122)
        stored_version = self.sqlite_connection.execute('PRAGMA user_version').fetchone()[0]
        self.assertEqual(stored_version, 122)

    def test_creates_user_version_postgresql_table(self):
        if not self.postgres_dsn:
            # FIXME: it seems failing is better choice here.
            raise unittest.SkipTest(MISSING_POSTGRES_CONFIG_MSG)
        pg_connection = self.pg_connection()
        _update_version(pg_connection, 123)
        version = pg_connection.execute('SELECT version FROM ambrylib.user_version;').fetchone()[0]
        self.assertEqual(version, 123)

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
        version = pg_connection.execute('SELECT version FROM ambrylib.user_version;').fetchone()[0]
        self.assertEqual(version, 123)

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


class GetStoredVersionTest(unittest.TestCase):

    def setUp(self):
        engine = create_engine('sqlite://')
        self.connection = engine.connect()

    def test_raises_DatabaseMissingError_if_unknown_engine_connection_passed(self):
        with fudge.patched_context(self.connection.engine, 'name', 'foo'):
            with self.assertRaises(DatabaseMissingError):
                get_stored_version(self.connection)


class MigrateTest(unittest.TestCase):

    def setUp(self):
        engine = create_engine('sqlite://')
        self.connection = engine.connect()

    @fudge.patch(
        'ambry.orm.database._is_missed',
        'test.test_orm.functional.migrations.0100_init.Migration.migrate',
        'ambry.orm.database._get_all_migrations')
    def test_runs_missed_migration_and_changes_version(self, fake_is_missed, fake_migrate, fake_get):
        # prepare state.
        fake_is_missed.expects_call().returns(True)
        fake_migrate.expects_call()
        test_migrations = [
            (100, 'test.test_orm.functional.migrations.0100_init')
        ]
        fake_get.expects_call().returns(test_migrations)

        # run.
        migrate(self.connection)

        # testing.
        stored_version = self.connection.execute('PRAGMA user_version').fetchone()[0]
        self.assertEqual(stored_version, 100)

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
        self.assertEqual(stored_version, 22)
