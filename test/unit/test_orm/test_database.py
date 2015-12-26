# -*- coding: utf-8 -*-
import os
import unittest

try:
    # py2, mock is external lib.
    from mock import patch, Mock
except ImportError:
    # py3, mock is included
    from unittest.mock import patch, Mock

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection as SQLAlchemyConnection, Engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool

from ambry.orm.database import get_stored_version, _validate_version, _migration_required, SCHEMA_VERSION,\
    _update_version, _is_missed, migrate
from ambry.orm.exc import DatabaseError, DatabaseMissingError
from ambry.orm.database import Database, ROOT_CONFIG_NAME_V, ROOT_CONFIG_NAME, POSTGRES_SCHEMA_NAME
from ambry.orm.dataset import Dataset

from ambry.run import get_runconfig

from test.factories import DatasetFactory, TableFactory,\
    ColumnFactory, PartitionFactory

from test.unit.asserts import assert_spec

from test.test_base import TestBase, PostgreSQLTestBase


class DatabaseTest(unittest.TestCase):

    def setUp(self):
        self.sqlite_db = Database('sqlite://')
        self.sqlite_db.create()

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
        self.assertEqual(db.driver, 'postgres')

    # .create tests
    def test_creates_new_database(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(self.sqlite_db._create_path, ['self'])
        assert_spec(self.sqlite_db.exists, ['self'])
        assert_spec(self.sqlite_db.create_tables, ['self'])
        assert_spec(self.sqlite_db._add_config_root, ['self'])

        # prepare state
        self.sqlite_db.exists = Mock(return_value=False)
        self.sqlite_db._create_path = Mock()
        self.sqlite_db.create_tables = Mock()

        # run
        ret = self.sqlite_db.create()

        # check result and calls.
        self.assertTrue(ret)
        self.sqlite_db.exists.assert_called_once_with()
        self.sqlite_db._create_path.assert_called_once_with()
        self.sqlite_db.create_tables.assert_called_once_with()

    def test_returns_false_if_database_exists(self):

        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(self.sqlite_db.exists, ['self'])

        # prepare state
        with patch.object(self.sqlite_db, 'exists', Mock(return_value=True)):
            ret = self.sqlite_db.create()
            self.assertFalse(ret)

    # ._create_path tests
    def test_makes_database_directory(self):

        # prepare state

        calls = []

        def my_exists(p):
            # returns False for first call and True for second.
            if p in calls:
                return True
            calls.append(p)
            return False

        # test
        with patch.object(os, 'makedirs', Mock()) as fake_makedirs:
            with patch.object(os.path, 'exists', Mock(side_effect=my_exists)):
                db = Database('sqlite:///test_database1.db')
                db._create_path()

                # test calls
                fake_makedirs.assert_called_once_with('/')

    def test_ignores_exception_if_makedirs_failed(self):

        # prepare state.
        calls = []

        def my_exists(p):
            # returns False for first call and True for second.
            if p in calls:
                return True
            calls.append(p)
            return False

        # test
        with patch.object(os, 'makedirs', Mock(side_effect=Exception('Fake exception'))) as fake_makedirs:
            with patch.object(os.path, 'exists', Mock(side_effect=my_exists)):
                db = Database('sqlite:///test_database1.db')
                db._create_path()

                fake_makedirs.assert_called_once_with('/')

    def test_raises_exception_if_dir_does_not_exist_after_creation_attempt(self):

        # test
        with patch.object(os.path, 'exists', Mock(return_value=False)):
            with patch.object(os, 'makedirs', Mock()) as fake_makedirs:
                try:
                    db = Database('sqlite:///test_database1.db')
                    db._create_path()
                except Exception as exc:
                    self.assertIn('Couldn\'t create directory', str(exc))
                fake_makedirs.assert_called_once_with('/')

    # .exists tests
    def test_sqlite_database_does_not_exists_if_file_not_found(self):
        db = Database('sqlite://no-such-file.db')
        self.assertFalse(db.exists())

    @patch('ambry.orm.database.os.path.exists')
    def test_returns_true_if_config_table_was_found_by_inspector(self, fake_path):
        # prepare state
        fake_path.return_value = True

        # testing.
        self.assertTrue(self.sqlite_db.exists())

    @patch('ambry.orm.database.os.path.exists')
    def test_returns_false_if_config_table_was_not_found_by_inspector(self, fake_path):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(self.sqlite_db.connection.execute, ['self', 'object'])

        # prepare state
        fake_path.return_value = True

        from sqlalchemy.engine.reflection import Inspector
        with patch.object(Inspector, 'get_table_names', Mock(return_value=[])) as fake_get:
            # run
            ret = self.sqlite_db.exists()

            # test
            self.assertFalse(ret)
            fake_get.assert_called_once_with(schema=self.sqlite_db._schema)

    # engine() tests.
    @patch('ambry.orm.database._validate_version')
    def test_engine_creates_and_caches_sqlalchemy_engine(self, fake_validate):
        db = Database('sqlite://')
        self.assertIsInstance(db.engine, Engine)
        self.assertIsInstance(db._engine, Engine)
        self.assertEqual(len(fake_validate.mock_calls), 1)

    # connection tests.
    def test_creates_and_caches_new_sqlalchemy_connection(self):
        db = Database('sqlite://')
        self.assertIsInstance(db.connection, SQLAlchemyConnection)
        self.assertIsInstance(db._connection, SQLAlchemyConnection)

    # .session tests # FIXME:

    # .open tests # FIXME:

    # .close tests
    def test_closes_session_and_connection(self):
        db = Database('sqlite://')
        with patch.object(db.session, 'close', Mock()) as fake_session_close:
            with patch.object(db.connection, 'close', Mock()) as fake_connection_close:
                db.close()

                fake_session_close.assert_called_once_with()
                fake_connection_close.assert_called_once_with()

        self.assertIsNone(db._session)
        self.assertIsNone(db._connection)

    # .commit tests
    def test_commits_session(self):
        db = Database('sqlite://')

        # init engine and session
        db.session

        with patch.object(db._session, 'commit', Mock()) as fake_commit:
            db.commit()
            fake_commit.assert_called_once_with()

    def test_raises_session_commit_exception(self):
        db = Database('sqlite://')

        # init engine and session
        db.session

        with patch.object(db._session, 'commit', Mock(side_effect=ValueError)):
            with self.assertRaises(ValueError):
                db.commit()

    # .rollback tests
    def test_rollbacks_session(self):
        db = Database('sqlite://')

        # init engine and session
        db.session

        with patch.object(db._session, 'rollback', Mock()) as fake_rollback:
            db.rollback()
            fake_rollback.assert_called_once_with()

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
        fake_drop = Mock(side_effect=OperationalError('select 1;', [], 'a'))
        with patch.object(db, 'drop', fake_drop):
            db.create_tables()
            fake_drop.assert_called_once_with()

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
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        TableFactory._meta.sqlalchemy_session = self.sqlite_db.session

        ds1 = DatasetFactory()
        self.sqlite_db.commit()
        table = TableFactory(dataset=ds1)
        ColumnFactory._meta.sqlalchemy_session = self.sqlite_db.session

        # Now all tables are created. Can we use ORM to create columns?
        ColumnFactory(name='id', table=table)
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
        with patch.object(db, '_add_config_root', Mock()):
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
        with patch.object(self.sqlite_db, 'close_session', Mock()) as fake_close:
            self.sqlite_db._add_config_root()
            fake_close.assert_called_once_with()

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


class GetVersionTest(TestBase):

    def test_returns_user_version_from_sqlite_pragma(self):
        engine = create_engine('sqlite://')
        connection = engine.connect()
        connection.execute('PRAGMA user_version = 22')
        version = get_stored_version(connection)
        self.assertEqual(version, 22)

    def test_returns_user_version_from_postgres_table(self):
        if not self.__class__._is_postgres:
            raise unittest.SkipTest('Postgres tests are disabled.')

        engine = create_engine(self.__class__.library_test_dsn,  poolclass=NullPool)
        with engine.connect() as conn:
            create_table_query = '''
                CREATE TABLE {}.user_version (
                    version INTEGER NOT NULL); '''\
                .format(POSTGRES_SCHEMA_NAME)

            conn.execute(create_table_query)
            conn.execute('INSERT INTO {}.user_version VALUES (22);'.format(POSTGRES_SCHEMA_NAME))
            conn.execute('COMMIT;')
            version = get_stored_version(conn)
            self.assertEqual(version, 22)


class ValidateVersionTest(unittest.TestCase):

    @patch('ambry.orm.database.get_stored_version')
    def test_raises_database_error_if_db_version_is_between_10_100(self, fake_get):
        fake_get.return_value = 88
        engine = create_engine('sqlite://')
        connection = engine.connect()
        with self.assertRaises(DatabaseError):
            _validate_version(connection)

    @patch('ambry.orm.database.get_stored_version')
    @patch('ambry.orm.database._migration_required')
    @patch('ambry.orm.database.migrate')
    def test_runs_migrations(self, fake_migrate, fake_required, fake_get):
        fake_required.return_value = True
        fake_get.return_value = 100

        engine = create_engine('sqlite://')
        connection = engine.connect()
        _validate_version(connection)
        fake_required.assert_called_once_with(connection)


class MigrationRequiredTest(unittest.TestCase):

    def setUp(self):
        engine = create_engine('sqlite://')
        self.connection = engine.connect()

    @patch('ambry.orm.database.get_stored_version')
    def test_raises_assertion_error_if_user_has_old_code(self, fake_get):
        fake_get.return_value = SCHEMA_VERSION + 10
        with self.assertRaises(AssertionError):
            _migration_required(self.connection)

    @patch('ambry.orm.database.get_stored_version')
    def test_returns_true_if_stored_version_is_less_than_actual(self, fake_get):
        fake_get.return_value = SCHEMA_VERSION - 1
        self.assertTrue(_migration_required(self.connection))

    @patch('ambry.orm.database.get_stored_version')
    def test_returns_false_if_stored_version_equals_to_actual(self, fake_get):
        fake_get.return_value = SCHEMA_VERSION
        self.assertFalse(_migration_required(self.connection))


class UpdateVersionTest(TestBase):

    def setUp(self):
        super(self.__class__, self).setUp()
        engine = create_engine('sqlite://')
        self.sqlite_connection = engine.connect()

    def test_updates_user_version_sqlite_pragma(self):
        _update_version(self.sqlite_connection, 122)
        stored_version = self.sqlite_connection.execute('PRAGMA user_version').fetchone()[0]
        self.assertEqual(stored_version, 122)

    def test_creates_user_version_postgresql_table(self):
        if not self.__class__._is_postgres:
            raise unittest.SkipTest('Postgres tests are disabled.')
        engine = create_engine(self.__class__.library_test_dsn,  poolclass=NullPool)
        with engine.connect() as conn:
            _update_version(conn, 123)
            version = conn\
                .execute('SELECT version FROM {}.user_version;'.format(POSTGRES_SCHEMA_NAME))\
                .fetchone()[0]
            self.assertEqual(version, 123)

    def test_updates_user_version_postgresql_table(self):
        if not self.__class__._is_postgres:
            raise unittest.SkipTest('Postgres tests are disabled.')
        engine = create_engine(self.__class__.library_test_dsn,  poolclass=NullPool)
        with engine.connect() as conn:
            create_table_query = '''
                CREATE TABLE {}.user_version (
                    version INTEGER NOT NULL); '''\
                .format(POSTGRES_SCHEMA_NAME)

            conn.execute(create_table_query)
            conn.execute('INSERT INTO {}.user_version VALUES (22);'.format(POSTGRES_SCHEMA_NAME))
            conn.execute('COMMIT;')

            _update_version(conn, 123)
            version = conn\
                .execute('SELECT version FROM {}.user_version;'.format(POSTGRES_SCHEMA_NAME))\
                .fetchone()[0]
            self.assertEqual(version, 123)

    def test_raises_DatabaseMissingError_error(self):
        with patch.object(self.sqlite_connection, 'engine', Mock(name='foo')):
            with self.assertRaises(DatabaseMissingError):
                _update_version(self.sqlite_connection, 1)


class IsMissedTest(unittest.TestCase):

    def setUp(self):
        engine = create_engine('sqlite://')
        self.connection = engine.connect()

    @patch('ambry.orm.database.get_stored_version')
    def test_returns_true_if_migration_is_not_applied(self, fake_stored):
        fake_stored.return_value = 1
        self.assertTrue(_is_missed(self.connection, 2))

    @patch('ambry.orm.database.get_stored_version')
    def test_returns_false_if_migration_applied(self, fake_stored):
        fake_stored.return_value = 2
        self.assertFalse(_is_missed(self.connection, 2))


class GetStoredVersionTest(unittest.TestCase):

    def setUp(self):
        engine = create_engine('sqlite://')
        self.connection = engine.connect()

    def test_raises_DatabaseMissingError_if_unknown_engine_connection_passed(self):
        with patch.object(self.connection, 'engine', Mock(name='foo')):
            with self.assertRaises(DatabaseError):
                get_stored_version(self.connection)


class MigrateTest(unittest.TestCase):

    def setUp(self):
        engine = create_engine('sqlite://')
        self.connection = engine.connect()

    @patch('ambry.orm.database._is_missed')
    @patch('test.functional.migrations.0100_init.Migration.migrate')
    @patch('ambry.orm.database._get_all_migrations')
    def test_runs_missed_migration_and_changes_version(self, fake_get, fake_migrate, fake_is_missed):
        # prepare state.
        fake_is_missed.return_value = True
        test_migrations = [
            (100, 'test.functional.migrations.0100_init')
        ]
        fake_get.return_value = test_migrations

        # run.
        migrate(self.connection)

        # testing.
        stored_version = self.connection.execute('PRAGMA user_version').fetchone()[0]
        self.assertEqual(stored_version, 100)
        fake_migrate.assert_called_once_with(self.connection)

    @patch('ambry.orm.database._is_missed')
    @patch('ambry.orm.migrations.0100_init.Migration.migrate')
    def test_does_not_change_version_if_migration_failed(self, fake_migrate, fake_is_missed):
        fake_is_missed.return_value = True

        class MyException(Exception):
            pass

        fake_migrate.side_effect = MyException('My fake exception')
        self.connection.execute('PRAGMA user_version = 22')
        with self.assertRaises(MyException):
            migrate(self.connection)
        stored_version = self.connection.execute('PRAGMA user_version').fetchone()[0]
        fake_migrate.assert_called_once_with(self.connection)
        self.assertEqual(stored_version, 22)
