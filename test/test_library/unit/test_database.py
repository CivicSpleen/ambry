# -*- coding: utf-8 -*-
import os
import shutil
import unittest
from tempfile import mkdtemp

import fudge
from fudge.inspector import arg

from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import IntegrityError, ProgrammingError, OperationalError
from sqlalchemy.orm.query import Query

from ambry.library.database import LibraryDb, ROOT_CONFIG_NAME_V, ROOT_CONFIG_NAME
from ambry.orm import Dataset, Config, Partition, File, Column, ColumnStat, Table
from ambry.dbexceptions import ConflictError, DatabaseError
from ambry.database.inserter import ValueInserter

from test.test_library.factories import DatasetFactory, ConfigFactory,\
    TableFactory, ColumnFactory, PartitionFactory,\
    ColumnStatFactory, FileFactory

from test.test_library.asserts import assert_spec

TEST_TEMP_DIR = 'test-library-'


class LibraryDbTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_temp_dir = mkdtemp(prefix=TEST_TEMP_DIR)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.test_temp_dir)

    def setUp(self):
        self.test_temp_dir = self.__class__.test_temp_dir
        library_db_file = os.path.join(self.test_temp_dir, 'test_database.db')
        self.sqlite_db = LibraryDb(driver='sqlite', dbname=library_db_file)
        self.sqlite_db.enable_delete = True
        self.sqlite_db.create_tables()
        self.sqlite_db.commit()

        # each factory requires db session. Populate all of them here, because we know the session.
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        TableFactory._meta.sqlalchemy_session = self.sqlite_db.session
        ColumnFactory._meta.sqlalchemy_session = self.sqlite_db.session
        PartitionFactory._meta.sqlalchemy_session = self.sqlite_db.session
        ColumnStatFactory._meta.sqlalchemy_session = self.sqlite_db.session
        ConfigFactory._meta.sqlalchemy_session = self.sqlite_db.session
        FileFactory._meta.sqlalchemy_session = self.sqlite_db.session
        # TODO: Uncomment and implement.
        # CodeFactory._meta.sqlalchemy_session = self.sqlite_db.session

        self.query = self.sqlite_db.session.query

    def tearDown(self):
        fudge.clear_expectations()
        fudge.clear_calls()
        try:
            os.remove(self.sqlite_db.dbname)
        except OSError:
            pass

    # helpers
    def _assert_exists(self, model_class, **filter_kwargs):
        query = self.query(model_class)\
            .filter_by(**filter_kwargs)
        assert query.first() is not None

    def _assert_does_not_exist(self, model_class, **filter_kwargs):
        query = self.query(model_class)\
            .filter_by(**filter_kwargs)
        assert query.first() is None

    @unittest.skip('Will implement it just before merge.')
    def test_initialization_raises_exception_if_driver_not_found(self):
        with self.assertRaises(ValueError):
            LibraryDb(driver='1')

    def test_initialization_populates_port(self):
        db = LibraryDb(driver='postgres', port=5232)
        self.assertIn('5232', db.dsn)

    def test_initialization_uses_library_schema_for_postgres(self):
        db = LibraryDb(driver='postgres')
        self.assertEquals(db._schema, 'library')

    def test_initialization_uses_library_schema_for_postgis(self):
        db = LibraryDb(driver='postgis')
        self.assertEquals(db._schema, 'library')

    @fudge.patch(
        'sqlalchemy.create_engine')
    def test_engine_creates_new_sqlalchemy_engine(self, fake_create):
        engine_stub = fudge.Fake().is_a_stub()
        fake_create.expects_call()\
            .returns(engine_stub)
        db = LibraryDb(driver='postgis')
        self.assertEquals(db.engine, engine_stub)

    @fudge.patch(
        'sqlalchemy.create_engine',
        'sqlalchemy.event',
        'ambry.database.sqlite._on_connect_update_sqlite_schema')
    def test_engine_listens_to_connect_signal_for_sqlite_driver(self, fake_create,
                                                                fake_event, fake_on):
        fake_event\
            .provides('listen')
        engine_stub = fudge.Fake().is_a_stub()
        fake_create.expects_call()\
            .returns(engine_stub)
        fake_on.expects_call()
        db = LibraryDb(driver='sqlite')
        self.assertEquals(db.engine, engine_stub)

    def test_connection_creates_new_sqlalchemy_connection(self):
        fake_connection = fudge.Fake()

        fake_engine = fudge.Fake()\
            .provides('connect')\
            .returns(fake_connection)

        db = LibraryDb(driver='sqlite')
        db._engine = fake_engine
        self.assertEquals(db.connection, fake_connection)

    def test_connection_sets_path_to_library_for_postgres(self):
        fake_connection = fudge.Fake('connection')\
            .provides('execute')\
            .with_args('SET search_path TO library')\
            .expects_call()

        fake_engine = fudge.Fake()\
            .provides('connect')\
            .returns(fake_connection)

        db = LibraryDb(driver='postgres')
        db._engine = fake_engine
        self.assertEquals(db.connection, fake_connection)

    def test_connection_sets_path_to_library_for_postgis(self):
        fake_connection = fudge.Fake('connection')\
            .provides('execute')\
            .with_args('SET search_path TO library')\
            .expects_call()

        fake_engine = fudge.Fake()\
            .provides('connect')\
            .returns(fake_connection)

        db = LibraryDb(driver='sqlite')
        db._engine = fake_engine
        self.assertEquals(db.connection, fake_connection)

    # .close tests
    def test_closes_session_and_connection(self):
        db = LibraryDb(driver='sqlite')
        db.session.close = fudge.Fake('session.close').expects_call()
        db.connection.close = fudge.Fake('connection.close').expects_call()
        db.close()
        fudge.verify()
        self.assertIsNone(db._session)
        self.assertIsNone(db._connection)

    # .commit tests
    def test_commit_commits_session(self):
        self.sqlite_db._session.commit = fudge.Fake().expects_call()
        self.sqlite_db.commit()

        fudge.verify()

    def test_commit_raises_session_commit_exception(self):
        self.sqlite_db._session.commit = fudge.Fake().expects_call().raises(ValueError)

        with self.assertRaises(ValueError):
            self.sqlite_db.commit()

        fudge.verify()

    # .rollback tests
    def test_rollbacks_session(self):
        self.sqlite_db.session.rollback = fudge.Fake('session.rollback').expects_call()
        self.sqlite_db.rollback()
        fudge.verify()

    # .inspector tests
    def test_contains_engine_inspector(self):
        db = LibraryDb(driver='sqlite')
        self.assertIsInstance(db.inspector, Inspector)
        self.assertEquals(db.engine, db.inspector.engine)

    # .exists tests
    def test_sqlite_database_does_not_exists_if_file_not_found(self):
        db = LibraryDb(driver='sqlite', dbname='no-such-file.db')
        self.assertFalse(db.exists())

    def test_returns_false_if_dataset_does_not_exist(self):
        query = "SELECT * FROM datasets WHERE d_vid = '{}' ".format(ROOT_CONFIG_NAME_V)
        self.assertIsNone(self.sqlite_db.connection.execute(query).fetchone())
        ret = self.sqlite_db.exists()
        self.assertFalse(ret)

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

    def test_returns_true_if_root_config_dataset_exists(self):
        # first assert signatures of the functions we are going to mock did not change.
        # prepare state
        root_config_ds = DatasetFactory()
        root_config_ds.vid = ROOT_CONFIG_NAME_V
        self.sqlite_db.session.commit()

        # testing.
        ret = self.sqlite_db.exists()
        self.assertTrue(ret)

    # clean tests
    def test_clean_deletes_all_instances(self):

        conf1 = ConfigFactory()
        ds1 = DatasetFactory()
        file1 = FileFactory()
        # TODO: Uncomment and implement
        # code1 = CodeFactory()
        partition1 = PartitionFactory()

        table1 = TableFactory(dataset=ds1)
        column1 = ColumnFactory(table=table1)
        colstat1 = ColumnStatFactory()

        self.sqlite_db.session.commit()

        models = [
            # (Code, dict(oid=code1.oid)),
            (Column, dict(vid=column1.vid)),
            (ColumnStat, dict(d_vid=colstat1.d_vid)),
            (Config, dict(d_vid=conf1.d_vid)),
            (Dataset, dict(vid=ds1.vid)),
            (File, dict(path=file1.path)),
            (Partition, dict(vid=partition1.vid)),
            (Table, dict(vid=table1.vid))
        ]

        # validate existance
        for model, kwargs in models:
            self._assert_exists(model, **kwargs)

        self.sqlite_db.clean()

        for model, kwargs in models:
            self._assert_does_not_exist(model, **kwargs)

    def test_raises_DatabaseError_if_deleting_failed_with_OperationalError(self):

        fake_session = fudge.Fake()\
            .expects('query')\
            .raises(OperationalError('select 1;', [], 'a'))

        with fudge.patched_context(self.sqlite_db, '_session', fake_session):
            with self.assertRaises(DatabaseError):
                self.sqlite_db.clean()

    def test_raises_DatabaseError_if_deleting_failed_with_IntegrityError(self):

        fake_session = fudge.Fake()\
            .expects('query')\
            .raises(IntegrityError('select 1;', [], 'a'))

        with fudge.patched_context(self.sqlite_db, '_session', fake_session):
            with self.assertRaises(DatabaseError):
                self.sqlite_db.clean()

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
        self.sqlite_db._add_config_root = fudge.Fake('_add_config_root').expects_call()
        ret = self.sqlite_db.create()
        self.assertTrue(ret)
        fudge.verify()

    def test_returns_false_if_database_exists(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(self.sqlite_db.exists, ['self'])

        # prepare state
        self.sqlite_db.exists = fudge.Fake('exists').expects_call().returns(True)
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
        library_db_file = os.path.join(self.test_temp_dir, 'no-such-dir', 'test_database1.db')

        # test
        with fudge.patched_context(os, 'makedirs', fake_makedirs):
            with fudge.patched_context(os.path, 'exists', fake_exists):
                db = LibraryDb(driver='sqlite', dbname=library_db_file)
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
        library_db_file = os.path.join(self.test_temp_dir, 'no-such-dir', 'test_database1.db')

        # test
        with fudge.patched_context(os, 'makedirs', fake_makedirs):
            with fudge.patched_context(os.path, 'exists', fake_exists):
                db = LibraryDb(driver='sqlite', dbname=library_db_file)
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
        library_db_file = os.path.join(self.test_temp_dir, 'no-such-dir', 'test_database1.db')

        # test
        with fudge.patched_context(os, 'makedirs', fake_makedirs):
            with fudge.patched_context(os.path, 'exists', fake_exists):
                try:
                    db = LibraryDb(driver='sqlite', dbname=library_db_file)
                    db._create_path()
                except Exception as exc:
                    self.assertIn('Couldn\'t create directory', exc.message)
        fudge.verify()

    # .drop tests
    def test_does_not_allow_to_delete_if_deleting_disabled(self):
        self.sqlite_db.enable_delete = False
        try:
            self.sqlite_db.drop()
        except Exception as exc:
            self.assertIn('Deleting not enabled', exc.message)

    # .clone tests
    def test_clone_returns_new_instance(self):
        db = LibraryDb(driver='sqlite')
        new_db = db.clone()
        self.assertNotEquals(db, new_db)
        self.assertEquals(db.driver, new_db.driver)
        self.assertEquals(db.server, new_db.server)
        self.assertEquals(db.dbname, new_db.dbname)
        self.assertEquals(db.username, new_db.username)
        self.assertEquals(db.password, new_db.password)

    # .create_tables test
    def test_creates_dataset_table(self):

        # Now all tables are created. Can we use ORM to create datasets?
        DatasetFactory()
        self.sqlite_db.session.commit()

    def test_creates_config_table(self):

        # Now all tables are created. Can we use ORM to create configs?
        ConfigFactory(key='a', value='b')
        self.sqlite_db.session.commit()

    def test_creates_table_table(self):

        # Now all tables are created. Can we use ORM to create datasets?
        ds1 = DatasetFactory()
        self.sqlite_db.session.commit()
        TableFactory(dataset=ds1)
        self.sqlite_db.session.commit()

    def test_creates_column_table(self):

        # Now all tables are created. Can we use ORM to create columns?

        # Column requires table and dataset.
        ds1 = DatasetFactory()
        self.sqlite_db.session.commit()

        table1 = TableFactory(dataset=ds1)
        self.sqlite_db.session.commit()

        ColumnFactory(table=table1)
        self.sqlite_db.session.commit()

    def test_creates_file_table(self):
        FileFactory()
        self.sqlite_db.session.commit()

    def test_creates_partition_table(self):

        ds1 = DatasetFactory()
        PartitionFactory(dataset=ds1)
        self.sqlite_db.session.commit()

    @unittest.skip('Uncomment and implement.')
    def test_creates_code_table(self):
        # CodeFactory()
        self.sqlite_db.session.commit()

    def test_creates_columnstat_table(self):
        self.sqlite_db.session.commit()
        ColumnStatFactory()
        self.sqlite_db.session.commit()

    # ._add_config_root
    def test_creates_new_root_config(self):
        # prepare state
        datasets = self.query(Dataset).all()
        self.assertEquals(len(datasets), 0)

        # testing
        self.sqlite_db._add_config_root()
        datasets = self.query(Dataset).all()
        self.assertEquals(len(datasets), 1)
        self.assertEquals(datasets[0].name, ROOT_CONFIG_NAME)
        self.assertEquals(datasets[0].vname, ROOT_CONFIG_NAME_V)

    def test_closes_session_if_root_config_exists(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(self.sqlite_db.close_session, ['self'])

        # prepare state
        DatasetFactory(id_=ROOT_CONFIG_NAME, vid=ROOT_CONFIG_NAME)
        self.sqlite_db.session.commit()
        self.sqlite_db.close_session = fudge.Fake('close_session').expects_call()

        # testing
        self.sqlite_db._add_config_root()
        fudge.verify()

    # ._clean_config_root tests
    def tests_resets_instance_fields(self):
        ds = DatasetFactory()
        ds.id_ = ROOT_CONFIG_NAME
        ds.name = 'name'
        ds.vname = 'vname'
        ds.source = 'source'
        ds.dataset = 'dataset'
        ds.creator = 'creator'
        ds.revision = 33
        self.sqlite_db.session.merge(ds)
        self.sqlite_db.commit()

        self.sqlite_db._clean_config_root()

        # refresh dataset
        ds = self.query(Dataset).filter(
            Dataset.id_ == ROOT_CONFIG_NAME).one()
        self.assertEquals(ds.name, ROOT_CONFIG_NAME)
        self.assertEquals(ds.vname, ROOT_CONFIG_NAME_V)
        self.assertEquals(ds.source, ROOT_CONFIG_NAME)
        self.assertEquals(ds.dataset, ROOT_CONFIG_NAME)
        self.assertEquals(ds.creator, ROOT_CONFIG_NAME)
        self.assertEquals(ds.revision, 1)

    # .inserter test
    # TODO: ValueInserter does not work without bundle. Fix it.
    @unittest.skip('ValueInserter requires bundle, but inserter method gives None instead.')
    def test_returns_value_inserter(self):
        ret = self.sqlite_db.inserter('datasets')
        self.assertIsInstance(ret, ValueInserter)

    # set_config_value tests
    def test_creates_new_config_if_config_does_not_exists(self):
        group = 'group-1'
        key = 'key-1'
        value = 'value-1'
        self.sqlite_db.set_config_value(group, key, value)
        self._assert_exists(Config, group=group, key=key, value=value)

    def test_changes_existing_config(self):
        group = 'group-1'
        key = 'key-1'
        value = 'value-1'

        DatasetFactory(id_=ROOT_CONFIG_NAME, vid=ROOT_CONFIG_NAME_V)
        ConfigFactory(
            d_vid=ROOT_CONFIG_NAME_V,
            group=group, key=key, value=value)
        self._assert_exists(Config, group=group, key=key, value=value)

        new_value = 'value-2'
        self.sqlite_db.set_config_value(group, key, new_value)
        self._assert_exists(Config, group=group, key=key, value=new_value)
        self._assert_does_not_exist(Config, value=value)

    # get_config_value tests
    def test_returns_config(self):
        group = 'group-1'
        key = 'key-1'
        value = 'value-1'

        DatasetFactory(id_=ROOT_CONFIG_NAME, d_vid=ROOT_CONFIG_NAME_V)
        ConfigFactory(
            d_vid=ROOT_CONFIG_NAME_V,
            group=group, key=key, value=value)
        ret = self.sqlite_db.get_config_value(group, key)
        self.assertIsNotNone(ret)
        self.assertEquals(ret.value, value)

    def test_returns_none_if_config_does_not_exist(self):
        ret = self.sqlite_db.get_config_value('group1', 'key1')
        self.assertIsNone(ret)

    def test_returns_none_if_config_query_failed(self):
        fake_filter = fudge.Fake()\
            .expects_call()\
            .raises(Exception('MyFakeException'))
        with fudge.patched_context(Query, 'filter', fake_filter):
            ret = self.sqlite_db.get_config_value('group1', 'key1')
            self.assertIsNone(ret)

    # get_config_group tests
    def test_returns_dict_with_key_and_values(self):
        group1 = 'group-1'
        key1 = 'key-1'
        value1 = 'value-1'

        key2 = 'key-2'
        value2 = 'value-2'

        DatasetFactory(id_=ROOT_CONFIG_NAME, d_vid=ROOT_CONFIG_NAME_V)

        ConfigFactory(
            d_vid=ROOT_CONFIG_NAME_V,
            group=group1, key=key1, value=value1)
        ConfigFactory(
            d_vid=ROOT_CONFIG_NAME_V,
            group=group1, key=key2, value=value2)

        ret = self.sqlite_db.get_config_group(group1)
        self.assertIn(key1, ret)
        self.assertIn(key2, ret)

        self.assertEquals(ret[key1], value1)
        self.assertEquals(ret[key2], value2)

    def test_returns_empty_dict_if_group_does_not_exist(self):

        group1 = 'group-1'
        self._assert_does_not_exist(Config, group=group1)

        ret = self.sqlite_db.get_config_group(group1)
        self.assertEquals(ret, {})

    def test_returns_empty_dict_on_any_error(self):
        # TODO: it is a bad idea to catch all errors without logging. Refactor.

        group1 = 'group-1'
        fake_session = fudge.Fake()\
            .expects('query')\
            .raises(Exception('My fake exception'))

        with fudge.patched_context(self.sqlite_db, '_session', fake_session):
            ret = self.sqlite_db.get_config_group(group1)
            self.assertEquals(ret, {})

    # .get_config_rows tests
    def test_returns_config_config_with_key_splitted_by_commas(self):
        group1 = 'config'
        key1 = '1.2.3.5.6'

        config1 = ConfigFactory(group=group1, key=key1)
        ret = self.sqlite_db.get_config_rows(config1.d_vid)
        self.assertEquals(len(ret), 1)
        conf1 = ret[0]

        # check splitted key
        key = conf1[0]
        self.assertEquals(key[0], '1')
        self.assertEquals(key[1], '2')
        self.assertEquals(key[2], '3')

        # check value
        value = conf1[1]
        self.assertEquals(value, config1.value)

    def test_returns_process_with_key_splitted_by_commas(self):
        group1 = 'process'
        key1 = '1.2.3.5.6'

        config1 = ConfigFactory(group=group1, key=key1)
        ret = self.sqlite_db.get_config_rows(config1.d_vid)
        self.assertEquals(len(ret), 1)
        conf1 = ret[0]

        # check splitted key
        key = conf1[0]
        self.assertEquals(key[0], 'process')
        self.assertEquals(key[1], '1')
        self.assertEquals(key[2], '2')

        # check value
        value = conf1[1]
        self.assertEquals(value, config1.value)

    # .get_bundle_value
    def test_returns_config_value(self):
        # TODO: Strange method. Isn't .get_config().value the same?
        group = 'group-1'
        key = 'key-1'
        value = 'value-1'

        DatasetFactory(id_=ROOT_CONFIG_NAME, d_vid=ROOT_CONFIG_NAME_V)

        ConfigFactory(
            d_vid=ROOT_CONFIG_NAME_V,
            group=group, key=key, value=value)

        self.assertEquals(
            self.sqlite_db.get_bundle_value(ROOT_CONFIG_NAME_V, group, key),
            value)

    def test_returns_none_if_config_does_not_exists(self):
        # TODO: Strange method. Isn't .get_config().value the same?
        group = 'group-1'
        key = 'key-1'
        self.assertIsNone(self.sqlite_db.get_bundle_value(ROOT_CONFIG_NAME_V, group, key))

    # get_bundle_values
    def test_returns_configs_of_the_group(self):
        group = 'group-1'
        key1 = 'key-1'
        value1 = 'value-1'

        key2 = 'key-2'
        value2 = 'value-2'

        DatasetFactory(id_=ROOT_CONFIG_NAME, d_vid=ROOT_CONFIG_NAME_V)

        ConfigFactory(
            d_vid=ROOT_CONFIG_NAME_V,
            group=group, key=key1, value=value1)
        ConfigFactory(
            d_vid=ROOT_CONFIG_NAME_V,
            group=group, key=key2, value=value2)

        ret = self.sqlite_db.get_bundle_values(ROOT_CONFIG_NAME_V, group)
        self.assertEquals(len(ret), 2)
        values = [x.value for x in ret]
        self.assertIn(value1, values)
        self.assertIn(value2, values)

    def test_returns_empty_list_if_group_configs_do_not_exists(self):
        group = 'group-1'

        self.assertEquals(
            self.sqlite_db.get_bundle_values(ROOT_CONFIG_NAME_V, group),
            [])

    def test_returns_None_on_any_value_retrieve_error(self):
        # TODO: catching all errors without logging is bad idea. Refactor.
        group = 'group-1'

        fake_session = fudge.Fake()\
            .expects('query')\
            .raises(Exception('My fake exception'))

        with fudge.patched_context(self.sqlite_db, '_session', fake_session):
            ret = self.sqlite_db.get_bundle_values(ROOT_CONFIG_NAME_V, group)
            self.assertIsNone(ret, {})

    # .config_values property tests
    def test_contains_dict_with_groups_keys_and_values(self):
        group = 'group-1'
        key1 = 'key-1'
        value1 = 'value-1'

        group2 = 'group-2'
        key2 = 'key-2'
        value2 = 'value-2'

        DatasetFactory(id_=ROOT_CONFIG_NAME, d_vid=ROOT_CONFIG_NAME_V)

        ConfigFactory(
            d_vid=ROOT_CONFIG_NAME_V,
            group=group, key=key1, value=value1)
        ConfigFactory(
            d_vid=ROOT_CONFIG_NAME_V,
            group=group2, key=key2, value=value2)
        self.assertIn(
            (group, key1),
            self.sqlite_db.config_values)
        self.assertIn(
            (group2, key2),
            self.sqlite_db.config_values)

        self.assertEquals(
            self.sqlite_db.config_values[(group, key1)],
            value1)
        self.assertEquals(
            self.sqlite_db.config_values[(group2, key2)],
            value2)

    # ._mark_update test
    def test_updates_config(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(self.sqlite_db.set_config_value, ['self', 'group', 'key', 'value'])

        self.sqlite_db.set_config_value = fudge.Fake('set_config_value')\
            .expects_call()\
            .with_args('activity', 'change', arg.any())

        self.sqlite_db._mark_update()
        fudge.verify()

    def test_contains_empty_dict(self):
        self.assertEquals(self.sqlite_db.config_values, {})

    # .install_dataset_identity tests
    def tests_installs_new_dataset_identity(self):

        class FakeIdentity(object):
            dict = {
                'source': 'source',
                'dataset': 'dataset',
                'revision': 1,
                'version': '0.1.1'}
            sname = 'sname'
            vname = 'vname'
            fqname = 'fqname'
            cache_key = 'cache_key'

        self.sqlite_db.install_dataset_identity(FakeIdentity())

        self._assert_exists(Dataset, name='sname')
        # TODO: test other fields

    def tests_raises_ConflictError_exception_if_save_failed(self):
        fake_commit = fudge.Fake('commit')\
            .expects_call()\
            .raises(IntegrityError('a', 'a', 'a'))
        self.sqlite_db.commit = fake_commit

        class FakeIdentity(object):
            dict = {
                'source': 'source',
                'dataset': 'dataset',
                'revision': 1,
                'version': '0.1.1'}
            vid = '1'
            sname = 'sname'
            vname = 'vname'
            fqname = 'fqname'
            cache_key = 'cache_key'

        with self.assertRaises(ConflictError):
            self.sqlite_db.install_dataset_identity(FakeIdentity(), overwrite=True)

    # .mark_table_installed tests
    def test_marks_table_as_installed(self):
        # prepare state

        ds1 = DatasetFactory()
        table1 = TableFactory(dataset=ds1)
        assert table1.installed is None

        # test
        self.sqlite_db.mark_table_installed(table1.vid)
        self.assertEqual(
            self.query(Table).filter_by(vid=table1.vid).one().installed,
            'y')

    # .mark_partition_installed tests
    def test_marks_partition_as_installed(self):
        # prepare state
        ds1 = DatasetFactory()
        partition1 = PartitionFactory(dataset=ds1)
        assert partition1.installed is None

        # test
        self.sqlite_db.mark_partition_installed(partition1.vid)
        self.assertEqual(
            self.query(Partition).filter_by(vid=partition1.vid).one().installed,
            'y')

    # .remove_bundle tests
    @unittest.skip('Where is Library.get_id definition?')
    def test_removes_all_partitions(self):
        pass

    @unittest.skip('Where is Library.get_id definition?')
    def test_deletes_dataset_colstats(test):
        pass

    @unittest.skip('Where is Library.get_id definition?')
    def test_deletes_dataset(test):
        pass

    # .delete_dataset_colstats tests
    def test_deletes_column_stat(self):
        # prepare state
        colstat1 = ColumnStatFactory()

        # save id to get rid of ObjectDeletedError.
        colstat1_d_vid = colstat1.d_vid
        self.sqlite_db.session.commit()

        # testing.
        self.sqlite_db.delete_dataset_colstats(colstat1.d_vid)
        self.assertEquals(
            self.query(ColumnStat).filter_by(d_vid=colstat1_d_vid).all(),
            [])

    # .remove_dataset tests
    def test_removes_dataset_colstats(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(self.sqlite_db.delete_dataset_colstats, ['self', 'dvid'])

        # prepare state.
        ds1 = DatasetFactory()
        fake_delete = fudge.Fake().expects_call()

        # testing
        with fudge.patched_context(LibraryDb, 'delete_dataset_colstats', fake_delete):
            self.sqlite_db.remove_dataset(ds1.vid)
        fudge.verify()

    def test_removes_dataset(self):

        # prepare state.
        ds1 = DatasetFactory()
        ds1_vid = ds1.vid

        # testing
        self.sqlite_db.remove_dataset(ds1.vid)
        self.assertEquals(
            self.query(Dataset).filter_by(vid=ds1_vid).all(),
            [],
            'Dataset was not removed.')

    # .remove_partition_record tests
    def test_removes_partition_and_stat(self):

        # prepare state

        ds1 = DatasetFactory()
        table1 = TableFactory(dataset=ds1)
        partition1 = PartitionFactory(dataset=ds1, table=table1)
        column1 = ColumnFactory(table=table1)
        colstat1 = ColumnStatFactory(p_vid=partition1.vid, c_vid=column1.vid, d_vid=ds1.vid)
        self.sqlite_db.session.commit()

        # save necessary ids for later use.
        partition_vid = partition1.vid
        colstat1_c_vid = colstat1.c_vid

        # testing
        self.sqlite_db.remove_partition_record(partition_vid)
        partition_query = self.query(Partition).filter_by(vid=partition_vid)
        self.assertEquals(partition_query.all(), [], 'Partition was not deleted.')

        colstat_query = self.query(ColumnStat).filter_by(c_vid=colstat1_c_vid)
        self.assertEquals(colstat_query.all(), [], 'ColumnStat instance was not deleted.')

    # .get tests
    # TODO:

    # .get_table tests

    def test_returns_table(self):

        # prepare state
        ds1 = DatasetFactory()
        table1 = TableFactory(dataset=ds1)

        # testing
        ret = self.sqlite_db.get_table(table1.vid)
        self.assertIsInstance(ret, Table)
        self.assertEquals(ret.vid, table1.vid)

    # .tables tests
    @unittest.skip(
        '.tables() method raises TypeError: list indices must be integers exception and seems unused.')
    def test_dict_with_all_tables(self):

        # prepare state
        ds1 = DatasetFactory()
        table1 = TableFactory(dataset=ds1)

        ds2 = DatasetFactory()
        table2 = TableFactory(dataset=ds2)

        # testing
        ret = self.sqlite_db.tables()
        self.assertIsInstance(ret, dict)
        self.assertIn(table1.name, ret)
        self.assertIn(table2.name, ret)

    # .list tests
    # TODO:

    # .all_vids tests
    def test_returns_all_datasets_and_partitions(self):
        # prepare state

        ds1 = DatasetFactory()
        table1 = TableFactory(dataset=ds1)
        self.sqlite_db.session.commit()

        partition1 = PartitionFactory(dataset=ds1, t_id=table1.id_)
        partition2 = PartitionFactory(dataset=ds1, t_id=table1.id_)
        self.sqlite_db.session.commit()

        # testing
        ret = self.sqlite_db.all_vids()
        self.assertEquals(len(ret), 3)
        self.assertIn(ds1.vid, ret)
        self.assertIn(partition1.vid, ret)
        self.assertIn(partition2.vid, ret)

    # .datasets tests
    @unittest.skip('raises `AttributeError: type object \'Dataset\' has no attribute \'location\'` error.')
    def test_returns_dict_with_library_datasets(self):
        # prepare state

        ds1 = DatasetFactory(location=Dataset.LOCATION.LIBRARY)
        ds2 = DatasetFactory(location=Dataset.LOCATION.LIBRARY)
        ds3 = DatasetFactory(location=Dataset.LOCATION.PARTITION)

        # testing
        ret = self.sqlite_db.datasets()
        self.assertIsInstance(ret, dict)
        self.assertEquals(len(ret.keys()), 2)
        self.assertIn(ds1.vid, ret)
        self.assertIn(ds2.vid, ret)
        self.assertIn(ds3.vid, ret)
