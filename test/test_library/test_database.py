import os
import unittest

import fudge

from ambry.library.database import LibraryDb
from ambry.orm import Dataset, Config, Partition, File, Column, ColumnStat, Table, Code

from test.test_library.factories import DatasetFactory, ConfigFactory,\
    TableFactory, ColumnFactory, FileFactory, PartitionFactory, CodeFactory,\
    ColumnStatFactory


class LibraryDbTest(unittest.TestCase):
    def setUp(self):
        self.sqlite_db = LibraryDb(driver='sqlite', dbname='test_database.db')
        self.sqlite_db.enable_delete = True

    def tearDown(self):
        self.sqlite_db.drop()
        os.remove('test_database.db')

    # helpers
    def _assert_exists(self, model_class, **filter_kwargs):
        query = self.sqlite_db.session.query(model_class)\
            .filter_by(**filter_kwargs)
        assert query.first() is not None

    def _assert_does_not_exist(self, model_class, **filter_kwargs):
        query = self.sqlite_db.session.query(model_class)\
            .filter_by(**filter_kwargs)
        assert query.first() is None

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

    def test_commit_commits_session(self):
        fake_session = fudge.Fake('session')\
            .provides('commit')\
            .expects_call()
        db = LibraryDb(driver='sqlite')
        db.Session = fake_session
        db._session = fake_session
        db.commit()

    def test_commit_raises_session_commit_exception(self):
        fake_session = fudge.Fake('session')\
            .provides('commit')\
            .expects_call()\
            .raises(ValueError)
        db = LibraryDb(driver='sqlite')
        db.Session = fake_session
        db._session = fake_session
        with self.assertRaises(ValueError):
            db.commit()

    # exists tests
    def test_sqlite_database_does_not_exists_if_file_not_found(self):
        db = LibraryDb(driver='sqlite', dbname='no-such-file.db')
        self.assertFalse(db.exists())

    # clone tests
    def test_clone_returns_new_instance(self):
        db = LibraryDb(driver='sqlite')
        new_db = db.clone()
        self.assertNotEquals(db, new_db)
        self.assertEquals(db.driver, new_db.driver)
        self.assertEquals(db.server, new_db.server)
        self.assertEquals(db.dbname, new_db.dbname)
        self.assertEquals(db.username, new_db.username)
        self.assertEquals(db.password, new_db.password)

    # create_tables test
    def test_creates_dataset_table(self):
        self.sqlite_db.create_tables()

        # Now all tables are created. Can we use ORM to create datasets?
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        DatasetFactory()
        self.sqlite_db.session.commit()

    def test_creates_config_table(self):
        self.sqlite_db.create_tables()

        # Now all tables are created. Can we use ORM to create configs?
        ConfigFactory._meta.sqlalchemy_session = self.sqlite_db.session
        ConfigFactory(key='a', value='b')
        self.sqlite_db.session.commit()

    def test_creates_table_table(self):
        self.sqlite_db.create_tables()

        # Now all tables are created. Can we use ORM to create configs?
        TableFactory._meta.sqlalchemy_session = self.sqlite_db.session
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        ds1 = DatasetFactory()
        self.sqlite_db.session.commit()
        TableFactory(dataset=ds1)
        self.sqlite_db.session.commit()

    def test_creates_column_table(self):
        self.sqlite_db.create_tables()

        # Now all tables are created. Can we use ORM to create columns?
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        TableFactory._meta.sqlalchemy_session = self.sqlite_db.session
        ColumnFactory._meta.sqlalchemy_session = self.sqlite_db.session

        # it requires table and dataset.
        ds1 = DatasetFactory()
        self.sqlite_db.session.commit()

        table1 = TableFactory(dataset=ds1)
        self.sqlite_db.session.commit()

        ColumnFactory(table=table1)
        self.sqlite_db.session.commit()

    def test_creates_file_table(self):
        self.sqlite_db.create_tables()
        FileFactory._meta.sqlalchemy_session = self.sqlite_db.session
        FileFactory()
        self.sqlite_db.session.commit()

    def test_creates_partition_table(self):
        self.sqlite_db.create_tables()
        PartitionFactory._meta.sqlalchemy_session = self.sqlite_db.session
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        ds1 = DatasetFactory()
        self.sqlite_db.session.commit()
        PartitionFactory(dataset=ds1)
        self.sqlite_db.session.commit()

    def test_creates_code_table(self):
        self.sqlite_db.create_tables()
        CodeFactory._meta.sqlalchemy_session = self.sqlite_db.session
        CodeFactory()
        self.sqlite_db.session.commit()

    def test_creates_columnstat_table(self):
        self.sqlite_db.create_tables()
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        TableFactory._meta.sqlalchemy_session = self.sqlite_db.session
        ColumnFactory._meta.sqlalchemy_session = self.sqlite_db.session
        PartitionFactory._meta.sqlalchemy_session = self.sqlite_db.session
        ColumnStatFactory._meta.sqlalchemy_session = self.sqlite_db.session

        ds1 = DatasetFactory()
        self.sqlite_db.session.commit()
        partition1 = PartitionFactory(dataset=ds1)

        table1 = TableFactory(dataset=ds1)
        self.sqlite_db.session.commit()

        column1 = ColumnFactory(table=table1)

        ColumnStatFactory._meta.sqlalchemy_session = self.sqlite_db.session
        ColumnStatFactory(partition=partition1, column=column1)
        self.sqlite_db.session.commit()

    # clean tests
    def test_clean_deletes_all_instances(self):
        self.sqlite_db.create_tables()
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        TableFactory._meta.sqlalchemy_session = self.sqlite_db.session
        ColumnFactory._meta.sqlalchemy_session = self.sqlite_db.session
        PartitionFactory._meta.sqlalchemy_session = self.sqlite_db.session
        ColumnStatFactory._meta.sqlalchemy_session = self.sqlite_db.session
        ConfigFactory._meta.sqlalchemy_session = self.sqlite_db.session
        FileFactory._meta.sqlalchemy_session = self.sqlite_db.session
        CodeFactory._meta.sqlalchemy_session = self.sqlite_db.session

        conf1 = ConfigFactory()
        ds1 = DatasetFactory()
        file1 = FileFactory()
        code1 = CodeFactory()
        partition1 = PartitionFactory(dataset=ds1)

        table1 = TableFactory(dataset=ds1)
        column1 = ColumnFactory(table=table1)
        colstat1 = ColumnStatFactory(partition=partition1, column=column1)

        self.sqlite_db.session.commit()

        models = [
            (Code, dict(oid=code1.oid)),
            (Column, dict(vid=column1.vid)),
            (ColumnStat, dict(id=colstat1.id)),
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
