# -*- coding: utf-8 -*-

import os
from tempfile import mkdtemp
import unittest

import fudge

from sqlalchemy.exc import OperationalError

from ambry.bundle import DbBundle
from ambry.dbexceptions import NotFoundError, DatabaseError
from ambry.library import Library, _new_library
from ambry.library.database import LibraryDb
from ambry.library.files import Files
from ambry.orm import File

from test.test_library.asserts import assert_spec
from test.test_library.factories import DatasetFactory, ConfigFactory,\
    TableFactory, ColumnFactory, FileFactory, PartitionFactory, CodeFactory,\
    ColumnStatFactory


SQLITE_DATABASE = 'test_library_test_init.db'
BUNDLES_DIR_PREFIX = 'test_library_test_init_bundles'


class NewLibraryTest(unittest.TestCase):
    def setUp(self):
        self.sqlite_db = LibraryDb(driver='sqlite', dbname=SQLITE_DATABASE)
        self.sqlite_db.enable_delete = True
        self.sqlite_db.create_tables()

    def tearDown(self):
        try:
            os.remove(SQLITE_DATABASE)
        except OSError:
            pass

    def test_raises_DatabaseError_if_database_creation_failed(self):

        fake_create = fudge.Fake().expects_call().raises(OperationalError('select 1;', [], 'a'))
        config = {
            'database': {
                'driver': 'sqlite',
                'dbname': 'unused1.db',
            },
            'filesystem': 'http://example.com'}
        with fudge.patched_context(LibraryDb, 'create', fake_create):
            with self.assertRaises(DatabaseError):
                _new_library(config)

    def test_upstream_setting_is_deprecated(self):

        config = {
            'database': {
                'driver': 'sqlite',
                'dbname': 'test_init1_unused.db',
            },
            'filesystem': 'http://example.com',
            'upstream': 'upstream'}
        with self.assertRaises(DeprecationWarning):
            _new_library(config)


class LibraryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cache = mkdtemp(prefix=BUNDLES_DIR_PREFIX)

    @classmethod
    def tearDownClass(cls):
        os.rmdir(cls.cache)

    def setUp(self):
        self.sqlite_db = LibraryDb(driver='sqlite', dbname=SQLITE_DATABASE)
        self.sqlite_db.enable_delete = True
        self.sqlite_db.create_tables()

        self.query = self.sqlite_db.session.query

        # each factory requires db session. Populate all of them here, because we know the session.
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        TableFactory._meta.sqlalchemy_session = self.sqlite_db.session
        ColumnFactory._meta.sqlalchemy_session = self.sqlite_db.session
        PartitionFactory._meta.sqlalchemy_session = self.sqlite_db.session
        ColumnStatFactory._meta.sqlalchemy_session = self.sqlite_db.session
        ConfigFactory._meta.sqlalchemy_session = self.sqlite_db.session
        FileFactory._meta.sqlalchemy_session = self.sqlite_db.session
        CodeFactory._meta.sqlalchemy_session = self.sqlite_db.session

        # cache is directory name where to store packaged bundles.
        self.cache = self.__class__.cache

    def tearDown(self):
        fudge.clear_expectations()
        fudge.clear_calls()
        try:
            os.remove(SQLITE_DATABASE)
        except OSError:
            pass

    # .clone tests
    # TODO:

    # ._create_bundle tests
    def test_returns_existing_bundle(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        bundle1 = DbBundle('temp1.db')
        lib.bundles[bundle1.path] = bundle1

        # testing
        returned_bundle = lib._create_bundle(bundle1.path)
        self.assertIs(returned_bundle, bundle1)

    def test_creates_new_bundle(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)

        path = 'path1'
        self.assertNotIn(path, lib.bundles)

        # testing
        new_bundle = lib._create_bundle(path)
        self.assertIsInstance(new_bundle, DbBundle)
        self.assertIn(path, lib.bundles)

    # .close tests
    def test_closes_all_bundles_and_database(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(DbBundle.close, ['self'])

        # prepare state
        fake_close = fudge.Fake().expects_call()
        self.sqlite_db.close = fudge.Fake().expects_call()

        lib = Library(self.cache, self.sqlite_db)
        bundle1 = DbBundle('temp1.db')
        lib.bundles[bundle1.path] = bundle1

        bundle2 = DbBundle('temp2.db')
        lib.bundles[bundle2.path] = bundle2

        # testing
        with fudge.patched_context(DbBundle, 'close', fake_close):
            lib.close()
        fudge.verify()

    # .commit tests
    def test_commits_to_database(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(DbBundle.close, ['self'])

        # prepare state
        self.sqlite_db.commit = fudge.Fake().expects_call()

        lib = Library(self.cache, self.sqlite_db)
        lib.commit()

        # testing
        fudge.verify()

    # ._meta_set tests
    def test_saves_given_setting_to_database(self):
        lib = Library(self.cache, self.sqlite_db)
        lib._meta_set('key1', 'value1')
        lib.commit()

        # testing. Get config from database to make sure it exists.
        saved_value = self.sqlite_db.get_config_value('library', 'key1')
        self.assertIsNotNone(saved_value)
        self.assertEquals(saved_value.key, 'key1')
        self.assertEquals(saved_value.value, 'value1')

    # ._meta_set tests
    def test_gets_config_from_database(self):
        # prepare state.
        lib = Library(self.cache, self.sqlite_db)
        self.sqlite_db.set_config_value('library', 'key1', 'value1')
        self.sqlite_db.commit()

        # testing.
        config_value = lib._meta_get('key1')
        self.assertEquals(config_value, 'value1')

    def test_returns_none_if_setting_does_not_exist(self):
        # prepare state.
        lib = Library(self.cache, self.sqlite_db)

        # testing.
        config_value = lib._meta_get('?no-such-key?')
        self.assertIsNone(config_value)

    # .warehouse_url property tests
    def test_contains_warehouse_url(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(Library._meta_get, ['self', 'key'])

        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib._meta_get = fudge.Fake()\
            .expects_call()\
            .with_args('warehouse_url')\
            .returns('http://example.com')

        # testing
        self.assertEquals(lib.warehouse_url, 'http://example.com')

    # .warehouse_url setter tests
    def test_sets_meta_config(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(Library._meta_set, ['self', 'key', 'value'])

        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib._meta_set = fudge.Fake('_meta_set')\
            .expects_call()\
            .with_args('warehouse_url', 'http://example.com')

        # testing
        lib.warehouse_url = 'http://example.com'
        fudge.verify()

    # .put_bundle tests
    # TODO:

    # .put_partition tests
    # TODO:

    # .remove tests
    # TODO:

    # .list tests
    # TODO:

    # .list_bundles tests
    def test_returns_last_versions_only(self):
        # prepare state.
        lib = Library(self.cache, self.sqlite_db)

        # create datasets for bundles
        ds1 = DatasetFactory(version='0.0.1')
        ds2 = DatasetFactory(version='0.0.1')
        ds1_vid = ds1.vid
        ds2_vid = ds2.vid
        self.sqlite_db.session.commit()

        # test
        bundles = [x for x in lib.list_bundles(locations=None)]
        self.assertEquals(len(bundles), 2)
        vids = [x.dataset.vid for x in bundles]
        self.assertIn(ds1_vid, vids)
        self.assertIn(ds2_vid, vids)

    # ._get_bundle_by_cache_key
    @fudge.patch('ckcache.multi.AltReadCache.get')
    def test_returns_false_if_path_does_not_exists(self, fake_get):
        fake_get.expects_call().returns('/bundle-path')
        lib = Library(self.cache, self.sqlite_db)
        ret = lib._get_bundle_by_cache_key('cache_key1')
        self.assertFalse(ret)

    # .has tests
    # TODO:

    # .get tests
    def test_raises_NotFoundError_on_dataset_resolve_fail(self):
        lib = Library(self.cache, self.sqlite_db)
        lib.resolve = fudge.Fake().expects_call().returns(None)
        try:
            lib.get('ref1')
            raise AssertionError('NotFoundError was not raised')
        except NotFoundError as exc:
            self.assertIn('Failed to resolve reference', exc.message)

    def test_raises_NotFoundError_if_bundle_missed(self):
        lib = Library(self.cache, self.sqlite_db)

        ds1 = DatasetFactory()
        self.sqlite_db.session.commit()
        lib.resolve = fudge.Fake().expects_call().returns(ds1)
        lib._get_bundle_by_cache_key = fudge.Fake().expects_call().returns(None)
        try:
            lib.get('ref1')
            raise AssertionError('NotFoundError was not raised')
        except NotFoundError as exc:
            self.assertIn('Failed to get bundle from cache key', exc.message)

    # .tables property tests
    def test_contains_all_tables(self):
        self.sqlite_db.create_tables()
        lib = Library(self.cache, self.sqlite_db)

        ds1 = DatasetFactory()
        table1 = TableFactory(dataset=ds1)

        ds2 = DatasetFactory()
        table2 = TableFactory(dataset=ds2)
        self.assertIn(table1, lib.tables)
        self.assertIn(table2, lib.tables)

    # .tables_no_columns property tests
    def test_contains_all_tables_with_columns(self):
        self.sqlite_db.create_tables()
        lib = Library(self.cache, self.sqlite_db)

        ds1 = DatasetFactory()
        table1 = TableFactory(dataset=ds1)

        ds2 = DatasetFactory()
        table2 = TableFactory(dataset=ds2)
        self.assertIn(table1, lib.tables)
        self.assertIn(table2, lib.tables)

    # .table tests
    def test_returns_table_found_by_vid(self):
        self.sqlite_db.create_tables()
        lib = Library(self.cache, self.sqlite_db)

        ds1 = DatasetFactory()
        table1 = TableFactory(dataset=ds1)

        self.assertEquals(lib.table(table1.vid), table1)

    def test_returns_table_found_by_id(self):
        self.sqlite_db.create_tables()
        lib = Library(self.cache, self.sqlite_db)

        ds1 = DatasetFactory()
        table1 = TableFactory(dataset=ds1)
        assert table1.id_ != table1.vid

        self.assertEquals(lib.table(table1.id_), table1)

    # .derived_tables tests
    def test_returns_tables_found_by_proto_vid(self):
        # prepare state
        self.sqlite_db.create_tables()
        lib = Library(self.cache, self.sqlite_db)

        ds1 = DatasetFactory()
        ds2 = DatasetFactory()
        ds3 = DatasetFactory()
        table1 = TableFactory(dataset=ds1, proto_vid='1')
        table2 = TableFactory(dataset=ds2, proto_vid='1')
        table3 = TableFactory(dataset=ds3, proto_vid='2')

        # testing
        derived_tables = lib.derived_tables('1')
        self.assertEquals(len(derived_tables), 2)
        self.assertIn(table1, derived_tables)
        self.assertIn(table2, derived_tables)
        self.assertNotIn(table3, derived_tables)

    @unittest.skip('.all() method of query never raises NoResultFound error.')
    def test_raises_NotFoundError_if_tables_not_found(self):
        # prepare state
        self.sqlite_db.create_tables()
        lib = Library(self.cache, self.sqlite_db)

        # testing
        with self.assertRaises(NotFoundError):
            lib.derived_tables('1')

    # .dataset tests
    def test_returns_dataset_by_given_vid(self):
        # prepare state
        self.sqlite_db.create_tables()
        lib = Library(self.cache, self.sqlite_db)

        ds1 = DatasetFactory()

        # testing
        ds = lib.dataset(ds1.vid)
        self.assertEquals(ds, ds1)

    def test_returns_dataset_by_given_id(self):
        # prepare state
        self.sqlite_db.create_tables()
        lib = Library(self.cache, self.sqlite_db)

        ds1 = DatasetFactory()
        self.assertNotEquals(ds1.vid, ds1.id_)

        # testing
        ds = lib.dataset(ds1.id_)
        self.assertEquals(ds, ds1)

    def test_raises_no_result_found(self):
        # prepare state
        self.sqlite_db.create_tables()
        lib = Library(self.cache, self.sqlite_db)

        # testing
        with self.assertRaises(NotFoundError):
            lib.dataset('the-id')

    # .datasets tests
    def test_returns_all_datasets(self):
        # prepare state
        self.sqlite_db.create_tables()
        lib = Library(self.cache, self.sqlite_db)

        ds1 = DatasetFactory()
        ds2 = DatasetFactory()

        # testing
        datasets = lib.datasets()
        self.assertIn(ds1, datasets)
        self.assertIn(ds2, datasets)

    # .versioned_datasets tests
    def test_returns_dict_with_versioned_datasets(self):
        # prepare state
        self.sqlite_db.create_tables()
        lib = Library(self.cache, self.sqlite_db)

        ds1_id = 'dds01'
        ds1_01 = DatasetFactory(id_=ds1_id, revision=1)
        ds1_02 = DatasetFactory(id_=ds1_id, revision=2)
        ds2 = DatasetFactory()

        # testing
        datasets = lib.versioned_datasets()
        self.assertIn(ds1_id, datasets)
        # highest revision lives on top level
        self.assertEquals(datasets[ds1_id]['vid'], ds1_02.vid)
        self.assertEquals(datasets[ds1_id]['revision'], ds1_02.revision)
        self.assertIn(ds2.id_, datasets)

        # ds1 contains version with other revision
        self.assertIn(ds1_01.vid, datasets[ds1_id]['other_versions'])

    # .bundle tests
    # TODO:

    # .partition tests
    def test_returns_partition_by_vid(self):
        # prepare state
        self.sqlite_db.create_tables()
        ds1 = DatasetFactory()
        partition1 = PartitionFactory(dataset=ds1)

        lib = Library(self.cache, self.sqlite_db)

        # testing
        partition = lib.partition(partition1.vid)
        self.assertEquals(partition, partition1)

    def test_returns_partition_by_id(self):
        # prepare state
        self.sqlite_db.create_tables()
        ds1 = DatasetFactory()
        partition1 = PartitionFactory(dataset=ds1)
        self.assertNotEquals(partition1.vid, partition1.id_)

        lib = Library(self.cache, self.sqlite_db)

        # testing
        partition = lib.partition(partition1.id_)
        self.assertEquals(partition, partition1)

    def test_raises_NotFoundError_if_partition_does_not_exist(self):
        # prepare state
        self.sqlite_db.create_tables()
        ds1 = DatasetFactory()
        partition1 = PartitionFactory(dataset=ds1)
        self.assertNotEquals(partition1.vid, partition1.id_)

        lib = Library(self.cache, self.sqlite_db)

        # testing
        with self.assertRaises(NotFoundError):
            lib.partition('the-vid')

    # .dataset_partitions tests
    def test_returns_given_dataset_partitions(self):
        # prepare state
        self.sqlite_db.create_tables()
        self.sqlite_db.session.commit()

        ds1 = DatasetFactory()
        table1 = TableFactory(dataset=ds1)
        partition1 = PartitionFactory(dataset=ds1, t_id=table1.id_)
        partition2 = PartitionFactory(dataset=ds1, t_id=table1.id_)

        ds2 = DatasetFactory()
        table2 = TableFactory(dataset=ds2)
        partition3 = PartitionFactory(dataset=ds2, t_id=table2.id_)

        lib = Library(self.cache, self.sqlite_db)

        # testing
        partitions = lib.dataset_partitions(ds1.vid)
        self.assertEquals(len(partitions), 2)
        self.assertIn(partition1, partitions)
        self.assertIn(partition2, partitions)
        self.assertNotIn(partition3, partitions)

    # .partitions property tests
    def test_contains_all_partitions(self):
        # prepare state
        self.sqlite_db.create_tables()
        ds1 = DatasetFactory()
        table1 = TableFactory(dataset=ds1)
        partition1 = PartitionFactory(dataset=ds1, t_id=table1.id_)
        partition2 = PartitionFactory(dataset=ds1, t_id=table1.id_)

        ds2 = DatasetFactory()
        table2 = TableFactory(dataset=ds2)
        partition3 = PartitionFactory(dataset=ds2, t_id=table2.id_)

        lib = Library(self.cache, self.sqlite_db)

        # testing
        partitions = lib.partitions
        self.assertEquals(len(partitions), 3)
        self.assertIn(partition1, partitions)
        self.assertIn(partition2, partitions)
        self.assertIn(partition3, partitions)

    # .stores property tests
    def test_contains_all_stores(self):
        lib = Library(self.cache, self.sqlite_db)
        FileFactory(type_=Files.TYPE.STORE)
        FileFactory(type_=Files.TYPE.STORE)
        stores = lib.stores
        self.assertEquals(len(stores), 2)

    # .store tests
    def test_returns_store_file_by_ref(self):
        lib = Library(self.cache, self.sqlite_db)
        ref = 'ref1'
        file1 = FileFactory(type_=Files.TYPE.STORE, ref=ref)
        FileFactory(type_=Files.TYPE.STORE, ref='ref2')
        ret = lib.store(ref)
        self.assertIsInstance(ret, File)
        self.assertEquals(ret.oid, file1.oid)

    # .remove_store tests
    def test_removes_store(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        ref = 'ref1'
        FileFactory(type_=Files.TYPE.STORE, ref=ref)
        file2 = FileFactory(type_=Files.TYPE.STORE, ref='ref2')

        # testing
        with fudge.patched_context(Library, 'warehouse', fudge.Fake().is_a_stub()):
            lib.remove_store(ref)
            all_ = self.query(File).all()
            self.assertEquals(len(all_), 1)
        self.assertEquals(all_[0].ref, file2.ref)
