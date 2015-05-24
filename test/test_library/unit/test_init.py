# -*- coding: utf-8 -*-

import csv
import os
from StringIO import StringIO
from tempfile import mkdtemp
import unittest

from boto.exception import S3ResponseError

import ckcache

from sqlite3 import DatabaseError as SqliteDatabaseError

from ckcache.multi import AltReadCache

import fudge
from fudge.inspector import arg

from sqlalchemy.exc import OperationalError, IntegrityError
from sqlalchemy.orm.exc import NoResultFound

from ambry.bundle import DbBundle
from ambry.dbexceptions import ConfigurationError, NotFoundError,\
    DatabaseError, DependencyError, ConflictError
from ambry.identity import Identity
from ambry.library import Library, _new_library
from ambry.library.database import LibraryDb
from ambry.library.files import Files
from ambry.orm import File, Dataset, Partition
from ambry.source import SourceTree
from ambry import warehouse
from ambry import util
from ambry.warehouse.manifest import Manifest

from test.test_library.asserts import assert_spec
from test.test_library.factories import DatasetFactory, ConfigFactory,\
    TableFactory, ColumnFactory, FileFactory, PartitionFactory, CodeFactory,\
    ColumnStatFactory, WarehouseFactory


SQLITE_DATABASE = 'test_library_test_init.db'
BUNDLES_DIR_PREFIX = 'test_library_test_init_bundles'


class NewLibraryTest(unittest.TestCase):
    def setUp(self):
        self.sqlite_db = LibraryDb(driver='sqlite', dbname=SQLITE_DATABASE)
        self.sqlite_db.enable_delete = True
        self.sqlite_db.create_tables()

    def tearDown(self):
        self.sqlite_db.close()
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

    def test_logs_S3ResponseError(self):
        fake_s3 = fudge.Fake('s3').has_attr(bucket='bucket1')

        config = {
            'database': {
                'driver': 'sqlite',
                'dbname': 'test_init1_unused.db',
            },
            'filesystem': 'http://example.com',
            'remotes': {'s3': fake_s3},
            'root': 'UNUSED?'}

        fake_new_cache = fudge.Fake()\
            .expects_call().returns(fudge.Fake().is_a_stub())\
            .next_call().raises(S3ResponseError('status', 'reason'))

        fake_logger = fudge.Fake('logger')\
            .expects('error').with_args(arg.contains('Failed to init'))

        # get_logger need 3 return values, first for the LibraryDb constructor logger, second
        # for the _new_library logger, third for Library constructor.
        fake_get_logger = fudge.Fake('get_logger')\
            .expects_call().returns(fudge.Fake().is_a_stub())\
            .next_call().returns(fake_logger)\
            .next_call().returns(fudge.Fake().is_a_stub())

        with fudge.patched_context(ckcache, 'new_cache', fake_new_cache):
            with fudge.patched_context(util, 'get_logger', fake_get_logger):
                _new_library(config)

    def test_separates_host_and_port(self):

        config = {
            'database': {
                'driver': 'sqlite',
                'dbname': 'test_init1_unused.db',
            },
            'filesystem': 'http://example.com',
            'host': 'localhost:8080',
            'root': 'UNUSED?'}

        fake_new_cache = fudge.Fake()\
            .expects_call().returns(fudge.Fake().is_a_stub())\

        with fudge.patched_context(ckcache, 'new_cache', fake_new_cache):
            lib = _new_library(config)
            self.assertEquals(lib.host, 'localhost')
            self.assertEquals(lib.port, '8080')

    def test_uses_80_port_if_not_given(self):

        config = {
            'database': {
                'driver': 'sqlite',
                'dbname': 'test_init1_unused.db',
            },
            'filesystem': 'http://example.com',
            'host': 'localhost',
            'root': 'UNUSED?'}

        fake_new_cache = fudge.Fake()\
            .expects_call().returns(fudge.Fake().is_a_stub())\

        with fudge.patched_context(ckcache, 'new_cache', fake_new_cache):
            lib = _new_library(config)
            self.assertEquals(lib.host, 'localhost')
            self.assertEquals(lib.port, 80)


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
        self.sqlite_db.close()
        try:
            os.remove(SQLITE_DATABASE)
        except OSError:
            pass

        WarehouseFactory.clean()

    # .__init__ tests
    def test_raises_ConfigurationError_if_cache_not_given(self):
        cache = None
        with self.assertRaises(ConfigurationError):
            Library(cache, self.sqlite_db)

    # .clone tests
    def test_returns_cloned_library(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db, host='localhost', port='8080')

        # testing
        cloned = lib.clone()
        self.assertIsNot(cloned, lib)
        self.assertIsInstance(cloned, Library)
        self.assertEquals(lib.cache, cloned.cache)

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

    def test_synchronizes_store_warehouses(self):

        # prepare state
        lib = Library(self.cache, self.sqlite_db)

        # create stores
        FileFactory(type_=Files.TYPE.STORE)
        FileFactory(type_=Files.TYPE.STORE)

        warehouse1 = fudge.Fake('warehouse1').is_a_stub()
        warehouse2 = fudge.Fake('warehouse2').is_a_stub()

        fake_warehouse = fudge.Fake('warehouse')\
            .expects_call().returns(warehouse1)\
            .next_call().returns(warehouse2)

        fake_sync_warehouse = fudge.Fake('sync_warehouse')\
            .expects_call().with_args(warehouse1)\
            .next_call().with_args(warehouse2)

        # testing
        with fudge.patched_context(Library, 'warehouse', fake_warehouse):
            with fudge.patched_context(Library, 'sync_warehouse', fake_sync_warehouse):
                lib.warehouse_url = 'http://example.com'
        fudge.verify()

    # .put_bundle tests
    # TODO:

    # .put_partition tests
    # TODO:

    # .remove tests
    def test_removes_bundle_from_database_and_cache(self):
        # prepare state.
        # create datasets for bundles
        lib = Library(self.cache, self.sqlite_db)
        ds1 = DatasetFactory(version='0.0.1')
        ds1_vid = ds1.vid
        self.sqlite_db.session.commit()
        bundle = [x for x in lib.list_bundles(locations=None)][0]
        fake_cache = fudge.Fake()\
            .expects('remove')\
            .with_args(bundle.identity.cache_key, propagate=True)

        # test
        with fudge.patched_context(lib, 'cache', fake_cache):
            lib.remove(bundle)

        # was deleted from database
        self.assertEquals(self.query(Dataset).filter_by(vid=ds1_vid).all(), [])
        # was deleted from cache
        # TODO: implement
        fudge.verify()

    # .list tests
    # TODO:

    # .list_bundles tests
    def test_returns_last_revision_only(self):
        # prepare state.
        lib = Library(self.cache, self.sqlite_db)

        # create datasets for bundles
        ds1 = DatasetFactory(revision=1)
        ds2 = DatasetFactory(id_=ds1.id_, revision=2)
        ds1_vid = ds1.vid
        ds2_vid = ds2.vid
        self.sqlite_db.session.commit()

        # test
        bundles = [x for x in lib.list_bundles(locations=None)]
        self.assertEquals(len(bundles), 1)
        vids = [x.dataset.vid for x in bundles]
        self.assertIn(ds1_vid, vids)
        self.assertNotIn(ds2_vid, vids)

    def test_returns_all_revisions(self):
        # prepare state.
        lib = Library(self.cache, self.sqlite_db)

        # create datasets for bundles
        ds1 = DatasetFactory(revision=1)
        ds2 = DatasetFactory(id_=ds1.id_, revision=2)
        ds1_vid = ds1.vid
        ds2_vid = ds2.vid
        self.sqlite_db.session.commit()

        # test
        bundles = [x for x in lib.list_bundles(locations=None, last_version_only=False)]
        self.assertEquals(len(bundles), 2)
        vids = [x.dataset.vid for x in bundles]
        self.assertIn(ds1_vid, vids)
        self.assertIn(ds2_vid, vids)

    # .path tests
    def test_returns_cache_path_by_key(self):

        # prepare state.
        cache = fudge.Fake().expects('path').returns('the-path')
        lib = Library(cache, self.sqlite_db)

        # testing
        ret = lib.path('cache-key')
        self.assertEquals(ret, 'the-path')
        fudge.verify()

    # ._get_bundle_by_cache_key
    @fudge.patch('ckcache.multi.AltReadCache.get')
    def test_returns_false_if_path_does_not_exists(self, fake_get):
        fake_get.expects_call().returns('/bundle-path')
        lib = Library(self.cache, self.sqlite_db)
        ret = lib._get_bundle_by_cache_key('cache_key1')
        self.assertFalse(ret)

    def test_logs_DatabaseError(self):
        # prepare state

        lib = Library(self.cache, self.sqlite_db)

        lib._create_bundle = fudge.Fake()\
            .expects_call()\
            .raises(SqliteDatabaseError)
        lib.logger.error = fudge.Fake().expects_call()

        fake_get = fudge.Fake().expects_call().returns('/bundle-path')
        fake_exists = fudge.Fake().expects_call().returns(True)

        # testing
        with fudge.patched_context(os.path, 'exists', fake_exists):
            with fudge.patched_context(AltReadCache, 'get', fake_get):
                with self.assertRaises(SqliteDatabaseError):
                    lib._get_bundle_by_cache_key('cache_key1')
        fudge.verify()

    def test_logs_AttributeError(self):
        # prepare state

        lib = Library(self.cache, self.sqlite_db)

        lib._create_bundle = fudge.Fake()\
            .expects_call()\
            .raises(AttributeError)
        lib.logger.error = fudge.Fake().expects_call()

        fake_get = fudge.Fake().expects_call().returns('/bundle-path')
        fake_exists = fudge.Fake().expects_call().returns(True)

        # testing
        with fudge.patched_context(os.path, 'exists', fake_exists):
            with fudge.patched_context(AltReadCache, 'get', fake_get):
                with self.assertRaises(AttributeError):
                    lib._get_bundle_by_cache_key('cache_key1')
        fudge.verify()

    def test_logs_OperationalError(self):
        # prepare state

        lib = Library(self.cache, self.sqlite_db)

        lib._create_bundle = fudge.Fake()\
            .expects_call()\
            .raises(OperationalError('select 1;', [], 'a'))
        lib.logger.error = fudge.Fake().expects_call()

        fake_get = fudge.Fake().expects_call().returns('/bundle-path')
        fake_exists = fudge.Fake().expects_call().returns(True)

        # testing
        with fudge.patched_context(os.path, 'exists', fake_exists):
            with fudge.patched_context(AltReadCache, 'get', fake_get):
                with self.assertRaises(SqliteDatabaseError):
                    lib._get_bundle_by_cache_key('cache_key1')
        fudge.verify()

    # .has tests
    def test_asks_cache_about_dataset(self):
        # prepare state
        ds1 = DatasetFactory()
        lib = Library(self.cache, self.sqlite_db)
        ds_ident = lib.resolve(ds1.vid)
        lib.cache = fudge.Fake()\
            .expects('has')\
            .with_args(ds_ident.cache_key)\
            .returns(True)

        # testing
        self.assertTrue(lib.has(ds1.vid))
        fudge.verify()

    def test_asks_cache_about_partition(self):
        # prepare state
        ds1 = DatasetFactory()
        partition1 = PartitionFactory(dataset=ds1)
        lib = Library(self.cache, self.sqlite_db)
        ds_ident = lib.resolve(partition1.vid)
        lib.cache = fudge.Fake()\
            .expects('has')\
            .with_args(ds_ident.partition.cache_key)\
            .returns(True)

        # testing
        self.assertTrue(lib.has(partition1.vid))
        fudge.verify()

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
        tables = lib.tables_no_columns
        self.assertIn(table1, tables)
        self.assertIn(table2, tables)

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

    def test_raises_NotFoundError_if_table_not_found(self):
        print('\nTODO: .first() does not raise NoResultFound. See .table() method.\n')
        # FIXME: what is the proper behaviour? Raising or returning None?
        # Assuming returning None, because it's production code.
        self.sqlite_db.create_tables()
        lib = Library(self.cache, self.sqlite_db)

        # testing
        self.assertIsNone(lib.table('vid'))

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
        # hide log message
        with fudge.patched_context(lib, 'logger', fudge.Fake().is_a_stub()):
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

    def test_returns_store_file_by_path(self):
        lib = Library(self.cache, self.sqlite_db)
        path = 'the-path'
        file1 = FileFactory(type_=Files.TYPE.STORE, path=path)
        ret = lib.store(path)
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

    def test_logs_error_and_deletes_store_if_warehouse_not_found(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        ref = 'ref1'
        FileFactory(type_=Files.TYPE.STORE, ref=ref)
        lib.logger.error = fudge.Fake('error')\
            .expects_call()\
            .with_args(arg.contains('Didn\'t find warehouse'))

        lib.warehouse = fudge.Fake('warehouse')\
            .expects_call()\
            .raises(NotFoundError('FakeNotFound'))

        # testing
        lib.remove_store(ref)
        self.assertEquals(self.query(File).all(), [])
        fudge.verify()

    # .warehouse tests
    def test_raises_NotFoundError_if_store_not_found(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib.store = fudge.Fake().expects_call().returns(None)

        # testing
        with self.assertRaises(NotFoundError):
            lib.warehouse('uid')

    def test_creates_new_warehouse_and_returns_it(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(warehouse.database_config, ['db', 'base_dir'])
        assert_spec(warehouse.new_warehouse, ['config', 'elibrary', 'logger'])

        uid = 'ref2'
        path = 'the-path'
        FileFactory(type_=Files.TYPE.STORE, ref=uid, path=path)

        # prepare state
        lib = Library(self.cache, self.sqlite_db)

        # testing
        fudge.patch_object(
            warehouse,
            'database_config',
            fudge.Fake().expects_call().with_args(path).returns({}))

        # Returning value of the fake new_warehouse is not valid warehouse, but it does not matter here.
        fudge.patch_object(
            warehouse,
            'new_warehouse',
            fudge.Fake().expects_call().returns('WAREHOUSE'))

        ret = lib.warehouse(uid)
        self.assertEquals(ret, 'WAREHOUSE')

        fudge.verify()

    # .manifests property tests
    def test_returns_list_with_all_manifests(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        FileFactory(type_=Files.TYPE.MANIFEST)
        FileFactory(type_=Files.TYPE.MANIFEST)
        FileFactory(type_=Files.TYPE.STORE)

        # testing
        manifests = lib.manifests
        self.assertEquals(len(manifests), 2)
        self.assertEquals(manifests[0].type_, Files.TYPE.MANIFEST)
        self.assertEquals(manifests[1].type_, Files.TYPE.MANIFEST)

    # .manifest tests
    def test_returns_pair_of_none(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)

        # testing
        manifest, content = lib.manifest('the-uid')
        self.assertIsNone(manifest)
        self.assertIsNone(content)

    def test_returns_manifest_and_its_content(self):

        # prepare state
        lib = Library(self.cache, self.sqlite_db)

        uid = 'uid'
        FileFactory(type_=Files.TYPE.MANIFEST, ref=uid, content='manifest1')

        # testing
        manifest, content = lib.manifest(uid)
        self.assertIsInstance(manifest, File)
        self.assertIsInstance(content, Manifest)

    # .remove_manifest tests
    def test_raises_NotFoundError_if_manifest_not_found(self):

        # prepare state
        lib = Library(self.cache, self.sqlite_db)

        # testing
        with self.assertRaises(NotFoundError):
            lib.remove_manifest('uid')

    def test_deletes_found_manifest(self):

        # prepare state
        lib = Library(self.cache, self.sqlite_db)

        uid = 'uid'
        FileFactory(type_=Files.TYPE.MANIFEST, ref=uid, content='manifest1')

        # testing
        self.assertEquals(len(self.query(File).all()), 1)
        lib.remove_manifest(uid)
        self.assertEquals(len(self.query(File).all()), 0)

    # .find tests
    @unittest.skip('AttributeError: \'LibraryDb\' object has no attribute \'find\'')
    def test_finds_in_the_db(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        fake_find = fudge.Fake()\
            .expects_call()\
            .with_args('FAKE_QUERY')\
            .returns('FAKE_RETURN')

        # testing
        with fudge.patched_context(self.sqlite_db, 'find', fake_find):
            ret = lib.find('FAKE_QUERY')
            self.assertEquals(ret, 'FAKE_RETURN')

    # .locate tests
    def test_returns_pair_of_nones_if_ident_resolve_failed(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)

        ref = 'ref1'
        FileFactory(type_=Files.TYPE.MANIFEST, ref=ref, content='manifest1')

        # testing
        ident, location = lib.locate(ref)
        self.assertIsNone(ident)
        self.assertIsNone(location)

    def test_returns_ident_and_cache_if_ident_is_cached(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)

        fake_cache = fudge.Fake('cache').expects('has').returns(True)
        lib.cache = fake_cache

        ds1 = DatasetFactory()

        # testing
        ret1, ret2 = lib.locate(ds1.identity)
        fudge.verify()
        self.assertIsInstance(ret1, Identity)
        self.assertEquals(ret1.vid, ds1.vid)
        self.assertEquals(ret2, fake_cache)

    def test_returns_ident_and_remote_location_if_ident_is_not_cached(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib.cache = fudge.Fake('cache').expects('has').returns(False)

        # also, remote have to contain ident.
        remote1 = fudge.Fake()\
            .has_attr(priority=1)\
            .expects('has')\
            .returns(True)

        lib._remotes = {
            'remote1': remote1}
        ds1 = DatasetFactory()

        # testing
        ret1, ret2 = lib.locate(ds1.identity)
        fudge.verify()
        self.assertIsInstance(ret1, Identity)
        self.assertEquals(ret1.vid, ds1.vid)
        self.assertEquals(ret2, remote1)

    def test_returns_remote_location_with_lowest_priority(self):
        print('\nTODO: what is the reason about low priority?\n')

        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib.cache = fudge.Fake('cache').expects('has').returns(False)

        # also, remote have to contain ident.
        remote1 = fudge.Fake()\
            .has_attr(priority=5)

        remote2 = fudge.Fake()\
            .has_attr(priority=3)

        remote3 = fudge.Fake()\
            .has_attr(priority=1)\
            .expects('has')\
            .returns(True)

        lib._remotes = {
            'remote1': remote1,
            'remote2': remote2,
            'remote3': remote3}
        ds1 = DatasetFactory()

        # testing
        ret1, ret2 = lib.locate(ds1.identity)
        fudge.verify()
        self.assertIsInstance(ret1, Identity)
        self.assertEquals(ret1.vid, ds1.vid)
        self.assertEquals(ret2, remote3)

    def test_returns_ident_and_none_if_location_not_found(self):

        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib.cache = fudge.Fake('cache').expects('has').returns(False)
        lib._remotes = {}
        ds1 = DatasetFactory()

        # testing
        ret1, ret2 = lib.locate(ds1.identity)
        fudge.verify()
        self.assertIsInstance(ret1, Identity)
        self.assertEquals(ret1.vid, ds1.vid)
        self.assertIsNone(ret2)

    # .locate_one test
    # @unittest.skip('.pop() on tuple returned by locate()? Is it highest or lowest?')
    def test_returns_ident_with_highest_priority_location(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib.cache = fudge.Fake('cache').expects('has').returns(False)
        lib._remotes = {}
        ds1 = DatasetFactory()

        # testing
        lib.locate_one(ds1.identity)
        fudge.verify()

        # TODO: enhance after .locate_one or .locate tests.

    # .dep tests
    def test_raises_DependencyError_if_name_not_found_in_dependencies(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib._dependencies = {'dep1': {}}

        # testing
        raised = False
        try:
            lib.dep('dep1')
        except DependencyError as exc:
            raised = True
            self.assertIn('No dependency', exc.message)
        self.assertTrue(raised)

    def test_raises_DependencyError_if_object_ref_not_found(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib._dependencies = {'dep1': 'dep1ref'}
        lib.get = fudge.Fake().expects_call().raises(NoResultFound('msg'))

        # testing
        raised = False
        try:
            lib.dep('dep1')
        except DependencyError as exc:
            raised = True
            self.assertIn('Failed to get dependency', exc.message)
        self.assertTrue(raised)

    def test_returns_found_dependency(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib._dependencies = {'dep1': 'dep1ref'}
        # Note: Going to return wrong dependency. But it does not matter here.
        # TODO: change return value to match the return value of the .get() method (bundle?).
        lib.get = fudge.Fake().expects_call().returns('DEPENDENCY')

        # testing
        ret = lib.dep('dep1')
        self.assertEquals(ret, 'DEPENDENCY')

    def test_calls_dependency_callback(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib._dependencies = {'dep1': 'dep1ref'}
        lib.dep_cb = fudge.Fake('dep_cb').expects_call()
        # Note: Going to return wrong dependency. But it does not matter here.
        # TODO: change return value to match the return value of the .get() method (bundle?).
        lib.get = fudge.Fake().expects_call().returns('DEPENDENCY')

        # testing
        lib.dep('dep1')
        fudge.verify()

    # .dependencies property tests
    def test_contains_cached_dependencies(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib._dependencies = {'dep1': 'dep1ref'}

        # testing
        deps = lib.dependencies
        self.assertEquals(deps, {'dep1': 'dep1ref'})

    def test_caches_found_dependencies(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib._dependencies = {}
        lib._get_dependencies = fudge.Fake().expects_call().returns({'dep1': 'dep1ref'})

        # testing
        deps = lib.dependencies
        self.assertEquals(deps, {'dep1': 'dep1ref'})
        self.assertEquals(lib._dependencies, {'dep1': 'dep1ref'})

    # ._get_dependencies tests
    def test_raises_ConfigurationError_if_bundle_is_empty(self):

        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        self.assertIsNone(lib._bundle)

        # testing
        with self.assertRaises(ConfigurationError):
            lib._get_dependencies()

    def test_returns_empty_dict_is_bundle_has_no_dependencies(self):
        # TODO: Create BundleFactory and use it here
        class FakeMetadata(object):
            dependencies = {}

        class FakeBundle(object):
            metadata = FakeMetadata()

        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib._bundle = FakeBundle()

        # testing
        ret = lib._get_dependencies()
        self.assertEquals(ret, {})

    def test_returns_dict_with_resolved_dependencies_identities(self):
        # TODO: Create BundleFactory and use it here
        ds1 = DatasetFactory()
        ds2 = DatasetFactory()

        class FakeMetadata(object):
            dependencies = {'dataset1': ds1.vid, 'dataset2': ds2.vid}

        class FakeBundle(object):
            metadata = FakeMetadata()

        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib._bundle = FakeBundle()

        # testing
        ret = lib._get_dependencies()
        self.assertIn('dataset1', ret)
        self.assertIn('dataset2', ret)

        self.assertEquals(ret['dataset1'].vid, ds1.vid)
        self.assertEquals(ret['dataset2'].vid, ds2.vid)

    def test_raises_DependencyError_if_dependency_resolve_failed(self):
        # TODO: Create BundleFactory and use it here

        class FakeMetadata(object):
            dependencies = {'missed-dep': 'missed-dep'}

        class FakeBundle(object):
            metadata = FakeMetadata()

        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib._bundle = FakeBundle()

        # testing
        with self.assertRaises(DependencyError):
            lib._get_dependencies()

    # .check_dependencies tests
    def test_returns_empty_dict_if_all_dependencies_exist(self):
        ds1 = DatasetFactory()
        ds2 = DatasetFactory()

        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib._dependencies = {'dataset1': ds1.vid, 'dataset2': ds2.vid}

        # testing
        ret = lib.check_dependencies(download=False)
        self.assertEquals(ret, {})

    def test_raises_NotFoundError_if_resolve_failed(self):

        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib._dependencies = {'dataset1': 'missed-vid'}

        # testing
        with self.assertRaises(NotFoundError):
            lib.check_dependencies(download=False)

    def test_gets_bundle_and_raises_NotFoundError_on_faile(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(Library.get, ['self', 'ref', 'remote', 'force', 'cb', 'location'])

        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib._dependencies = {'dataset1': 'vid1'}
        fake_get = fudge.Fake()\
            .expects_call()\
            .with_args('vid1', cb=arg.any())\
            .returns(None)

        # testing
        with fudge.patched_context(lib, 'get', fake_get):
            with self.assertRaises(NotFoundError):
                lib.check_dependencies(download=True)

    # .source property tests
    def test_returns_source_tree(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(
            SourceTree.__init__,
            ['self', 'base_dir', 'library', 'logger'])

        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib.source_dir = 'the-dir'

        fake_init = fudge.Fake('init')\
            .expects_call()\
            .with_args(lib.source_dir, lib, lib.logger)

        # testing
        with fudge.patched_context(SourceTree, '__init__', fake_init):
            source = lib.source
            self.assertIsInstance(source, SourceTree)

    # .new_files property tests
    def test_generates_new_installed_files(self):

        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        # add two installed files with new state.
        FileFactory(type_=Files.TYPE.BUNDLE, state='new')
        FileFactory(type_=Files.TYPE.PARTITION, state='new')

        # add not installed
        FileFactory(type_=Files.TYPE.STORE, state='new')

        # add with another state
        FileFactory(type_=Files.TYPE.BUNDLE)

        # testing
        new_files_gen = lib.new_files

        # is generator
        self.assertTrue(hasattr(new_files_gen, 'next'))
        new_files = [x for x in new_files_gen]
        self.assertEquals(len(new_files), 2)
        self.assertEquals(new_files[0].state, 'new')
        self.assertEquals(new_files[1].state, 'new')

    # .push tests
    def test_raises_exception_if_upstream_is_empty(self):

        # prepare state
        lib = Library(self.cache, self.sqlite_db)

        # testing
        raised = False
        try:
            lib.push(None)
        except Exception as exc:
            raised = True
            self.assertIn('without defining a upstream.', exc.message)
        self.assertTrue(raised)

    # .sync_library tests
    @fudge.patch('os.walk')
    def test_removes_bundles_and_partitions_from_db(self, fake_walk):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib.cache = fudge.Fake().has_attr(cache_dir='fake')
        fake_walk.expects_call()\
            .returns([])
        FileFactory(type_=Files.TYPE.BUNDLE)
        FileFactory(type_=Files.TYPE.PARTITION)

        # testing
        lib.sync_library(clean=True)
        self.assertEquals(self.query(File).all(), [])

    def test_logs_failed_to_open_bundle(self):

        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib.cache = fudge.Fake().has_attr(cache_dir='fake')
        dirpath1 = 'dirpath'
        dirnames1 = ['dir1', 'dir2']
        filenames1 = ['file1.db', 'file2']
        walk_return = [(dirpath1, dirnames1, filenames1)]
        lib._create_bundle = fudge.Fake()\
            .expects_call()\
            .with_args(arg.contains('file1.db'))
        lib.logger.error = fudge.Fake()\
            .expects_call()\
            .with_args(arg.contains('Failed to open bundle'))

        fake_walk = fudge.Fake().expects_call()\
            .returns(walk_return)

        # testing
        with fudge.patched_context(os, 'walk', fake_walk):
            lib.sync_library()
        fudge.verify()

    def test_installs_found_bundles(self):

        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(
            Library.put_bundle,
            ['self', 'bundle', 'source', 'install_partitions', 'file_state', 'commit'])
        assert_spec(
            Library.put_partition,
            ['self', 'partition', 'source', 'file_state', 'commit'])
        assert_spec(
            Library._create_bundle,
            ['self', 'path'])

        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib.cache = fudge.Fake()\
            .has_attr(repo_id='repo-id', cache_dir='cache-dir')

        # mocking os.walk
        dirpath1 = 'dirpath'
        dirnames1 = ['dir1', 'dir2']
        filenames1 = ['file1.db', 'file2']
        walk_return = [(dirpath1, dirnames1, filenames1)]
        fake_walk = fudge.Fake().expects_call()\
            .returns(walk_return)

        # mocking _create_bundle
        # TODO: Create BundleFactory and use it here
        class FakePartitions(list):
            count = 1
            identity = {}

        fake_identity = fudge.Fake().has_attr(is_bundle=True, vname='vname')

        class FakeBundle(object):
            identity = fake_identity
            partitions = FakePartitions('abc')  # It is wrong partitions, but it does not matter here.

            def close(self):
                pass

        fake_bundle1 = FakeBundle()
        lib._create_bundle = fudge.Fake()\
            .expects_call()\
            .with_args(arg.contains('file1.db'))\
            .returns(fake_bundle1)

        lib.put_bundle = fudge.Fake().expects_call()
        lib.put_partition = fudge.Fake().expects_call()

        # testing
        with fudge.patched_context(os, 'walk', fake_walk):
            ret = lib.sync_library()
            self.assertEquals(ret, [fake_bundle1])
        fudge.verify()

    # .sync_remotes tests
    # TODO:

    # .sync_source tests
    def test_deletes_all_sources_from_db(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db, source_dir='the-dir')

        FileFactory(type_=Dataset.LOCATION.SOURCE)
        fake_source = fudge.Fake()\
            .expects('_dir_list')\
            .returns({})

        # testing
        with fudge.patched_context(Library, 'source', fake_source):
            lib.sync_source(clean=True)
            self.assertEquals(self.query(File).all(), [])

    def test_synces_all_identities(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db, source_dir='the-dir')
        ds1 = DatasetFactory()

        fake_source = fudge.Fake()\
            .expects('_dir_list')\
            .returns({'ident1': ds1.identity})
        fake_sync = fudge.Fake('sync_source_dir')\
            .expects_call()\
            .with_args(
                arg.passes_test(lambda x: x.vid == ds1.vid),
                ds1.identity.bundle_path)

        # testing
        with fudge.patched_context(Library, 'source', fake_source):
            with fudge.patched_context(Library, 'sync_source_dir', fake_sync):
                lib.sync_source()

    def test_logs_all_errors_while_syncing(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db, source_dir='the-dir')
        ds1 = DatasetFactory()

        fake_source = fudge.Fake()\
            .expects('_dir_list')\
            .returns({'ident1': ds1.identity})

        fake_sync = fudge.Fake('sync_source_dir')\
            .expects_call()\
            .raises(Exception('My fake exc'))

        fake_logger = fudge.Fake('logger')\
            .expects('error')\
            .with_args(arg.contains('Failed to sync'))

        # testing
        with fudge.patched_context(Library, 'source', fake_source):
            with fudge.patched_context(Library, 'sync_source_dir', fake_sync):
                with fudge.patched_context(lib, 'logger', fake_logger):
                    lib.sync_source()

    # .sync_source_dir tests
    def test_installs_dataset_identity(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        ds1 = DatasetFactory()
        self.sqlite_db.install_dataset_identity = fudge.Fake()\
            .expects_call()\
            .with_args(arg.passes_test(lambda x: x.vid == ds1.vid))

        # testing
        with fudge.patched_context(Library, 'source', fudge.Fake().is_a_stub()):
            with fudge.patched_context(Library, 'files', fudge.Fake().is_a_stub()):
                lib.sync_source_dir(ds1.identity, 'the-path')
                fudge.verify()

    def test_rollbacks_library_database_on_ConflictError(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        ds1 = DatasetFactory()
        self.sqlite_db.install_dataset_identity = fudge.Fake('install_dataset_identity')\
            .expects_call()\
            .raises(ConflictError('msg'))
        self.sqlite_db.rollback = fudge.Fake('rollback')\
            .expects_call()

        # testing
        with fudge.patched_context(Library, 'source', fudge.Fake().is_a_stub()):
            with fudge.patched_context(Library, 'files', fudge.Fake().is_a_stub()):
                lib.sync_source_dir(ds1.identity, 'the-path')
                fudge.verify()

    def test_rollbacks_library_database_on_IntegrityError(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        ds1 = DatasetFactory()
        self.sqlite_db.install_dataset_identity = fudge.Fake('install_dataset_identity')\
            .expects_call()\
            .raises(IntegrityError('select 1;', [], 'a'))
        self.sqlite_db.rollback = fudge.Fake('rollback')\
            .expects_call()

        # testing
        with fudge.patched_context(Library, 'source', fudge.Fake().is_a_stub()):
            with fudge.patched_context(Library, 'files', fudge.Fake().is_a_stub()):
                lib.sync_source_dir(ds1.identity, 'the-path')
                fudge.verify()

    def test_installs_bundle_source(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(
            SourceTree.bundle,
            ['self', 'path', 'buildbundle_ok'])
        assert_spec(
            Files.install_bundle_source,
            ['self', 'bundle', 'source', 'commit'])

        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        ds1 = DatasetFactory()
        self.sqlite_db.install_dataset_identity = fudge.Fake('install_dataset_identity')\
            .is_a_stub()

        fake_bundle = fudge.Fake('bundle').is_a_stub()
        fake_source = fudge.Fake('source')\
            .expects('bundle')\
            .returns(fake_bundle)

        fake_files = fudge.Fake()\
            .expects('install_bundle_source')\
            .with_args(fake_bundle, fake_source, commit=True)

        # testing

        with fudge.patched_context(Library, 'source', fake_source):
            with fudge.patched_context(Library, 'files', fake_files):
                lib.sync_source_dir(ds1.identity, 'the-path')
                fudge.verify()

    def test_rollbacks_database_on_IntegrityError_raised_while_bundle_installing(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(
            SourceTree.bundle,
            ['self', 'path', 'buildbundle_ok'])
        assert_spec(
            Files.install_bundle_source,
            ['self', 'bundle', 'source', 'commit'])

        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        ds1 = DatasetFactory()
        self.sqlite_db.install_dataset_identity = fudge.Fake('install_dataset_identity')\
            .is_a_stub()

        fake_bundle = fudge.Fake('bundle').is_a_stub()
        fake_source = fudge.Fake('source')\
            .expects('bundle')\
            .returns(fake_bundle)

        fake_files = fudge.Fake()\
            .expects('install_bundle_source')\
            .raises(IntegrityError('select 1;', [], 'a'))

        self.sqlite_db.rollback = fudge.Fake('rollback')\
            .expects_call()

        # testing

        with fudge.patched_context(Library, 'source', fake_source):
            with fudge.patched_context(Library, 'files', fake_files):
                lib.sync_source_dir(ds1.identity, 'the-path')
                fudge.verify()

    # .sync_warehouse tests
    def test_installs_datastore(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(
            Files.install_data_store,
            ['self', 'w', 'name', 'title', 'summary', 'cache', 'url', 'commit'])

        # prepare state
        lib = Library(self.cache, self.sqlite_db)

        warehouse1 = WarehouseFactory(
            database=self.sqlite_db)

        fake_install = fudge.Fake('install_data_store').expects_call()\
            .with_args(
                warehouse1, name=warehouse1.name, title=warehouse1.title,
                url={}, summary=warehouse1.summary)

        # testing

        with fudge.patched_context(Files, 'install_data_store', fake_install):
            lib.sync_warehouse(warehouse1)

    def test_links_store_and_partitions(self):

        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        self.sqlite_db.commit()

        # add partition to the library
        l_partition1 = PartitionFactory()

        warehouse1 = WarehouseFactory(
            database=self.sqlite_db)

        # add partitions to the warehouse library
        PartitionFactory._meta.sqlalchemy_session = warehouse1.library.database.session
        w_partition1 = PartitionFactory()

        fake_store = fudge.Fake()\
            .expects('link_partition')\
            .with_args(l_partition1)

        fake_install = fudge.Fake('install_data_store').expects_call().returns(fake_store)

        fake_partition = fudge.Fake('Library.partition()')\
            .expects_call()\
            .with_args(w_partition1.vid)\
            .returns(l_partition1)

        # fake link_store for both - dataset and partition.
        fake_link = fudge.Fake('link_store')\
            .expects_call()\
            .with_args(fake_store)\
            .next_call()\
            .with_args(fake_store)\

        # testing

        with fudge.patched_context(Files, 'install_data_store', fake_install):
            with fudge.patched_context(lib, 'partition', fake_partition):
                with fudge.patched_context(Partition, 'link_store', fake_link):
                    with fudge.patched_context(Dataset, 'link_store', fake_link):
                        lib.sync_warehouse(warehouse1)
        fudge.verify()

    def test_links_manifests(self):

        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        self.sqlite_db.commit()

        # add partition to the library
        local_manifest = FileFactory(type_=Files.TYPE.MANIFEST, data={})

        warehouse1 = WarehouseFactory(
            database=self.sqlite_db)

        # add manifests to the warehouse library
        FileFactory._meta.sqlalchemy_session = warehouse1.library.database.session
        FileFactory(type_=Files.TYPE.MANIFEST)

        fake_store = fudge.Fake().is_a_stub()
        fake_install = fudge.Fake('install_data_store').expects_call().returns(fake_store)

        # fake link_store for both - dataset and partition.
        fake_link = fudge.Fake('link_store')\
            .expects_call()\
            .with_args(fake_store)\
            .next_call()\
            .with_args(fake_store)\

        fake_install_manifest = fudge.Fake().expects_call().returns(local_manifest)

        # testing

        with fudge.patched_context(Files, 'install_data_store', fake_install):
            with fudge.patched_context(Files, 'install_manifest', fake_install_manifest):
                with fudge.patched_context(File, 'link_store', fake_link):
                    lib.sync_warehouse(warehouse1)
        fudge.verify()

    # .sync_warehouses tests
    def test_synces_all_warehouses(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib.warehouse = fudge.Fake('warehouse').expects_call()
        lib.sync_warehouse = fudge.Fake('sync_warehouse').expects_call()
        FileFactory(type_=Files.TYPE.STORE)
        FileFactory(type_=Files.TYPE.STORE)

        # testing
        lib.sync_warehouses()
        fudge.verify()

    # .warehouse_cache property tests
    def test_contains_warehouse_cache(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        lib._warehouse_cache = 'THE-CACHE'

        # testing
        self.assertEquals(lib.warehouse_cache, 'THE-CACHE')

    # ._gen_schema test
    def test_returns_dump_of_the_schema(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)

        # testing
        ret = lib._gen_schema()

        # it's a generator
        self.assertTrue(hasattr(ret, 'next'))

    # .schema_as_csv tests
    def test_returns_content_of_the_csv(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)

        def my_gen():
            # first yield header
            yield ['table', 'col1', 'col2']
            for i in range(10):
                # now yield rows
                yield {'col1': str(i), 'col2': str(i), 'table': 'table%s' % i}

        lib._gen_schema = fudge.Fake().expects_call().calls(my_gen)

        # testing
        ret = lib.schema_as_csv()
        schema_reader = csv.reader(StringIO(ret))
        header = schema_reader.next()
        self.assertEquals(header, ['table', 'col1', 'col2'])

    # .info property tests
    def test_contains_verbose_info_string(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)

        # testing
        info = lib.info
        self.assertIn('Library', info)
        self.assertIn('Database', info)
        self.assertIn('Cache', info)
        self.assertIn('Remotes', info)

    # .dict property tests
    def test_contains_dict_with_library_info(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)

        # testing
        ret = lib.dict
        self.assertIn('name', ret)
        self.assertIn('database', ret)
        # TODO: enhance

    # .summary_dict property tests
    def test_contains_summary_dict(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)

        # testing
        ret = lib.summary_dict
        self.assertIn('name', ret)
        self.assertIn('database', ret)
        self.assertIn('cache', ret)

    # .schema_dict property tests
    def test_contains_dict_with_tables(self):
        # prepare state
        lib = Library(self.cache, self.sqlite_db)
        ds1 = DatasetFactory()
        table1 = TableFactory(dataset=ds1)

        # testing
        dct = lib.schema_dict
        self.assertIn(table1.vid, dct)
