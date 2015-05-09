# -*- coding: utf-8 -*-

import os
from tempfile import mkdtemp
import unittest
from .factories import DatasetFactory

from ambry.bundle.bundle import DbBundle
from ambry.library import Library

import fudge

from ambry.library.database import LibraryDb
from ambry.dbexceptions import NotFoundError

from sqlalchemy.exc import OperationalError

from ambry.dbexceptions import DatabaseError
from ambry.library import _new_library


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

        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session

        # cache is directory name where to store packaged bundles.
        self.cache = self.__class__.cache

    def tearDown(self):
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
    # TODO:

    # .commit tests
    # TODO:

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

    # .warehouse_url tests
    # TODO:

    # .put_bundle tests
    # TODO:

    # .put_partition tests
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
        fake_get.expects_call().returns('does-not-exist')
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
