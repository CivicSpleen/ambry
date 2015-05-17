# -*- coding: utf-8 -*-

import unittest

import fudge

from ambry.library.doccache import DocCache


class DocCacheTest(unittest.TestCase):

    def setUp(self):
        self.fake_library = fudge.Fake().is_a_stub()

        # use cache with dict instead of DictCache
        self.fake_library._doc_cache = False

    # _munge_key tests
    def test_uses_given_key_and_deletes_it_from_kwargs(self):
        dc = DocCache(self.fake_library)
        key, args, kwargs = dc._munge_key(_key='my-key')
        self.assertIn('my-key', key)
        self.assertNotIn('_key', kwargs)

    def test_uses_given_key_to_create_prefix(self):
        dc = DocCache(self.fake_library)
        key, args, kwargs = dc._munge_key(_key='my-key')
        self.assertEquals('m/y/my-key', key)

    def test_adds_given_prefix_to_the_key_and_deletes_prefix_from_kwargs(self):
        dc = DocCache(self.fake_library)
        key, args, kwargs = dc._munge_key(_key='my-key', _key_prefix='prefix')
        self.assertEquals('prefix/m/y/my-key', key)
        self.assertNotIn('_key_prefix', kwargs)

    def test_uses_argument_names_to_create_key(self):
        dc = DocCache(self.fake_library)
        key, args, kwargs = dc._munge_key('arg1', 'arg2')
        self.assertEquals('a/r/arg1_arg2', key)

        # it should returns the same args
        self.assertEquals(len(args), 2)
        self.assertIn('arg1', args)
        self.assertIn('arg2', args)

    def test_uses_optional_argument_names_to_create_key(self):
        dc = DocCache(self.fake_library)
        key, args, kwargs = dc._munge_key(kwarg1='kwarg1', kwarg2='kwarg2')
        self.assertEquals('k/w/kwarg1_kwarg2', key)

        # it should returns the same args
        self.assertEquals(len(kwargs), 2)
        self.assertIn('kwarg1', kwargs)
        self.assertIn('kwarg2', kwargs)

    def test_uses_upper_prefixes(self):
        dc = DocCache(self.fake_library)
        dc.prefix_upper = True
        key, args, kwargs = dc._munge_key(_key='TheKey')
        self.assertEquals('_/T/_The_Key', key)

    # .cache tests
    def test_adds_return_value_to_the_cache_if_cache_is_empty(self):
        dc = DocCache(self.fake_library)
        self.assertEquals(dc._cache, {})
        dc.cache(sorted, [1, 2], reverse=True)

        # reverted list is in the cache.
        self.assertIn([2, 1], dc._cache.values())

    # .cache tests
    def test_adds_return_value_to_the_cache_if_cache_is_ignored(self):
        dc = DocCache(self.fake_library)
        dc.ignore_cache = True

        # cache is empty
        self.assertEquals(dc._cache, {})

        dc.cache(sorted, [1, 2], reverse=True)

        key = dc._munge_key([1, 2], reverse=True)[0]

        self.assertIn(key, dc._cache)
        dc._cache[key] = [4, 3, 2, 1]

        # force adding
        dc.cache(sorted, [1, 2], reverse=True)
        self.assertEquals(dc._cache[key], [2, 1])

    def test_returns_value_from_cache(self):
        dc = DocCache(self.fake_library)

        key = dc._munge_key([1, 2], reverse=True)[0]
        dc._cache[key] = [3, 2, 1]

        ret = dc.cache(sorted, [1, 2], reverse=True)
        self.assertEquals(ret, [3, 2, 1])

    # .clean tests
    @fudge.patch(
        'ckcache.dictionary.DictCache.clean')
    def test_cleans_dictcache(self, fake_clean):
        fake_clean.expects_call()
        # force cache to use DictCache
        self.fake_library._doc_cache = True
        dc = DocCache(self.fake_library)
        dc.clean()

    def test_cleans_dictionary(self):
        dc = DocCache(self.fake_library)
        dc._cache = {'a': 'b'}
        dc.clean()
        self.assertEquals(dc._cache, {})

    # .remove tests
    def test_removes_given_call_from_cache(self):
        dc = DocCache(self.fake_library)

        key = dc._munge_key([1, 2], reverse=True)[0]
        dc._cache[key] = [2, 1]

        dc.remove([1, 2], reverse=True)
        self.assertNotIn(key, dc._cache)

    # .compiled_times tests
    @unittest.skip('Is times deque deprecated?')
    def test_returns_times_sorted_by_time(self):
        dc = DocCache(self.fake_library)

        dc.cache(sorted, [1, 2], reverse=True)
        dc.cache(sorted, [1, 2], reverse=True)
        dc.cache(sorted, [1, 2, 3], reverse=False)

        dc.times[0].time = 0.1
        dc.times[1].time = 0.5
        dc.times[2].time = 0.3

        ret = dc.compiled_times()
        self.assertEquals(ret[0].time, 0.5)
        self.assertEquals(ret[1].time, 0.3)
        self.assertEquals(ret[2].time, 0.1)

    # .library_info tests
    def test_adds_library_info_to_the_cache(self):
        self.fake_library.summary_dict = {'a': 'b'}
        dc = DocCache(self.fake_library)
        dc.library_info()
        self.assertIn('l/i/library_info', dc._cache)
        self.assertEquals(dc._cache['l/i/library_info'], self.fake_library.summary_dict)

    # .bundle_index tests
    def test_adds_bundle_index_to_the_cache(self):
        self.fake_library.versioned_datasets = lambda: ['a', 'b']
        dc = DocCache(self.fake_library)
        dc.bundle_index()
        self.assertIn('b/u/bundle_index', dc._cache)
        self.assertEquals(
            dc._cache['b/u/bundle_index'],
            self.fake_library.versioned_datasets())

    # .dataset tests
    def test_adds_dataset_call_to_the_cache(self):

        class FakeDataset(object):
            dict = {
                'title': 'title1',
                'summary': 'summary1'}

        self.fake_library.dataset = lambda x: FakeDataset()

        dc = DocCache(self.fake_library)
        dc.dataset('vid')
        # TODO: is using _key_prefix as key suffix valid behaviour?
        self.assertIn('ds/v/i/vidds', dc._cache)
        self.assertEquals(dc._cache['ds/v/i/vidds'], FakeDataset().dict)

    # .bundle_summary tests
    def test_adds_bundle_summary_to_the_cache(self):

        class FakeBundle(object):
            summary_dict = {'a': 'b'}

        self.fake_library.bundle = lambda x: FakeBundle()

        dc = DocCache(self.fake_library)
        dc.bundle_summary('vid')

        # TODO: is using _key_prefix as key suffix valid behaviour?
        self.assertIn('bs/v/i/vidbs', dc._cache)
        self.assertEquals(dc._cache['bs/v/i/vidbs'], FakeBundle().summary_dict)

    # .bundle tests
    def test_adds_bundle_to_the_cache(self):

        class FakeBundle(object):
            dict = {'a': 'b'}

        self.fake_library.bundle = lambda x: FakeBundle()

        dc = DocCache(self.fake_library)
        dc.bundle('vid')

        self.assertIn('v/i/vid', dc._cache)
        self.assertEquals(dc._cache['v/i/vid'], FakeBundle().dict)

    # .partition tests
    def test_adds_partition_to_the_cache(self):

        class FakePartition(object):
            dict = {'a': 'b'}

        self.fake_library.partition = lambda x: FakePartition()

        dc = DocCache(self.fake_library)
        dc.partition('vid')

        self.assertIn('v/i/vid', dc._cache)
        self.assertEquals(dc._cache['v/i/vid'], FakePartition().dict)

    # .table tests
    def test_adds_table_to_the_cache(self):

        class FakeTable(object):
            nonull_col_dict = {'a': 'b'}

        self.fake_library.table = lambda x: FakeTable()

        dc = DocCache(self.fake_library)
        dc.table('vid')

        self.assertIn('v/i/vid', dc._cache)
        self.assertEquals(dc._cache['v/i/vid'], FakeTable().nonull_col_dict)

    def test_adds_warehouse_to_the_cache(self):

        class FakeWarehouse(object):
            dict = {'a': 'b'}

        self.fake_library.warehouse = lambda x: FakeWarehouse()

        dc = DocCache(self.fake_library)
        dc.warehouse('vid')

        self.assertIn('v/i/vid', dc._cache)
        self.assertEquals(dc._cache['v/i/vid'], FakeWarehouse().dict)

    def test_adds_manifest_to_the_cache(self):

        class FakeManifest(object):
            dict = {'a': 'b'}

        def fake_manifest(vid):
            return '', FakeManifest()

        self.fake_library.manifest = fake_manifest

        dc = DocCache(self.fake_library)
        dc.manifest('vid')

        self.assertIn('v/i/vid', dc._cache)
        self.assertEquals(dc._cache['v/i/vid'], FakeManifest().dict)

    # .table_version_map
    def test_adds_table_version_map_to_the_cache(self):

        class FakeTable(object):
            def __init__(self, id_, vid):
                self.id_ = id_
                self.vid = vid

        self.fake_library.tables_no_columns = [
            FakeTable('1', 'vid1'),
            FakeTable('2', 'vid2'),
            FakeTable('3', 'vid3'),
            FakeTable('3', 'vid4')]

        dc = DocCache(self.fake_library)
        dc.table_version_map()

        self.assertIn('t/a/table_version_map', dc._cache)
        self.assertIn('1', dc._cache['t/a/table_version_map'])
        self.assertIn('2', dc._cache['t/a/table_version_map'])
        self.assertIn('3', dc._cache['t/a/table_version_map'])

        self.assertEquals(dc._cache['t/a/table_version_map']['1'], ['vid1'])
        self.assertEquals(dc._cache['t/a/table_version_map']['2'], ['vid2'])
        self.assertEquals(dc._cache['t/a/table_version_map']['3'], ['vid3', 'vid4'])
