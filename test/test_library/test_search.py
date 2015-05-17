# -*- coding: utf-8 -*-
from math import log
import os
import shutil
import unittest
from tempfile import mkdtemp

from whoosh import index

from geoid.civick import GVid

import fudge

from ambry.library import Library
from ambry.library.database import LibraryDb
from ambry.library.search import SearchTermParser, SearchResult, Search

from .factories import DatasetFactory, PartitionFactory
from .helpers import assert_spec

BUNDLES_DIR_PREFIX = 'test_library_test_search_bundles'
DOC_CACHE_DIR_PREFIX = 'test_library_test_search_doc_cache'


class SearchResultTest(unittest.TestCase):

    # .score property tests
    def test_contains_computed_score(self):
        res1 = SearchResult()
        B_SCORE = 10
        P_SCORE = 8
        res1.b_score = B_SCORE
        res1.p_score = P_SCORE

        expected = B_SCORE + log(P_SCORE)

        self.assertEquals(res1.score, expected)


class SearchTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cache = mkdtemp(prefix=BUNDLES_DIR_PREFIX)
        cls.doc_cache = mkdtemp(prefix=DOC_CACHE_DIR_PREFIX)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache)
        shutil.rmtree(cls.doc_cache)

    def setUp(self):
        SQLITE_DATABASE = 'test_library_test_search.db'
        self.sqlite_db = LibraryDb(driver='sqlite', dbname=SQLITE_DATABASE)
        self.sqlite_db.enable_delete = True
        self.sqlite_db.create_tables()

        # Each factory requires sqlalchemy session.
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        PartitionFactory._meta.sqlalchemy_session = self.sqlite_db.session

        doc_cache = self._get_cache()
        self.lib = Library(
            self.__class__.cache, self.sqlite_db,
            doc_cache=doc_cache)

    # helpers
    def _get_cache(self):
        def fake_path(path, propagate=False, missing_ok=True):
            # replacement for the cache.path
            if path == 'search/dataset':
                dir_ = 'i_index'
            elif path == 'search/identifiers':
                dir_ = 'd_index'
            else:
                raise Exception('Do not know how to handle %s path' % path)
            return os.path.join(self.__class__.doc_cache, dir_)

        doc_cache = fudge.Fake('doc_cache').provides('path').calls(fake_path)
        return doc_cache

    # .reset tests
    @fudge.patch(
        'os.path.exists',
        'shutil.rmtree')
    def test_removes_index_dir(self, fake_exists, fake_rmtree):
        # prepare state.
        search = Search(self.lib)
        fake_exists.expects_call().with_args(search.d_index_dir).returns(True)
        fake_rmtree.expects_call().with_args(search.d_index_dir)

        # testing
        search.reset()
        self.assertIsNone(search._dataset_index)

    # .get_or_new_index tests
    @fudge.patch('os.path.exists', 'os.makedirs')
    def test_creates_new_index_if_path_does_not_exist(self, fake_exists, fake_makedirs):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(index.create_in, ['dirname', 'schema', 'indexname'])

        # prepare state.
        SCHEMA = 'schema'
        DIR = 'the-dir'

        # We have to create Search instance before mocking because __init__ uses os modules.
        search = Search(self.lib)

        fake_exists.expects_call().with_args(DIR).returns(False)
        fake_makedirs.expects_call().with_args(DIR)
        fake_create_in = fudge.Fake().expects_call().with_args(DIR, SCHEMA)

        # testing
        with fudge.patched_context(index, 'create_in', fake_create_in):
            search.get_or_new_index(SCHEMA, DIR)

    @fudge.patch('os.path.exists')
    def test_opens_existing_index_if_path_exists(self, fake_exists):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(index.open_dir, ['dirname', 'indexname', 'readonly', 'schema'])

        # prepare state.
        SCHEMA = 'schema'

        # We have to create Search instance before mocking because __init__ uses os modules.
        search = Search(self.lib)

        fake_exists.expects_call().with_args(search.d_index_dir).returns(True)
        fake_open_dir = fudge.Fake().expects_call().with_args(search.d_index_dir)

        # testing
        with fudge.patched_context(index, 'open_dir', fake_open_dir):
            search.get_or_new_index(SCHEMA, search.d_index_dir)

    # .index_datasets tests
    def test_indexes_library_datasets(self):

        # prepare state.
        DatasetFactory()
        DatasetFactory()

        search = Search(self.lib)
        search.index_dataset = fudge.Fake().expects_call()

        # testing
        with fudge.patched_context(search, 'all_datasets', []):
            search.index_datasets()

    # .datasets property tests
    # TODO:

    # .search_datasets_tests
    def test_returns_dict_with_datasets_found_by_searcher(self):

        class MyDict(dict):
            pass

        class FakeSearcher(object):
            def search(self, query, limit=20):
                # returns result of the search need by search_datasets.
                result1 = MyDict({'vid': 'vid1', 'bvid': 'bvid1', 'type': 'type1'})
                result1.score = 0.5
                result2 = MyDict({'vid': 'vid2', 'bvid': 'bvid2', 'type': 'b'})
                result2.score = 0.6
                return [result1, result2]

            def __enter__(self, *args, **kwargs):
                return self

            def __exit__(self, *args, **kwargs):
                pass

        class FakeIdentifierIndex(object):
            schema = '?'

            def searcher(*args, **kwargs):
                return FakeSearcher()

        search = Search(self.lib)
        search._dataset_index = FakeIdentifierIndex()
        ret = search.search_datasets('about me')

        self.assertIsInstance(ret, dict)
        self.assertIn('bvid1', ret)
        self.assertIn('bvid2', ret)

        # scores copied properly
        self.assertEquals(ret['bvid1'].p_score, 0.5)
        self.assertEquals(ret['bvid1'].b_score, 0)

        self.assertEquals(ret['bvid2'].p_score, 0)
        self.assertEquals(ret['bvid2'].b_score, 0.6)

    # .make_query_from_terms tests
    def test_converts_about_to_terms(self):
        search = Search(self.lib)
        cterms = search.make_query_from_terms({'about': 'Beslan'})
        expected = '( type:b AND doc:(Beslan) ) OR ( type:p AND doc:(Beslan) )'
        self.assertEquals(cterms, expected)

    def test_converts_with_to_terms(self):
        search = Search(self.lib)
        cterms = search.make_query_from_terms({'with': 'Beslan'})
        expected = '( type:p AND doc:(Beslan) )'
        self.assertEquals(cterms, expected)

    def test_converts_in_to_terms(self):
        search = Search(self.lib)
        cterms = search.make_query_from_terms({'in': 'Beslan, Farn'})
        expected = '( type:p AND keywords:(Beslan, Farn) )'
        self.assertEquals(cterms, expected)

    def test_converts_source_to_terms(self):
        search = Search(self.lib)
        cterms = search.make_query_from_terms({'source': 'Beslan'})
        expected = ' (type:b AND keywords:Beslan ) AND '
        self.assertEquals(cterms, expected)

    def test_converts_by_to_terms(self):
        search = Search(self.lib)
        cterms = search.make_query_from_terms({'by': 'Beslan'})
        expected = '( type:p AND keywords:(Beslan) )'
        self.assertEquals(cterms, expected)

    def test_converts_string_to_terms(self):
        search = Search(self.lib)
        cterms = search.make_query_from_terms('about Beslan')
        expected = '( type:b AND doc:(beslan) ) OR ( type:p AND doc:(beslan) )'
        self.assertEquals(cterms, expected)

    def test_joins_terms_with_or(self):
        search = Search(self.lib)
        cterms = search.make_query_from_terms({'by': 'Beslan', 'about': 'Beslan'})
        expected = '( type:b AND doc:(Beslan) ) OR ( type:p AND keywords:(Beslan) AND doc:(Beslan) )'
        self.assertEquals(cterms, expected)

    # .search_partitions tests
    def test_generates_vids_of_the_found_docs(self):

        class MyDict(dict):
            pass

        class FakeSearcher(object):
            def search(self, query, limit=20):
                # returns result of the search need by search_datasets.
                result1 = MyDict({'vid': 'vid1', 'bvid': 'bvid1', 'type': 'type1'})
                result1.score = 0.5
                result2 = MyDict({'vid': 'vid2', 'bvid': 'bvid2', 'type': 'b'})
                result2.score = 0.6
                return [result1, result2]

            def __enter__(self, *args, **kwargs):
                return self

            def __exit__(self, *args, **kwargs):
                pass

        class FakeIdentifierIndex(object):
            schema = '?'

            def searcher(*args, **kwargs):
                return FakeSearcher()

        search = Search(self.lib)
        search._dataset_index = FakeIdentifierIndex()
        ret = search.search_partitions('about me')
        self.assertTrue(hasattr(ret, 'next'))
        vids = [x for x in ret]
        self.assertIn('vid1', vids)
        self.assertIn('vid2', vids)

    # .identifier_index property tests
    def test_caches_identifier(self):
        search = Search(self.lib)
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(search.get_or_new_index, ['self', 'schema', 'dir'])

        # prepare state
        # return just a string, it is not valid index, but it does not matter here.
        fake_get = fudge.Fake().expects_call().returns('INDEX')

        with fudge.patched_context(search, 'get_or_new_index', fake_get):
            self.assertEquals(search.identifier_index, 'INDEX')

    def test_uses_cached_identifier(self):
        # prepare state
        search = Search(self.lib)

        # Use a string as index, it is not valid index, but it does not matter here.
        search._identifier_index = 'INDEX'

        self.assertEquals(search.identifier_index, 'INDEX')

    # .index_identifiers tests
    def test_add_document_to_writer_for_each_given_identifier(self):
        # prepare state

        # TODO: It is so complicated. Find another way to mock indexer.
        fake_writer = fudge.Fake()\
            .expects('add_document')\
            .expects('commit')

        class FakeSearcher(object):
            pass

        FakeSearcher.documents = fudge.Fake()\
            .expects_call()\
            .returns([])

        search = Search(self.lib)

        search._identifier_index = fudge.Fake('Index')\
            .provides('writer')\
            .returns(fake_writer)\
            .provides('searcher')\
            .returns(FakeSearcher())

        # testing
        identifiers = [
            {'identifier': 'ident1', 'type': 'type1', 'name': 'name1'},
            {'identifier': 'ident2', 'type': 'type2', 'name': 'name2'}]

        search.index_identifiers(identifiers)
        fudge.verify()

    # .expand_place_ids tests
    def test_returns_place_vids(self):
        # first assert signatures of the functions we are going to mock did not change.
        assert_spec(Search.search_identifiers, ['self', 'search_phrase', 'limit'])
        assert_spec(GVid.parse, ['cls', 'gvid'])

        # prepare state
        search = Search(self.lib)
        score = 1
        vid = 'vid-1'
        t = 'type'
        name = 'California1'
        fake_search = fudge.Fake().expects_call().returns([(score, vid, t, name)])
        fake_parse = fudge.Fake().expects_call().returns([])

        # testing
        with fudge.patched_context(Search, 'search_identifiers', fake_search):
            with fudge.patched_context(GVid, 'parse', fake_parse):
                ret = search.expand_place_ids('California')
                self.assertEquals(ret, [vid])

    def test_returns_given_terms_if_place_vids_do_not_exist(self):
        # prepare state
        search = Search(self.lib)

        # testing
        ret = search.expand_place_ids('California')
        self.assertEquals(ret, 'California')

    # .all_identifiers property tests
    def test_returns_dict_with_all_identifiers(self):
        # prepare state
        search = Search(self.lib)
        fake_identifiers = [
            {'name': 'name1', 'identifier': 'identifier1'},
            {'name': 'CA', 'identifier': 'identifier2'},
        ]

        # testing
        with fudge.patched_context(Search, 'identifiers', fake_identifiers):
            ret = search.identifier_map
            self.assertIsInstance(ret, dict)
            self.assertIn('identifier1', ret)
            self.assertIn('identifier2', ret)

    # .identifier_map tests
    def test_returns_dict_with_all_identifiers_except_abbreviations(self):
        # prepare state
        search = Search(self.lib)
        fake_identifiers = [
            {'name': 'name1', 'identifier': 'identifier1'},
            {'name': 'CA', 'identifier': 'identifier2'}]

        # testing
        with fudge.patched_context(Search, 'identifiers', fake_identifiers):
            ret = search.identifier_map
            self.assertIsInstance(ret, dict)
            self.assertIn('identifier1', ret)
            self.assertNotIn('identifier2', ret)

    # .from_to_as_term tests
    def test_returns_years_range(self):

        # prepare state
        search = Search(self.lib)

        # testing
        from_year = 1995
        to_year = 1996
        ret = search.from_to_as_term(from_year, to_year)
        self.assertEquals(ret, '[1995 TO 1996]')

    def test_returns_first_year_if_wrong_second_given(self):

        # prepare state
        search = Search(self.lib)

        # testing
        from_year = 'not-year'
        to_year = 1996
        ret = search.from_to_as_term(from_year, to_year)
        self.assertEquals(ret, '[TO 1996]')

    def test_returns_second_year_if_wrong_first_given(self):

        # prepare state
        search = Search(self.lib)

        # testing
        from_year = '1996'
        to_year = 'not-year'
        ret = search.from_to_as_term(from_year, to_year)
        self.assertEquals(ret, '[1996 TO]')

    def test_returns_None_if_both_are_wrong(self):

        # prepare state
        search = Search(self.lib)

        # testing
        from_year = 'not-year'
        to_year = 'not-year'
        ret = search.from_to_as_term(from_year, to_year)
        self.assertIsNone(ret)


class SearchTermParserTest(unittest.TestCase):

    # .s_quotedterm tests
    def test_strips_and_extends_with_QUOTEDTERM(self):
        ret = SearchTermParser.s_quotedterm('scanner', ' TokEn ')
        self.assertEquals(ret, (SearchTermParser.QUOTEDTERM, 'token'))

    # .s_term tests
    def test_strips_and_extends_with_TERM(self):
        ret = SearchTermParser.s_term('scanner', ' TerM ')
        self.assertEquals(ret, (SearchTermParser.TERM, 'term'))

    # .s_domain tests
    def test_strips_domain_and_extends_with_TERM(self):
        ret = SearchTermParser.s_domainname('scanner', ' Example.Com ')
        self.assertEquals(ret, (SearchTermParser.TERM, 'example.com'))

    # .s_marker tests
    def test_strips_marker_and_extends_with_MARKER(self):
        ret = SearchTermParser.s_markerterm('scanner', ' MarKer ')
        self.assertEquals(ret, (SearchTermParser.MARKER, 'marker'))

    # .s_year tests
    def test_strips_year_and_extends_with_YEAR(self):
        ret = SearchTermParser.s_year('scanner', ' 1996 ')
        self.assertEquals(ret, (SearchTermParser.YEAR, 1996))

    # .parse tests
    def test_parses_to_groups(self):
        parser = SearchTermParser()
        ret = parser.parse('developer in Beslan')
        expected = {'about': 'developer', 'in': 'beslan'}
        self.assertEquals(ret, expected)
