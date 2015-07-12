# -*- coding: utf-8 -*-
import os
import unittest

from test.test_base import TestBase

from ambry.library.search_backends.whoosh_backend import WhooshSearchBackend,\
    IdentifierWhooshIndex, DatasetWhooshIndex
from ambry.library import new_library
from ambry.library.search import SearchResult


@unittest.skip('Not ready')
class WhooshSearchBackendTest(TestBase):

    def test_initializes_root_dir(self):
        rc = self.get_rc()
        library = new_library(rc)
        backend = WhooshSearchBackend(library)
        self.assertEquals(backend.root_dir, library._fs.search() + '/')

    def test_initializes_dataset_index(self):
        rc = self.get_rc()
        library = new_library(rc)
        backend = WhooshSearchBackend(library)
        self.assertIsInstance(backend.dataset_index, DatasetWhooshIndex)

    def _test_initializes_identifier_index(self):
        # FIXME:
        rc = self.get_rc()
        library = new_library(rc)
        backend = WhooshSearchBackend(library)
        self.assertIsInstance(backend.identifier_index, IdentifierWhooshIndex)

    def _test_initializes_partition_index(self):
        # FIXME:
        rc = self.get_rc()
        library = new_library(rc)
        backend = WhooshSearchBackend(library)
        self.assertIsInstance(backend.partition_index, IdentifierWhooshIndex)

    # _make_query_from_terms tests
    def test_converts_query(self):
        rc = self.get_rc()
        library = new_library(rc)
        backend = WhooshSearchBackend(library)
        cterms = backend._make_query_from_terms('California')
        self.assertEquals(
            cterms,
            '( type:dataset AND doc:(california) ) OR ( type:partition AND doc:(california) )')

    # _expand_place_ids tests
    def _test_expands_given_place_with_id(self):
        # FIXME:
        rc = self.get_rc()
        library = new_library(rc)
        backend = WhooshSearchBackend(library)
        temp = backend._expand_place_ids('California')

    # _from_to_as_term tests
    def test_converts_years_to_query(self):
        rc = self.get_rc()
        library = new_library(rc)
        backend = WhooshSearchBackend(library)
        period_term = backend._from_to_as_term('1978', '1979')
        self.assertEquals(period_term, '[1978 TO 1979]')


@unittest.skip('Not ready')
class DatasetWhooshIndexTest(TestBase):
    def test_intializes_index(self):
            rc = self.get_rc()
            library = new_library(rc)
            backend = WhooshSearchBackend(library)
            self.assertIsNotNone(backend.dataset_index.index)
            self.assertTrue(os.path.exists(backend.dataset_index.index_dir))

    # reset tests
    # FIXME: Implement.

    # search tests
    def test_returns_found_dataset(self):
        rc = self.get_rc()
        library = new_library(rc)
        backend = WhooshSearchBackend(library)

        # add dataset to backend.
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        backend.dataset_index.index_one(dataset)

        # search for just added document.
        found = backend.dataset_index.search(dataset.vid)
        self.assertIsInstance(found, dict)
        # FIXME: delete index after each tests.
        self.assertIn(dataset.vid, found)
        self.assertIsInstance(found[0], SearchResult)

    # _index_document tests
    def test_adds_dataset_document_to_the_index(self):
        rc = self.get_rc()
        library = new_library(rc)
        backend = WhooshSearchBackend(library)
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        backend.dataset_index.index_one(dataset)

        # search for just added document.
        all_docs = list(backend.dataset_index.index.searcher().documents())
        # FIXME: it breaks some times because index directory does not empty after tests
        # self.assertEquals(len(all_docs), 1)
        self.assertEquals(all_docs[0]['type'], 'dataset')
        self.assertEquals(all_docs[0]['vid'], dataset.vid)

    # _get_generic_schema tests
    def test_returns_whoosh_schema(self):
        rc = self.get_rc()
        library = new_library(rc)
        backend = WhooshSearchBackend(library)
        schema = backend.dataset_index._get_generic_schema()
        self.assertItemsEqual(
            ['bvid', 'doc', 'keywords', 'title', 'type', 'vid'],
            schema.names())

    # _delete tests
    def _test_deletes_dataset_from_index(self):
        # FIXME: is broken.
        rc = self.get_rc()
        library = new_library(rc)
        backend = WhooshSearchBackend(library)
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        backend.dataset_index.index_one(dataset)

        # search for just added document.
        all_docs = list(backend.dataset_index.index.searcher().documents())
        self.assertIn(dataset.vid, [x['vid'] for x in all_docs])
        backend.dataset_index._delete(dataset.vid)
        all_docs = list(backend.dataset_index.index.searcher().documents())
        self.assertNotIn(dataset.vid, [x['vid'] for x in all_docs])
