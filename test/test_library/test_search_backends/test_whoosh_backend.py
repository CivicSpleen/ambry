# -*- coding: utf-8 -*-
import os
import unittest

from test.test_base import TestBase

from ambry.library.search_backends.whoosh_backend import WhooshSearchBackend
from ambry.library import new_library
from ambry.library.search_backends.base import DatasetSearchResult, IdentifierSearchResult,\
    PartitionSearchResult
from test.test_orm.factories import PartitionFactory


@unittest.skip('Not ready')
class WhooshSearchBackendTest(TestBase):
    def setUp(self):
        super(self.__class__, self).setUp()
        rc = self.get_rc()
        self.library = new_library(rc)
        self.backend = WhooshSearchBackend(self.library)

    def test_initializes_root_dir(self):
        self.assertEquals(self.backend.root_dir, self.library._fs.search() + '/')


@unittest.skip('Not ready')
class DatasetWhooshIndexTest(TestBase):

    def setUp(self):
        super(self.__class__, self).setUp()
        rc = self.get_rc()
        library = new_library(rc)
        self.backend = WhooshSearchBackend(library)

    def tearDown(self):
        super(self.__class__, self).tearDown()
        if hasattr(self, 'backend'):
            self.backend.dataset_index.reset()

    def test_intializes_index(self):
        self.assertIsNotNone(self.backend.dataset_index.index)
        self.assertTrue(os.path.exists(self.backend.dataset_index.index_dir))

    # reset tests
    def test_removes_index_directory(self):
        self.backend.reset()
        self.assertFalse(os.path.exists(self.backend.dataset_index.index_dir))

    # search tests
    def test_returns_found_dataset(self):

        # add dataset to backend.
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        self.backend.dataset_index.index_one(dataset)

        # search just added document.
        found = list(self.backend.dataset_index.search(dataset.vid))
        all_vids = [x.vid for x in found]
        self.assertIn(dataset.vid, all_vids)
        self.assertIsInstance(found[0], DatasetSearchResult)

    def test_extends_found_dataset_with_partitions(self):

        # add dataset to backend.
        db = self.new_database()

        # create and index some partitions
        PartitionFactory._meta.sqlalchemy_session = db.session

        dataset = self.new_db_dataset(db, n=0)
        dataset.config.metadata.about.title = 'Test dataset'
        self.backend.dataset_index.index_one(dataset)
        partition1 = PartitionFactory(dataset=dataset, vname=dataset.vname)
        db.session.commit()
        self.backend.partition_index.index_one(partition1)

        # search just added document.
        found = self.backend.dataset_index.search('Test dataset')
        found_dataset = found[0]
        assert found_dataset.vid == dataset.vid
        self.assertEquals(len(found_dataset.partitions), 1)
        self.assertIn(partition1.vid, found_dataset.partitions)

    # _index_document tests
    def test_adds_dataset_document_to_the_index(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        self.backend.dataset_index.index_one(dataset)

        # search just added document.
        all_docs = list(self.backend.dataset_index.index.searcher().documents())
        self.assertEquals(all_docs[0]['type'], 'dataset')
        self.assertEquals(all_docs[0]['vid'], dataset.vid)

    # _get_generic_schema tests
    def test_returns_whoosh_schema(self):
        schema = self.backend.dataset_index._get_generic_schema()
        self.assertItemsEqual(
            ['bvid', 'doc', 'keywords', 'title', 'type', 'vid'],
            schema.names())

    # _delete tests
    def test_deletes_dataset_from_index(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        self.backend.dataset_index.index_one(dataset)

        # search just added document.
        all_docs = list(self.backend.dataset_index.index.searcher().documents())
        self.assertIn(dataset.vid, [x['vid'] for x in all_docs])
        self.backend.dataset_index._delete(vid=dataset.vid)
        all_docs = list(self.backend.dataset_index.index.searcher().documents())
        self.assertNotIn(dataset.vid, [x['vid'] for x in all_docs])


@unittest.skip('Not ready')
class IdentifierWhooshIndexTest(TestBase):

    def setUp(self):
        super(self.__class__, self).setUp()
        rc = self.get_rc()
        library = new_library(rc)
        self.backend = WhooshSearchBackend(library)

    def tearDown(self):
        super(self.__class__, self).tearDown()
        if hasattr(self, 'backend'):
            self.backend.identifier_index.reset()

    def test_intializes_index(self):
        self.assertIsNotNone(self.backend.identifier_index.index)
        self.assertTrue(os.path.exists(self.backend.identifier_index.index_dir))

    # reset tests
    def test_removes_index_directory(self):
        self.backend.reset()
        self.assertFalse(os.path.exists(self.backend.identifier_index.index_dir))

    # search tests
    def test_returns_found_identifier(self):
        # add dataset to backend.
        identifier = dict(
            identifier='gvid', type='type',
            name='name1')
        self.backend.identifier_index.index_one(identifier)

        # search just added document.
        found = list(self.backend.identifier_index.search('name1'))
        self.assertIsInstance(found, list)
        names = [x.name for x in found]
        self.assertIn('name1', names)
        self.assertIsInstance(found[0], IdentifierSearchResult)

    # _index_document tests
    def test_adds_identifier_document_to_the_index(self):
        identifier = dict(
            identifier='gvid', type='type',
            name='name1')
        self.backend.identifier_index.index_one(identifier)

        # search just added document.
        all_docs = list(self.backend.identifier_index.index.searcher().documents())
        self.assertEquals(all_docs[0]['identifier'], 'gvid')
        self.assertEquals(all_docs[0]['name'], 'name1')

    # _get_generic_schema tests
    def test_returns_whoosh_schema(self):
        schema = self.backend.identifier_index._get_generic_schema()
        self.assertItemsEqual(
            ['identifier', 'name', 'type'],
            schema.names())

    # _delete tests
    def test_deletes_identifier_from_index(self):
        identifier = dict(
            identifier='gvid', type='type',
            name='name1')
        self.backend.identifier_index.index_one(identifier)

        # find just added document.
        all_docs = list(self.backend.identifier_index.index.searcher().documents())
        self.assertIn('gvid', [x['identifier'] for x in all_docs])
        self.backend.identifier_index._delete(identifier='gvid')
        all_docs = list(self.backend.identifier_index.index.searcher().documents())
        self.assertNotIn('gvid', [x['identifier'] for x in all_docs])


@unittest.skip('Not ready')
class PartitionWhooshIndexTest(TestBase):

    def setUp(self):
        super(self.__class__, self).setUp()
        rc = self.get_rc()
        library = new_library(rc)
        self.backend = WhooshSearchBackend(library)

    def tearDown(self):
        super(self.__class__, self).tearDown()
        if hasattr(self, 'backend'):
            self.backend.partition_index.reset()

    def test_intializes_index(self):
        self.assertIsNotNone(self.backend.partition_index.index)
        self.assertTrue(os.path.exists(self.backend.partition_index.index_dir))

    # reset tests
    def test_removes_index_directory(self):
        self.backend.reset()
        self.assertFalse(os.path.exists(self.backend.partition_index.index_dir))

    # search tests
    def test_returns_found_partition(self):
        # add dataset to backend.
        db = self.new_database()
        PartitionFactory._meta.sqlalchemy_session = db.session
        partition1 = PartitionFactory()
        db.session.commit()
        self.backend.partition_index.index_one(partition1)

        # search just added document.
        found = list(self.backend.partition_index.search(partition1.vid))
        all_vids = [x.vid for x in found]
        self.assertIn(partition1.vid, all_vids)
        self.assertIsInstance(found[0], PartitionSearchResult)

    # _from_to_as_term tests
    def test_converts_years_to_query(self):
        period_term = self.backend.partition_index._from_to_as_term('1978', '1979')
        self.assertEquals(period_term, '[1978 TO 1979]')

    # _index_document tests
    def test_adds_partition_document_to_the_index(self):
        db = self.new_database()
        PartitionFactory._meta.sqlalchemy_session = db.session
        partition1 = PartitionFactory()
        db.session.commit()

        self.backend.partition_index.index_one(partition1)

        # search just added document.
        all_docs = list(self.backend.partition_index.index.searcher().documents())
        all_vids = [x['vid'] for x in all_docs]
        self.assertIn(partition1.vid, all_vids)

    # _get_generic_schema tests
    def test_returns_whoosh_schema(self):
        schema = self.backend.partition_index._get_generic_schema()
        self.assertItemsEqual(
            ['bvid', 'doc', 'keywords', 'title', 'vid'],
            schema.names())

    # _make_query_from_terms tests
    def test_creates_doc_query_string_from_about(self):
        query_string = self.backend.partition_index._make_query_from_terms('about help')
        # about searches in the default doc field, so it is not need field prefix.
        self.assertEquals(query_string, 'help')

    def test_creates_doc_query_string_from_with(self):
        query_string = self.backend.partition_index._make_query_from_terms('with help')
        # about searches in the default doc field, so it is not need field prefix.
        self.assertEquals(query_string, 'help')

    def test_creates_keywords_query_string_from_in(self):
        query_string = self.backend.partition_index._make_query_from_terms('in Beslan, Farn')
        # about searches in the default doc field, so it is not need field prefix.
        self.assertEquals(query_string, 'keywords:(beslan , farn)')

    def test_creates_keywords_query_string_from_by(self):
        query_string = self.backend.partition_index._make_query_from_terms('by Beslan')
        # about searches in the default doc field, so it is not need field prefix.
        self.assertEquals(query_string, 'keywords:(beslan)')

    def test_creates_keywords_query_string_from_period(self):
        query_string = self.backend.partition_index._make_query_from_terms('from 2001 to 2010')
        # about searches in the default doc field, so it is not need field prefix.
        self.assertEquals(query_string, 'keywords:([2001 TO 2010])')

    def test_complex_query_string(self):
        raw_query = 'about help in Beslan, Farn from 2001 to 2010 by Beslan'
        query_string = self.backend.partition_index._make_query_from_terms(raw_query)
        self.assertEquals(
            query_string,
            'help AND keywords:(beslan , farn AND beslan AND [2001 TO 2010])')

    # _delete tests
    def test_deletes_partition_from_index(self):
        db = self.new_database()
        PartitionFactory._meta.sqlalchemy_session = db.session
        partition1 = PartitionFactory()
        db.session.commit()

        self.backend.partition_index.index_one(partition1)

        # search just added document.
        all_docs = list(self.backend.partition_index.index.searcher().documents())
        self.assertIn(partition1.vid, [x['vid'] for x in all_docs])
        self.backend.partition_index._delete(vid=partition1.vid)
        all_docs = list(self.backend.partition_index.index.searcher().documents())
        self.assertNotIn(partition1.vid, [x['vid'] for x in all_docs])
