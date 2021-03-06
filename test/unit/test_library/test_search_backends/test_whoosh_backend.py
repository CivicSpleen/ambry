# -*- coding: utf-8 -*-

import os

from six import u

from ambry.library.search_backends.whoosh_backend import WhooshSearchBackend
from ambry.library.search_backends.base import DatasetSearchResult, IdentifierSearchResult,\
    PartitionSearchResult

from test.factories import PartitionFactory, DatasetFactory
from test.proto import TestBase


class WhooshSearchBackendTest(TestBase):
    def setUp(self):
        super(self.__class__, self).setUp()
        self.my_library = self.library(use_proto=False)
        self.backend = WhooshSearchBackend(self.my_library)

    def test_initializes_root_dir(self):
        self.assertEqual(self.backend.root_dir, self.my_library._fs.search() + '/')


class DatasetWhooshIndexTest(TestBase):

    def setUp(self):
        super(self.__class__, self).setUp()
        self.my_library = self.library(use_proto=False)

        # we need clear index for each test
        WhooshSearchBackend(self.my_library).dataset_index.reset()
        WhooshSearchBackend(self.my_library).partition_index.reset()
        self.backend = WhooshSearchBackend(self.my_library)

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
        DatasetFactory._meta.sqlalchemy_session = self.my_library.database.session
        dataset = DatasetFactory()
        self.backend.dataset_index.index_one(dataset)

        # search just added document.
        found = list(self.backend.dataset_index.search(dataset.vid))
        all_vids = [x.vid for x in found]
        self.assertIn(dataset.vid, all_vids)
        self.assertIsInstance(found[0], DatasetSearchResult)

    def test_extends_found_dataset_with_partitions(self):

        # create and index some partitions
        DatasetFactory._meta.sqlalchemy_session = self.my_library.database.session
        PartitionFactory._meta.sqlalchemy_session = self.my_library.database.session

        dataset = DatasetFactory(source='example.com')
        dataset.config.metadata.about.title = 'Test dataset'
        partition1 = PartitionFactory(dataset=dataset, vname=dataset.vname)

        self.backend.dataset_index.index_one(dataset)
        self.my_library.database.session.commit()
        self.backend.partition_index.index_one(partition1)

        # look for just added document.
        found = self.backend.dataset_index.search('example.com')
        found_dataset = found[0]
        assert found_dataset.vid == dataset.vid
        self.assertEqual(len(found_dataset.partitions), 1)
        self.assertIn(partition1.vid, found_dataset.partitions)

    # _index_document tests
    def test_adds_dataset_document_to_the_index(self):
        DatasetFactory._meta.sqlalchemy_session = self.my_library.database.session
        dataset = DatasetFactory()
        self.backend.dataset_index.index_one(dataset)

        # search just added document.
        all_docs = list(self.backend.dataset_index.index.searcher().documents())
        self.assertEqual(all_docs[0]['vid'], dataset.vid)

    def test_replaces_document_in_the_index(self):
        DatasetFactory._meta.sqlalchemy_session = self.my_library.database.session
        dataset = DatasetFactory()
        doc = self.backend.dataset_index._as_document(dataset)
        self.backend.dataset_index._index_document(doc)

        # search just added document.
        all_docs = list(self.backend.dataset_index.index.searcher().documents())
        self.assertEqual(len(all_docs), 1)
        self.assertEqual(all_docs[0]['vid'], dataset.vid)

        # update
        doc['doc'] = u('updated')
        self.backend.dataset_index._index_document(doc, force=True)
        all_docs = list(self.backend.dataset_index.index.searcher().documents(doc='updated'))
        self.assertEqual(len(all_docs), 1)
        self.assertEqual(all_docs[0]['vid'], dataset.vid)

    # _get_generic_schema tests
    def test_returns_whoosh_schema(self):
        schema = self.backend.dataset_index._get_generic_schema()
        self.assertEqual(
            sorted(['vid', 'doc', 'keywords', 'title']),
            sorted(schema.names()))

    # _delete tests
    def test_deletes_dataset_from_index(self):
        DatasetFactory._meta.sqlalchemy_session = self.my_library.database.session
        dataset = DatasetFactory()
        self.backend.dataset_index.index_one(dataset)

        # look for just added document.
        all_docs = list(self.backend.dataset_index.index.searcher().documents())
        self.assertIn(dataset.vid, [x['vid'] for x in all_docs])
        self.backend.dataset_index._delete(vid=dataset.vid)
        all_docs = list(self.backend.dataset_index.index.searcher().documents())
        self.assertNotIn(dataset.vid, [x['vid'] for x in all_docs])

    def test_returns_true_if_dataset_is_already_indexed(self):
        DatasetFactory._meta.sqlalchemy_session = self.my_library.database.session
        dataset = DatasetFactory()
        self.backend.dataset_index.index_one(dataset)

        # search just added document.
        self.assertTrue(self.backend.dataset_index.is_indexed(dataset))

    def test_returns_false_if_dataset_is_not_indexed(self):
        DatasetFactory._meta.sqlalchemy_session = self.my_library.database.session
        dataset = DatasetFactory()

        # search just added document.
        self.assertFalse(self.backend.dataset_index.is_indexed(dataset))


class IdentifierWhooshIndexTest(TestBase):

    def setUp(self):
        super(self.__class__, self).setUp()
        self.my_library = self.library()
        self.backend = WhooshSearchBackend(self.my_library)

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
        self.assertEqual(all_docs[0]['identifier'], 'gvid')
        self.assertEqual(all_docs[0]['name'], 'name1')

    # _get_generic_schema tests
    def test_returns_whoosh_schema(self):
        schema = self.backend.identifier_index._get_generic_schema()
        self.assertEqual(
            sorted(['identifier', 'name', 'type']),
            sorted(schema.names()))

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

    def test_returns_true_if_identifier_is_already_indexed(self):
        identifier = dict(
            identifier='gvid', type='type',
            name='name1')

        self.backend.identifier_index.index_one(identifier)

        # search just added document.
        self.assertTrue(self.backend.identifier_index.is_indexed(identifier))

    def test_returns_false_if_identifier_is_not_indexed(self):
        identifier = dict(
            identifier='gvid', type='type',
            name='name1')
        self.assertFalse(self.backend.identifier_index.is_indexed(identifier))


class PartitionWhooshIndexTest(TestBase):

    def setUp(self):
        super(self.__class__, self).setUp()
        self.my_library = self.library()
        self.backend = WhooshSearchBackend(self.my_library)

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
        PartitionFactory._meta.sqlalchemy_session = self.my_library.database.session
        partition1 = PartitionFactory()
        self.my_library.database.session.commit()
        self.backend.partition_index.index_one(partition1)

        # search just added document.
        found = list(self.backend.partition_index.search(partition1.vid))
        all_vids = [x.vid for x in found]
        self.assertIn(partition1.vid, all_vids)
        self.assertIsInstance(found[0], PartitionSearchResult)

    # _from_to_as_term tests
    def test_converts_years_to_query(self):
        period_term = self.backend.partition_index._from_to_as_term('1978', '1979')
        self.assertEqual(period_term, '[1978 TO 1979]')

    # .index_one tests
    def test_adds_partition_document_to_the_index(self):
        PartitionFactory._meta.sqlalchemy_session = self.my_library.database.session
        partition1 = PartitionFactory()
        self.my_library.database.session.commit()

        self.backend.partition_index.index_one(partition1)

        # search just added document.
        all_docs = list(self.backend.partition_index.index.searcher().documents())
        all_vids = [x['vid'] for x in all_docs]
        self.assertIn(partition1.vid, all_vids)

    def test_replaces_partition_document_in_the_index(self):
        PartitionFactory._meta.sqlalchemy_session = self.my_library.database.session
        partition1 = PartitionFactory()
        self.my_library.database.session.commit()
        doc = self.backend.partition_index._as_document(partition1)
        self.backend.partition_index._index_document(doc)

        # search just added document.
        all_docs = list(self.backend.partition_index.index.searcher().documents())
        all_vids = [x['vid'] for x in all_docs]
        self.assertIn(partition1.vid, all_vids)

        # update
        doc['doc'] = u('updated')
        self.backend.partition_index._index_document(doc, force=True)
        all_docs = list(self.backend.partition_index.index.searcher().documents(doc='updated'))
        self.assertEqual(len(all_docs), 1)
        self.assertEqual(all_docs[0]['vid'], partition1.vid)

    # _get_generic_schema tests
    def test_returns_whoosh_schema(self):
        schema = self.backend.partition_index._get_generic_schema()
        self.assertEqual(
            sorted(['dataset_vid', 'doc', 'keywords', 'title', 'vid']),
            sorted(schema.names()))

    # _make_query_from_terms tests
    def test_creates_doc_query_string_from_about(self):
        query_string = self.backend.partition_index._make_query_from_terms('about help')
        # about searches in the default doc field, so it is not need field prefix.
        self.assertEqual(query_string, 'help')

    def test_creates_doc_query_string_from_with(self):
        query_string = self.backend.partition_index._make_query_from_terms('with help')
        # about searches in the default doc field, so it is not need field prefix.
        self.assertEqual(query_string, 'help')

    def test_creates_keywords_query_string_from_in(self):
        query_string = self.backend.partition_index._make_query_from_terms('in Beslan, Farn')
        # about searches in the default doc field, so it is not need field prefix.
        self.assertEqual(query_string, 'keywords:(beslan , farn)')

    def test_creates_keywords_query_string_from_by(self):
        query_string = self.backend.partition_index._make_query_from_terms('by Beslan')
        # about searches in the default doc field, so it is not need field prefix.
        self.assertEqual(query_string, 'keywords:(beslan)')

    def test_creates_keywords_query_string_from_period(self):
        query_string = self.backend.partition_index._make_query_from_terms('from 2001 to 2010')
        # about searches in the default doc field, so it is not need field prefix.
        self.assertEqual(query_string, 'keywords:([2001 TO 2010])')

    def test_complex_query_string(self):
        raw_query = 'about help in Beslan, Farn from 2001 to 2010 by Beslan'
        query_string = self.backend.partition_index._make_query_from_terms(raw_query)
        self.assertEqual(
            query_string,
            'help AND keywords:(beslan , farn AND beslan AND [2001 TO 2010])')

    # _delete tests
    def test_deletes_partition_from_index(self):
        PartitionFactory._meta.sqlalchemy_session = self.my_library.database.session
        partition1 = PartitionFactory()
        self.my_library.database.session.commit()

        self.backend.partition_index.index_one(partition1)

        # search just added document.
        all_docs = list(self.backend.partition_index.index.searcher().documents())
        self.assertIn(partition1.vid, [x['vid'] for x in all_docs])
        self.backend.partition_index._delete(vid=partition1.vid)
        all_docs = list(self.backend.partition_index.index.searcher().documents())
        self.assertNotIn(partition1.vid, [x['vid'] for x in all_docs])

    # is_indexed test
    def test_returns_true_if_partition_is_already_indexed(self):
        PartitionFactory._meta.sqlalchemy_session = self.my_library.database.session
        partition1 = PartitionFactory()
        self.my_library.database.session.commit()

        self.backend.partition_index.index_one(partition1)

        self.assertTrue(self.backend.partition_index.is_indexed(partition1))

    def test_returns_false_if_identifier_is_not_indexed(self):
        PartitionFactory._meta.sqlalchemy_session = self.my_library.database.session
        partition1 = PartitionFactory()
        self.my_library.database.session.commit()

        self.assertFalse(self.backend.partition_index.is_indexed(partition1))
