# -*- coding: utf-8 -*-
import os
import unittest

from test.test_base import TestBase

from ambry.library.search_backends.sqlite_backend import SQLiteSearchBackend
from ambry.library import new_library
from ambry.library.search_backends.base import DatasetSearchResult, IdentifierSearchResult,\
    PartitionSearchResult
from test.test_orm.factories import PartitionFactory


@unittest.skip('Not ready')
class DatasetSQLiteIndexTest(TestBase):

    def setUp(self):
        super(self.__class__, self).setUp()
        rc = self.get_rc()
        library = new_library(rc)
        self.backend = SQLiteSearchBackend(library)

    def test_initialises_index(self):

        # dataset_index table created
        query = """
            SELECT name FROM sqlite_master WHERE type='table' AND name='dataset_index';
        """
        result = self.backend.library.database.connection.execute(query).fetchall()
        self.assertEquals(result, [('dataset_index',)])

    # reset tests
    def test_drops_dataset_index(self):
        query = """
            SELECT name FROM sqlite_master WHERE type='table' AND name='dataset_index';
        """
        # Table exists before drop.
        result = self.backend.library.database.connection.execute(query).fetchall()
        self.assertEquals(result, [('dataset_index',)])

        # drop
        self.backend.dataset_index.reset()

        # table does not exist after drop
        result = self.backend.library.database.connection.execute(query).fetchall()
        self.assertEquals(result, [])

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

    def _test_extends_found_dataset_with_partitions(self):
        # FIXME:

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
        query = """
            SELECT vid from dataset_index;
        """
        result = self.backend.library.database.connection.execute(query).fetchall()
        self.assertEquals(result[0][0], dataset.vid)

    # _delete tests
    def test_deletes_dataset_from_index(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        self.backend.dataset_index.index_one(dataset)

        query = """
            SELECT vid from dataset_index
            WHERE vid = :vid;
        """

        # assert document added.
        result = self.backend.library.database.connection.execute(query, vid=dataset.vid).fetchall()
        self.assertEquals(dataset.vid, result[0][0])

        self.backend.dataset_index._delete(vid=dataset.vid)

        # assert document is deleted.
        result = self.backend.library.database.connection.execute(query, vid=dataset.vid).fetchall()
        self.assertEquals(result, [])
