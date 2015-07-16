# -*- coding: utf-8 -*-
import unittest

from test.test_base import TestBase

from ambry.library.search_backends.whoosh_backend import WhooshSearchBackend
from ambry.library.search_backends.sqlite_backend import SQLiteSearchBackend
from ambry.library import new_library
from test.test_orm.factories import PartitionFactory


class AmbryReadyMixin(object):
    """ Basic functionality for all search backends. To test new backend add mixin
        and run all tests. If passed, new backend is ready to use as the ambry search backend.
    """

    # helpers
    def _assert_finds_dataset(self, dataset, search_phrase):
        found = self.backend.dataset_index.search(search_phrase)
        all_vids = [x.vid for x in found]
        self.assertIn(dataset.vid, all_vids)

    def _assert_finds_partition(self, partition, search_phrase):
        found = self.backend.partition_index.search(search_phrase)
        all_vids = [x.vid for x in found]
        self.assertIn(partition.vid, all_vids)

    # tests
    def test_add_dataset_to_the_index(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        self.backend.dataset_index.index_one(dataset)

        datasets = self.backend.dataset_index.all()
        all_vids = [x.vid for x in datasets]
        self.assertIn(dataset.vid, all_vids)

    def test_does_not_add_dataset_twice(self):
        # FIXME:
        pass

    def test_find_dataset_by_vid(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        self.backend.dataset_index.index_one(dataset)

        found = self.backend.dataset_index.search(dataset.vid)
        all_vids = [x.vid for x in found]
        self.assertIn(dataset.vid, all_vids)

    def test_find_dataset_by_title(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        dataset.config.metadata.about.title = 'The-title'
        self.backend.dataset_index.index_one(dataset)
        self._assert_finds_dataset(dataset, 'The-title')

    def test_find_dataset_by_summary(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        dataset.config.metadata.about.summary = 'Some summary of the dataset'
        self.backend.dataset_index.index_one(dataset)
        self._assert_finds_dataset(dataset, 'summary of the')

    def test_find_dataset_by_id(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        self.backend.dataset_index.index_one(dataset)
        self._assert_finds_dataset(dataset, str(dataset.identity.id_))

    def test_find_dataset_by_source(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        assert dataset.identity.source
        self.backend.dataset_index.index_one(dataset)
        self._assert_finds_dataset(dataset, dataset.identity.source)

    def _test_find_dataset_by_name(self):
        # FIXME:
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        assert str(dataset.identity.name)
        self.backend.dataset_index.index_one(dataset)
        self._assert_finds_dataset(dataset, str(dataset.identity.name))

    def _test_find_dataset_by_vname(self):
        # FIXME:
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        assert str(dataset.identity.vname)
        self.backend.dataset_index.index_one(dataset)
        self._assert_finds_dataset(dataset, str(dataset.identity.vname))

    def _test_find_dataset_by_column(self):
        # FIXME:
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        column_description = 'Some usefull column.'
        # FIXME: create column and bind it to the dataset.
        self.backend.dataset_index.index_one(dataset)
        self._assert_finds_dataset(dataset, 'usefull column')

    # FIXME: test dataset extended with partitions.

    #
    # Partition index tests.
    #


@unittest.skip('Not ready')
class WhooshBackendTest(TestBase, AmbryReadyMixin):

    def setUp(self):
        super(self.__class__, self).setUp()
        rc = self.get_rc()
        self.library = new_library(rc)
        self.backend = WhooshSearchBackend(self.library)

    # partition add
    # FIXME: move all tests to the base method.
    def test_add_partition_to_the_index(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        PartitionFactory._meta.sqlalchemy_session = db.session
        partition = PartitionFactory(dataset=dataset)
        self.backend.partition_index.index_one(partition)
        partitions = self.backend.partition_index.all()
        all_vids = [x.vid for x in partitions]
        self.assertIn(partition.vid, all_vids)

    # partition search
    def test_find_partition_by_vid(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        PartitionFactory._meta.sqlalchemy_session = db.session
        partition = PartitionFactory(dataset=dataset)
        self.backend.partition_index.index_one(partition)
        self._assert_finds_partition(partition, partition.vid)

    def test_find_partition_by_id(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        PartitionFactory._meta.sqlalchemy_session = db.session
        partition = PartitionFactory(dataset=dataset)
        self.backend.partition_index.index_one(partition)
        self._assert_finds_partition(partition, partition.identity.id_)

    def _test_find_partition_by_name(self):
        # FIXME:
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        PartitionFactory._meta.sqlalchemy_session = db.session
        partition = PartitionFactory(dataset=dataset)
        self.backend.partition_index.index_one(partition)
        self._assert_finds_partition(partition, str(partition.identity.name))

    def _test_find_partition_by_vname(self):
        # FIXME:
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        PartitionFactory._meta.sqlalchemy_session = db.session
        partition = PartitionFactory(dataset=dataset)
        self.backend.partition_index.index_one(partition)
        self._assert_finds_partition(partition, str(partition.identity.vname))

    # FIXME: Add tests for some complex queries (by, with, from, to, etc...)


@unittest.skip('Not ready')
class SQLiteBackendTest(TestBase, AmbryReadyMixin):

    def setUp(self):
        super(self.__class__, self).setUp()
        rc = self.get_rc()
        self.library = new_library(rc)
        self.backend = SQLiteSearchBackend(self.library)
