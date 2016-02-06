# -*- coding: utf-8 -*-
import os
import unittest

from test.test_base import TestBase, PostgreSQLTestBase

from ambry.library.search_backends.whoosh_backend import WhooshSearchBackend
from ambry.library.search_backends.sqlite_backend import SQLiteSearchBackend
from ambry.library.search_backends.postgres_backend import PostgreSQLSearchBackend

from test.factories import PartitionFactory, DatasetFactory, TableFactory

# Description of the search system:
# https://docs.google.com/document/d/1jLGRsYt4G6Tfo6m_Dtry6ZFRnDWpW6gXUkNPVaGxoO4/edit#

# FIXME: Add identifier index tests.


class AmbryReadyMixin(object):
    """ Basic functionality for all search backends. Requires self._my_library attribute.

        To test new backend add mixin and run all tests. If passed, new backend
        is ready to use as the ambry search backend.
    """

    # helpers
    def _assert_finds_dataset(self, dataset, search_phrase):
        found = self._my_library.search.search(search_phrase)
        all_vids = [x.vid for x in found]
        self.assertIn(dataset.vid, all_vids)

    def _assert_finds_partition(self, partition, search_phrase):
        found = self._my_library.search.backend.partition_index.search(search_phrase)
        all_vids = [x.vid for x in found]
        self.assertIn(partition.vid, all_vids)

    # tests
    def test_add_dataset_to_the_index(self):
        DatasetFactory._meta.sqlalchemy_session = self._my_library.database.session
        dataset = DatasetFactory()
        self._my_library.database.session.commit()
        self._my_library.search.index_dataset(dataset)

        datasets = self._my_library.search.backend.dataset_index.all()
        all_vids = [x.vid for x in datasets]
        self.assertIn(dataset.vid, all_vids)

    def test_search_dataset_by_vid(self):
        dataset = self.new_db_dataset(self._my_library.database, n=0)
        self._my_library.search.index_dataset(dataset)

        found = self._my_library.search.search(dataset.vid)
        all_vids = [x.vid for x in found]
        self.assertIn(dataset.vid, all_vids)

    def test_search_dataset_by_title(self):
        dataset = self.new_db_dataset(self._my_library.database, n=0)
        dataset.config.metadata.about.title = 'title'
        self._my_library.search.index_dataset(dataset)
        self._assert_finds_dataset(dataset, 'title')

    def test_search_dataset_by_summary(self):
        dataset = self.new_db_dataset(self._my_library.database, n=0)
        dataset.config.metadata.about.summary = 'Some summary of the dataset'
        self._my_library.search.index_dataset(dataset)
        self._assert_finds_dataset(dataset, 'summary of the')

    def test_search_dataset_by_id(self):
        dataset = self.new_db_dataset(self._my_library.database, n=0)
        self._my_library.search.index_dataset(dataset)
        self._assert_finds_dataset(dataset, str(dataset.identity.id_))

    def test_search_dataset_by_source(self):
        dataset = self.new_db_dataset(self._my_library.database, n=0, source='example.com')
        assert dataset.identity.source
        self._my_library.search.index_dataset(dataset)
        self._assert_finds_dataset(dataset, 'source example.com')

    def test_search_dataset_by_name(self):
        dataset = self.new_db_dataset(self._my_library.database, n=0, source='example.com')
        assert str(dataset.identity.name)
        self._my_library.search.index_dataset(dataset)
        self._assert_finds_dataset(dataset, str(dataset.identity.name))

    def test_search_dataset_by_vname(self):
        dataset = self.new_db_dataset(self._my_library.database, n=0, source='example.com')
        assert str(dataset.identity.vname)
        self._my_library.search.index_dataset(dataset)
        self._assert_finds_dataset(dataset, str(dataset.identity.vname))

    def test_does_not_add_dataset_twice(self):
        dataset = self.new_db_dataset(self._my_library.database, n=0)
        self._my_library.search.index_dataset(dataset)

        datasets = self._my_library.search.backend.dataset_index.all()
        self.assertEqual(len(datasets), 1)

        self._my_library.search.index_dataset(dataset)
        datasets = self._my_library.search.backend.dataset_index.all()
        self.assertEqual(len(datasets), 1)

    # partition add
    def test_add_partition_to_the_index(self):
        # dataset = self.new_db_dataset(self._my_library.database, n=0)
        DatasetFactory._meta.sqlalchemy_session = self._my_library.database.session
        PartitionFactory._meta.sqlalchemy_session = self._my_library.database.session
        dataset = DatasetFactory()
        partition = PartitionFactory(dataset=dataset)
        self._my_library.database.session.commit()
        self._my_library.search.index_partition(partition)
        partitions = self._my_library.search.backend.partition_index.all()
        all_vids = [x.vid for x in partitions]
        self.assertIn(partition.vid, all_vids)

    # partition search
    def test_search_partition_by_vid(self):
        DatasetFactory._meta.sqlalchemy_session = self._my_library.database.session
        PartitionFactory._meta.sqlalchemy_session = self._my_library.database.session
        dataset = DatasetFactory()
        partition = PartitionFactory(dataset=dataset)
        self._my_library.database.session.commit()
        self._my_library.search.index_partition(partition)
        self._assert_finds_partition(partition, partition.vid)

    def test_search_partition_by_id(self):
        DatasetFactory._meta.sqlalchemy_session = self._my_library.database.session
        PartitionFactory._meta.sqlalchemy_session = self._my_library.database.session
        dataset = DatasetFactory()
        partition = PartitionFactory(dataset=dataset)
        self._my_library.database.session.commit()
        self._my_library.search.index_partition(partition)
        self._assert_finds_partition(partition, partition.identity.id_)

    def test_search_partition_by_name(self):
        dataset = self.new_db_dataset(self._my_library.database, n=0, source='example.com')
        table = dataset.new_table('table2', description='table2')
        partition = dataset.new_partition(table, time=1, name='Partition1')
        self._my_library.database.commit()
        self._my_library.search.index_partition(partition)
        self._assert_finds_partition(partition, str(partition.identity.name))

    def test_search_partition_by_vname(self):
        # dataset = self.new_db_dataset(self._my_library.database, n=0, source='example.com')
        DatasetFactory._meta.sqlalchemy_session = self._my_library.database.session
        PartitionFactory._meta.sqlalchemy_session = self._my_library.database.session
        dataset = DatasetFactory()
        partition = PartitionFactory(dataset=dataset)
        self._my_library.database.session.commit()
        self._my_library.search.index_partition(partition)
        self._assert_finds_partition(partition, str(partition.identity.vname))

    # search tests
    def test_search_years_range(self):
        """ search by `source example.com from 1978 to 1979` (temporal bounds) """
        dataset = self.new_db_dataset(self._my_library.database, n=0, source='example.com')
        table = dataset.new_table('table2', description='table2')
        partition = dataset.new_partition(
            table, time=1,
            time_coverage=['1978', '1979'])
        self._my_library.database.commit()
        self._my_library.search.index_partition(partition)
        self._my_library.search.index_dataset(dataset)

        # find partition in the partition index.
        self._assert_finds_partition(partition, 'from 1978 to 1979')

        # find dataset extended with partition
        found = list(self._my_library.search.search('source example.com from 1978 to 1979'))
        self.assertEqual(len(found), 1)
        self.assertEqual(len(found[0].partitions), 1)
        self.assertIn(partition.vid, found[0].partitions)

    def test_search_about(self):
        """ search by `* about cucumber` """
        dataset = self.new_db_dataset(self._my_library.database, n=0)
        table = dataset.new_table('table2', description='table2')
        partition = dataset.new_partition(table, time=1)
        self._my_library.database.commit()
        partition.table.add_column('column1', description='cucumber')
        self._my_library.database.commit()
        self._my_library.search.index_partition(partition)
        self._my_library.search.index_dataset(dataset)

        # find partition in the partition index.
        self._assert_finds_partition(partition, 'about cucumber')

        # finds dataset extended with partition
        found = list(self._my_library.search.search('about cucumber'))
        self.assertEqual(len(found), 1)
        self.assertEqual(len(found[0].partitions), 1)
        self.assertIn(partition.vid, found[0].partitions)

    def test_search_with(self):
        """ search by `* with cucumber` """
        dataset = self.new_db_dataset(self._my_library.database, n=0)
        table = dataset.new_table('table2', description='table2')
        partition = dataset.new_partition(table, time=1)
        self._my_library.database.commit()
        partition.table.add_column('column1', description='cucumber')
        self._my_library.database.commit()
        self._my_library.search.index_dataset(dataset)
        self._my_library.search.index_partition(partition)

        # find partition in the partition index.
        self._assert_finds_partition(partition, 'dataset with cucumber')

        # finds dataset extended with partition
        found = list(self._my_library.search.search('dataset with cucumber'))
        self.assertEqual(len(found), 1)
        self.assertEqual(len(found[0].partitions), 1)
        self.assertIn(partition.vid, found[0].partitions)

    def test_search_in(self):
        """ search by `source example.com in California` (geographic bounds) """
        dataset = self.new_db_dataset(self._my_library.database, n=0, source='example.com')
        table = dataset.new_table('table2', description='table2')
        partition = dataset.new_partition(table, time=1, space_coverage=['california'])
        self._my_library.database.commit()
        self._my_library.search.index_dataset(dataset)
        self._my_library.search.index_partition(partition)

        # find partition in the partition index.
        self._assert_finds_partition(partition, 'in California')

        # finds dataset extended with partition
        found = list(self._my_library.search.search('source example.com in California'))
        self.assertEqual(len(found), 1)
        self.assertEqual(len(found[0].partitions), 1)
        self.assertIn(partition.vid, found[0].partitions)

    def test_search_by(self):
        """ search by `source example.com by county` (granularity search) """

        DatasetFactory._meta.sqlalchemy_session = self._my_library.database.session
        TableFactory._meta.sqlalchemy_session = self._my_library.database.session
        PartitionFactory._meta.sqlalchemy_session = self._my_library.database.session

        dataset = DatasetFactory(source='example.com')
        table = TableFactory(dataset=dataset, description='table2', name='table2')
        partition = PartitionFactory(dataset=dataset, table=table, grain_coverage=['county'])

        self._my_library.database.commit()
        self._my_library.search.index_dataset(dataset)
        self._my_library.search.index_partition(partition)

        # find partition in the partition index.
        self._assert_finds_partition(partition, 'by county')

        # finds dataset extended with partition
        found = list(self._my_library.search.search('source example.com by county'))
        self.assertEqual(len(found), 1)
        self.assertEqual(len(found[0].partitions), 1)
        self.assertIn(partition.vid, found[0].partitions)

    # test some complex examples.
    def test_range_and_in(self):
        """ search by `table2 from 1978 to 1979 in california` (geographic bounds and temporal bounds) """
        dataset = self.new_db_dataset(self._my_library.database, n=0)
        self._my_library.database.session.commit()
        table = dataset.new_table('table2', description='table2')
        partition = dataset.new_partition(
            table, time=1, space_coverage=['california'],
            time_coverage=['1978', '1979'])
        self._my_library.database.commit()
        self._my_library.search.index_dataset(dataset)
        self._my_library.search.index_partition(partition)

        # finds dataset extended with partition
        found = list(self._my_library.search.search('table2 from 1978 to 1979 in california'))
        self.assertEqual(len(found), 1)
        self.assertEqual(len(found[0].partitions), 1)
        self.assertIn(partition.vid, found[0].partitions)

    def test_search_identifier_by_part_of_the_name(self):
        ident1 = {
            'identifier': 'id1',
            'type': 'dataset',
            'name': 'name1'}

        ident2 = {
            'identifier': 'id2',
            'type': 'dataset',
            'name': 'name2'
        }

        self._my_library.search.index_identifiers([ident1, ident2])

        # testing.
        ret = list(self._my_library.search.search_identifiers('name'))
        self.assertEqual(len(ret), 2)
        self.assertListEqual(['id1', 'id2'], [x.vid for x in ret])


class WhooshTest(TestBase, AmbryReadyMixin):
    """ Library database is sqlite, search backend is whoosh. """

    def setUp(self):
        super(self.__class__, self).setUp()

        self._my_library = self.library()
        self._my_library.config.services.search = 'whoosh'

        # we need clean backend for test
        WhooshSearchBackend(self._my_library).reset()
        assert isinstance(self._my_library.search.backend, WhooshSearchBackend)


class InMemorySQLiteTest(TestBase, AmbryReadyMixin):
    """ Library database is in-memory sqlite, search backend is in-memory sqlite. """

    @classmethod
    def setUpClass(cls):
        super(InMemorySQLiteTest, cls).setUpClass()
        if not cls._is_sqlite:
            raise unittest.SkipTest('SQLite tests are disabled.')

    def setUp(self):
        super(self.__class__, self).setUp()

        self._my_library = self.library()
        self._DATABASE_DSN = self._my_library.database.dsn
        if self._my_library.database.dsn != 'sqlite://':
            self._my_library.drop()
            self._my_library.database.dsn = 'sqlite://'
            self._my_library.create()

        # make library to use library database for search.
        self._my_library.config.services.search = None
        assert isinstance(self._my_library.search.backend, SQLiteSearchBackend)
        self.assertEqual(self._my_library.database.dsn, 'sqlite://')

    def tearDown(self):
        super(self.__class__, self).tearDown()
        if self._my_library.database.dsn != self._DATABASE_DSN:
            self._my_library.database.dsn = self._DATABASE_DSN


class FileSQLiteTest(TestBase, AmbryReadyMixin):
    """ Library database is file-based sqlite, search backend is file-based sqlite. """

    @classmethod
    def setUpClass(cls):
        super(FileSQLiteTest, cls).setUpClass()
        if not cls._is_sqlite:
            raise unittest.SkipTest('SQLite tests are disabled.')

    def setUp(self):
        super(self.__class__, self).setUp()
        self.rc = self.get_rc()

        # force to use library database for search.
        self.rc.services.search = None

        self._my_library = self.library()
        assert isinstance(self._my_library.search.backend, SQLiteSearchBackend)
        self.assertIn('.db', self._my_library.database.dsn)


class PostgreSQLTest(PostgreSQLTestBase, AmbryReadyMixin):
    """ Library database is postgres, search backend is postgres. """

    def setUp(self):
        super(PostgreSQLTest, self).setUp()

        # force to use library database for search.
        rc = self.get_rc()
        rc.services.search = None
        self._my_library = self.library(rc)
        assert isinstance(self._my_library.search.backend, PostgreSQLSearchBackend)
