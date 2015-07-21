# -*- coding: utf-8 -*-
import unittest

from test.test_base import TestBase

from ambry.library.search_backends.whoosh_backend import WhooshSearchBackend
from ambry.library.search_backends.sqlite_backend import SQLiteSearchBackend
from ambry.library import new_library

from test.test_orm.factories import PartitionFactory


# To debug set SKIP_ALL to True and comment @skip decorator on test you want to run.
SKIP_ALL = False


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
    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
    def test_add_dataset_to_the_index(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        self.backend.dataset_index.index_one(dataset)

        datasets = self.backend.dataset_index.all()
        all_vids = [x.vid for x in datasets]
        self.assertIn(dataset.vid, all_vids)

    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
    def test_search_dataset_by_vid(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        self.backend.dataset_index.index_one(dataset)

        found = self.backend.dataset_index.search(dataset.vid)
        all_vids = [x.vid for x in found]
        self.assertIn(dataset.vid, all_vids)

    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
    def test_search_dataset_by_title(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        dataset.config.metadata.about.title = 'title'
        self.backend.dataset_index.index_one(dataset)
        self._assert_finds_dataset(dataset, 'title')

    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
    def test_search_dataset_by_summary(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        dataset.config.metadata.about.summary = 'Some summary of the dataset'
        self.backend.dataset_index.index_one(dataset)
        self._assert_finds_dataset(dataset, 'summary of the')

    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
    def test_search_dataset_by_id(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        self.backend.dataset_index.index_one(dataset)
        self._assert_finds_dataset(dataset, str(dataset.identity.id_))

    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
    def test_search_dataset_by_source(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0, source='example.com')
        assert dataset.identity.source
        self.backend.dataset_index.index_one(dataset)
        self._assert_finds_dataset(dataset, 'source example.com from 1978 to 1975')

    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
    def test_search_dataset_by_name(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0, source='example.com')
        assert str(dataset.identity.name)
        self.backend.dataset_index.index_one(dataset)
        self._assert_finds_dataset(dataset, str(dataset.identity.name))

    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
    def test_search_dataset_by_vname(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0, source='example.com')
        assert str(dataset.identity.vname)
        self.backend.dataset_index.index_one(dataset)
        self._assert_finds_dataset(dataset, str(dataset.identity.vname))

    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
    def test_does_not_add_dataset_twice(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        self.backend.dataset_index.index_one(dataset)

        datasets = self.backend.dataset_index.all()
        self.assertEquals(len(datasets), 1)

        self.backend.dataset_index.index_one(dataset)
        datasets = self.backend.dataset_index.all()
        self.assertEquals(len(datasets), 1)

    # partition add
    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
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
    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
    def test_search_partition_by_vid(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        PartitionFactory._meta.sqlalchemy_session = db.session
        partition = PartitionFactory(dataset=dataset)
        self.backend.partition_index.index_one(partition)
        self._assert_finds_partition(partition, partition.vid)

    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
    def test_search_partition_by_id(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        PartitionFactory._meta.sqlalchemy_session = db.session
        partition = PartitionFactory(dataset=dataset)
        self.backend.partition_index.index_one(partition)
        self._assert_finds_partition(partition, partition.identity.id_)

    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
    def test_search_partition_by_name(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0, source='example.com')
        table = dataset.new_table('table2', description='table2')
        partition = dataset.new_partition(table, time=1, name='Partition1')
        db.commit()
        self.backend.partition_index.index_one(partition)
        self._assert_finds_partition(partition, unicode(partition.identity.name))

    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
    def test_search_partition_by_vname(self):
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0, source='example.com')
        PartitionFactory._meta.sqlalchemy_session = db.session
        partition = PartitionFactory(dataset=dataset)
        self.backend.partition_index.index_one(partition)
        self._assert_finds_partition(partition, str(partition.identity.vname))

    # search tests
    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
    def test_search_years_range(self):
        """ search by `* from 1978 to 1979` """
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        PartitionFactory._meta.sqlalchemy_session = db.session
        partition = PartitionFactory(dataset=dataset, time_coverage=['1978', '1979'])
        db.session.commit()
        self.backend.partition_index.index_one(partition)
        self.backend.dataset_index.index_one(dataset)

        # find partition in the partition index.
        self._assert_finds_partition(partition, '* from 1978 to 1979')

        # finds dataset extended with partition
        found = self.backend.dataset_index.search('* from 1978 to 1979')
        self.assertEquals(len(found), 1)
        self.assertEquals(len(found[0].partitions), 1)
        self.assertIn(partition.vid, found[0].partitions)

    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
    def test_search_about(self):
        """ search by `* about cucumber` """
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        table = dataset.new_table('table2', description='table2')
        partition = dataset.new_partition(table, time=1)
        db.commit()
        partition.table.add_column('column1', description='cucumber')
        db.commit()
        self.backend.dataset_index.index_one(dataset)
        self.backend.partition_index.index_one(partition)

        # find partition in the partition index.
        self._assert_finds_partition(partition, 'about cucumber')

        # finds dataset extended with partition
        found = self.backend.dataset_index.search('about cucumber')
        self.assertEquals(len(found), 1)
        self.assertEquals(len(found[0].partitions), 1)
        self.assertIn(partition.vid, found[0].partitions)

    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
    def test_search_with(self):
        """ search by `* with cucumber` """
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        table = dataset.new_table('table2', description='table2')
        partition = dataset.new_partition(table, time=1)
        db.commit()
        partition.table.add_column('column1', description='cucumber')
        db.commit()
        self.backend.dataset_index.index_one(dataset)
        self.backend.partition_index.index_one(partition)

        # find partition in the partition index.
        self._assert_finds_partition(partition, 'dataset with cucumber')

        # finds dataset extended with partition
        found = self.backend.dataset_index.search('dataset with cucumber')
        self.assertEquals(len(found), 1)
        self.assertEquals(len(found[0].partitions), 1)
        self.assertIn(partition.vid, found[0].partitions)

    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
    def test_search_in(self):
        """ search by `* in California` """
        # FIXME: Not sure about proper field. Using space_coverage. Ask Eric for proper field.
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        table = dataset.new_table('table2', description='table2')
        partition = dataset.new_partition(table, time=1, space_coverage=['california'])
        db.commit()
        self.backend.dataset_index.index_one(dataset)
        self.backend.partition_index.index_one(partition)

        # find partition in the partition index.
        self._assert_finds_partition(partition, 'in California')

        # finds dataset extended with partition
        found = self.backend.dataset_index.search('dataset in California')
        self.assertEquals(len(found), 1)
        self.assertEquals(len(found[0].partitions), 1)
        self.assertIn(partition.vid, found[0].partitions)

    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
    def test_search_by(self):
        """ search by `* by California` """
        # FIXME: Ask Eric for real-life example and field which stores target for `by`.
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        table = dataset.new_table('table2', description='table2')
        partition = dataset.new_partition(table, time=1, grain_coverage=['california'])
        db.commit()
        self.backend.dataset_index.index_one(dataset)
        self.backend.partition_index.index_one(partition)

        # find partition in the partition index.
        self._assert_finds_partition(partition, 'by California')

        # finds dataset extended with partition
        found = self.backend.dataset_index.search('dataset by California')
        self.assertEquals(len(found), 1)
        self.assertEquals(len(found[0].partitions), 1)
        self.assertIn(partition.vid, found[0].partitions)

    # test some complex examples.
    @unittest.skipIf(SKIP_ALL, 'Debug skip.')
    def test_range_and_in(self):
        """ search by `table2 from 1978 to 1979 in california` """
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        db.session.commit()
        table = dataset.new_table('table2', description='table2')
        partition = dataset.new_partition(
            table, time=1, space_coverage=['california'],
            time_coverage=['1978', '1979'])
        db.commit()
        self.backend.dataset_index.index_one(dataset)
        self.backend.partition_index.index_one(partition)

        # finds dataset extended with partition
        found = self.backend.dataset_index.search('table2 from 1978 to 1979 in california')
        self.assertEquals(len(found), 1)
        self.assertEquals(len(found[0].partitions), 1)
        self.assertIn(partition.vid, found[0].partitions)


class WhooshBackendTest(TestBase, AmbryReadyMixin):

    def setUp(self):
        super(self.__class__, self).setUp()
        rc = self.get_rc()
        self.library = new_library(rc)

        # we need clean backend for test
        WhooshSearchBackend(self.library).reset()
        self.backend = WhooshSearchBackend(self.library)


class SQLiteBackendTest(TestBase, AmbryReadyMixin):

    def setUp(self):
        super(self.__class__, self).setUp()
        rc = self.get_rc()
        self.library = new_library(rc)
        self.backend = SQLiteSearchBackend(self.library)

    @unittest.skip('Not ready.')
    def test_search_years_range(self):
        """ search by `* from 1978 to 1979` """
        # FIXME:
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        PartitionFactory._meta.sqlalchemy_session = db.session
        partition = PartitionFactory(dataset=dataset, time_coverage=['1978', '1979'])
        db.session.commit()
        self.backend.partition_index.index_one(partition)
        self.backend.dataset_index.index_one(dataset)

        # find partition in the partition index.
        self._assert_finds_partition(partition, '* from 1978 to 1979')

        # finds dataset extended with partition
        found = self.backend.dataset_index.search('* from 1978 to 1979')
        self.assertEquals(len(found), 1)
        self.assertEquals(len(found[0].partitions), 1)
        self.assertIn(partition.vid, found[0].partitions)

    @unittest.skip('Not ready.')
    def test_search_in(self):
        """ search by `* in California` """
        # FIXME: Not sure about proper field. Using space_coverage. Ask Eric for proper field.
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        table = dataset.new_table('table2', description='table2')
        partition = dataset.new_partition(table, time=1, space_coverage=['california'])
        db.commit()
        self.backend.dataset_index.index_one(dataset)
        self.backend.partition_index.index_one(partition)

        # find partition in the partition index.
        self._assert_finds_partition(partition, 'in California')

        # finds dataset extended with partition
        found = self.backend.dataset_index.search('dataset in California')
        self.assertEquals(len(found), 1)
        self.assertEquals(len(found[0].partitions), 1)
        self.assertIn(partition.vid, found[0].partitions)

    @unittest.skip('Not ready.')
    def test_search_by(self):
        """ search by `* by California` """
        # FIXME: Ask Eric for real-life example and field which stores target for `by`.
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        table = dataset.new_table('table2', description='table2')
        partition = dataset.new_partition(table, time=1, grain_coverage=['california'])
        db.commit()
        self.backend.dataset_index.index_one(dataset)
        self.backend.partition_index.index_one(partition)

        # find partition in the partition index.
        self._assert_finds_partition(partition, 'by California')

        # finds dataset extended with partition
        found = self.backend.dataset_index.search('* by California')
        self.assertEquals(len(found), 1)
        self.assertEquals(len(found[0].partitions), 1)
        self.assertIn(partition.vid, found[0].partitions)

    # test some complex examples.
    @unittest.skip('Not ready.')
    def test_range_and_in(self):
        """ search by `table2 from 1978 to 1979 in california` """
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        db.session.commit()
        table = dataset.new_table('table2', description='table2')
        partition = dataset.new_partition(
            table, time=1, space_coverage=['california'],
            time_coverage=['1978', '1979'])
        db.commit()
        self.backend.dataset_index.index_one(dataset)
        self.backend.partition_index.index_one(partition)

        # finds dataset extended with partition
        found = self.backend.dataset_index.search('table2 from 1978 to 1979 in california')
        self.assertEquals(len(found), 1)
        self.assertEquals(len(found[0].partitions), 1)
        self.assertIn(partition.vid, found[0].partitions)
