# -*- coding: utf-8 -*-

from ambry.orm import Partition
from ambry.orm.exc import NotFoundError

from test.factories import PartitionFactory
from test.proto import TestBase


class LibraryTest(TestBase):
    def setUp(self):
        super(self.__class__, self).setUp()
        self.my_library = self.library(use_proto=False)

    def _assert_finds_by_ref(self, library, partition, ref):
        """ Asserts that partition can be found in the library by given vid.
        Args:
            partition (orm.Partition):
            ref (str): id, vid, name or vname of the partition
        """
        # validate given state.
        fields = 'id: {}, vid: {}, name: {}, vname: {}'\
            .format(partition.id, partition.vid, partition.name, partition.vname)

        self.assertEqual(
            len(set([partition.id, partition.vid, partition.name, partition.vname])),
            4,
            'Make partition fields unique. Otherwise search may be invalid. Fields: {}'.format(fields))

        # search and test.
        ret = library.partition(ref)
        self.assertIsInstance(ret, Partition)
        self.assertEqual(ret.vid, partition.vid)

    # .partition tests
    def test_finds_partition_by_id(self):
        PartitionFactory._meta.sqlalchemy_session = self.my_library.database.session
        partition = PartitionFactory()
        self._assert_finds_by_ref(self.my_library, partition, partition.id)

    def test_finds_partition_by_vid(self):
        PartitionFactory._meta.sqlalchemy_session = self.my_library.database.session
        partition = PartitionFactory()
        self._assert_finds_by_ref(self.my_library, partition, partition.vid)

    def test_finds_partition_by_name(self):
        PartitionFactory._meta.sqlalchemy_session = self.my_library.database.session
        partition = PartitionFactory()
        self._assert_finds_by_ref(self.my_library, partition, partition.name)

    def test_finds_partition_by_versioned_name(self):
        PartitionFactory._meta.sqlalchemy_session = self.my_library.database.session
        partition = PartitionFactory()
        self._assert_finds_by_ref(self.my_library, partition, partition.vname)

    def test_raises_NotFoundError_if_partition_not_found(self):
        with self.assertRaises(NotFoundError):
            self.my_library.partition('no-such-partition')
