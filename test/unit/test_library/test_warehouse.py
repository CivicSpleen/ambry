# -*- coding: utf-8 -*-

from ambry.library import new_library
from ambry.library.warehouse import Warehouse

from test.factories import PartitionFactory
from test.test_base import TestBase

try:
    # py2, mock is external lib.
    from mock import patch, MagicMock
except ImportError:
    # py3, mock is included
    from unittest.mock import patch, MagicMock


class WarehouseTest(TestBase):
    def setUp(self):
        super(self.__class__, self).setUp()
        rc = self.get_rc()
        self.library = new_library(rc)
        self.warehouse = Warehouse(self.library)

    # __init__ tests
    def _test_uses_library_db(self):
        # FIXME:
        pass

    def _test_uses_db_from_config(self):
        # FIXME:
        pass

    # query tests
    def test_uses_library_driver_backend(self):
        # FIXME:
        pass

    @patch('ambry.library.warehouse.Warehouse._install')
    @patch('ambry.library.warehouse.Warehouse._get_table_name')
    def test_installs_each_ref_found_in_the_query(self, fake_get, fake_install):
        w = Warehouse(self.library)
        PartitionFactory._meta.sqlalchemy_session = self.library.database.session
        partition = PartitionFactory()
        fake_get.return_value = partition.vid
        w.query('SELECT * FROM {};'.format(partition.vid))
        fake_install.assert_called_once_with(partition.vid)

    # _get_table_name tests
    # FIXME:

    # _install tests
    @patch('ambry.library.warehouse.add_partition')
    @patch('ambry.library.Library.partition')
    def test_creates_virtual_table_for_given_partition(self, fake_partition, fake_add):
        w = Warehouse(self.library)
        PartitionFactory._meta.sqlalchemy_session = self.library.database.session
        partition = PartitionFactory()
        fake_partition.return_value = partition  # FIXME: This should be bundle partition.
        w._install(partition.vid)
        self.assertEqual(len(fake_add.mock_calls), 1)
