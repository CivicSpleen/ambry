# -*- coding: utf-8 -*-

from ambry.library.warehouse import Warehouse

from test.factories import PartitionFactory
from test.test_base import TestBase

try:
    # py2, mock is external lib.
    from mock import patch
except ImportError:
    # py3, mock is included
    from unittest.mock import patch


class WarehouseTest(TestBase):
    def setUp(self):
        super(self.__class__, self).setUp()
        self.my_library = self.library()

    # __init__ tests
    def test_uses_library_db(self):
        config_warehouse = self.my_library.config.library.warehouse
        try:
            self.my_library.config.library.warehouse = None
            self.warehouse = Warehouse(self.my_library)
            self.assertEqual(self.my_library.database.dsn, self.warehouse._backend._dsn)
        finally:
            # restore value from config
            self.my_library.config.library.warehouse = config_warehouse

    def test_uses_database_from_config(self):
        config_warehouse = self.my_library.config.library.warehouse
        try:
            self.my_library.config.library.warehouse = 'sqlite:////tmp/temp1.db'
            self.warehouse = Warehouse(self.my_library)
            self.assertEqual('sqlite:////tmp/temp1.db', self.warehouse._backend._dsn)
        finally:
            # restore value from config
            self.my_library.config.library.warehouse = config_warehouse

    # .query() tests
    def test_sends_query_to_database_backend(self):
        w = Warehouse(self.my_library)
        PartitionFactory._meta.sqlalchemy_session = self.my_library.database.session
        partition = PartitionFactory()
        with patch.object(w._backend, 'query') as fake_query:
            query = 'SELECT * FROM {};'.format(partition.vid)
            w.query(query)

            # backend.query called once.
            self.assertEqual(len(fake_query.mock_calls), 1)

            # second argument of the call was the same query.
            self.assertEqual(fake_query.mock_calls[0][1][1], query)

    # .install() tests
    def test_finds_partition_by_refs_and_installs_partition_to_backend(self):
        w = Warehouse(self.my_library)
        PartitionFactory._meta.sqlalchemy_session = self.my_library.database.session
        partition = PartitionFactory()

        with patch.object(w._backend, 'install') as fake_install:
            w.install(partition.vid)

            # backend.query called once.
            self.assertEqual(len(fake_install.mock_calls), 1)

            # second argument of the is found partition.
            self.assertEqual(fake_install.mock_calls[0][1][1].ref, partition.ref)
