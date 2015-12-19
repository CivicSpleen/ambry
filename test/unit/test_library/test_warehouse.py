# -*- coding: utf-8 -*-

from ambry.library import new_library
from ambry.library.warehouse import Warehouse

from test.factories import PartitionFactory
from test.test_base import ConfigDatabaseTestBase

try:
    # py2, mock is external lib.
    from mock import patch
except ImportError:
    # py3, mock is included
    from unittest.mock import patch


class WarehouseTest(ConfigDatabaseTestBase):
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

    # .query() tests
    def test_uses_library_driver_backend(self):
        # FIXME:
        pass

    def test_sends_query_to_database_backend(self):
        w = Warehouse(self.library)
        PartitionFactory._meta.sqlalchemy_session = self.library.database.session
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
        w = Warehouse(self.library)
        PartitionFactory._meta.sqlalchemy_session = self.library.database.session
        partition = PartitionFactory()

        with patch.object(w._backend, 'install') as fake_install:
            w.install(partition.vid)

            # backend.query called once.
            self.assertEqual(len(fake_install.mock_calls), 1)

            # second argument of the is found partition.
            self.assertEqual(fake_install.mock_calls[0][1][1].ref, partition.ref)
