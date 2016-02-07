# -*- coding: utf-8 -*-
from unittest import TestCase

from ambry.library import Library
from ambry.library.warehouse import Warehouse

from ambry.orm import Partition

try:
    # py2, mock is external lib.
    from mock import patch, MagicMock, Mock
except ImportError:
    # py3, mock is included
    from unittest.mock import patch, MagicMock, Mock


class WarehouseTest(TestCase):
    def setUp(self):
        super(self.__class__, self).setUp()
        self._my_library = MagicMock(spec=Library)

    # __init__ tests
    def test_uses_library_db(self):
        self._my_library.config.library.warehouse = None
        warehouse = Warehouse(self._my_library)
        self.assertEqual(self._my_library.database.dsn, warehouse._backend._dsn)

    def test_uses_given_dsn(self):
        warehouse = Warehouse(self._my_library, dsn='sqlite:////tmp/temp1.db')
        self.assertEqual('sqlite:////tmp/temp1.db', warehouse._backend._dsn)

    # .query() tests
    @patch('ambry.bundle.asql_parser.find_indexable_materializable')
    def test_sends_drop_query_to_backend(self, fake_find):

        query = 'CREATE VIEW view1 AS SELECT * FROM p1vid;'
        drop = 'DROP VIEW IF EXISTS view1;'
        tables = []
        install = []
        materialize = []
        indexes = []
        fake_find.return_value = (query, drop, tables, install, materialize, indexes)
        w = Warehouse(self._my_library, dsn='sqlite:////tmp/temp1.db')
        with patch.object(w._backend, 'query') as fake_query:
            w.query(query, logger=Mock())

            # second argument of the first call was drop query.
            self.assertEqual(fake_query.mock_calls[0][1][1], drop)

            # second argument of the third call was create view query
            self.assertEqual(fake_query.mock_calls[2][1][1], query)

    # .install() tests
    def test_finds_partition_by_refs_and_installs_partition_to_backend(self):
        w = Warehouse(self._my_library, dsn='sqlite:////tmp/temp1.db')
        fake_partition = MagicMock(spec=Partition)
        fake_partition.vid = 'p1vid'
        fake_partition.ref = 'p1vid'

        # Make library to return my partition as search result.
        self._my_library.partition.return_value = fake_partition

        with patch.object(w._backend, 'install') as fake_install:
            w.install(fake_partition.vid)

            # backend.query called once.
            self.assertEqual(len(fake_install.mock_calls), 1)

            # second argument of the is found partition.
            self.assertEqual(fake_install.mock_calls[0][1][1].ref, fake_partition.ref)
