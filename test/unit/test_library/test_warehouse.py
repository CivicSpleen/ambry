# -*- coding: utf-8 -*-
from unittest import TestCase

from ambry.bundle.asql_parser import FIMRecord
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
        fake_find.return_value = FIMRecord(statement=query, drop=drop)
        w = Warehouse(self._my_library, dsn='sqlite:////tmp/temp1.db')
        with patch.object(w._backend, 'query') as fake_query:
            w.query(query, logger=Mock())
            # second argument of the first call was drop query.
            self.assertEqual(fake_query.mock_calls[0][1][1], drop)

    @patch('ambry.bundle.asql_parser.find_indexable_materializable')
    def test_installs_vids_recognized_as_installable(self, fake_find):

        query = 'INSTALL p1vid;'
        install = ['p1vid']
        fake_find.return_value = FIMRecord(statement=query, install=install)
        w = Warehouse(self._my_library, dsn='sqlite:////tmp/temp1.db')
        with patch.object(w, 'install') as fake_install:
            w.query(query, logger=Mock())
            fake_install.assert_called_once_with('p1vid')

    @patch('ambry.bundle.asql_parser.find_indexable_materializable')
    def test_materializes_vids_recognized_as_materializable(self, fake_find):

        query = 'MATERIALIZE p1vid;'
        materialize = ['p1vid']
        fake_find.return_value = FIMRecord(statement=query, materialize=materialize)
        w = Warehouse(self._my_library, dsn='sqlite:////tmp/temp1.db')
        with patch.object(w, 'materialize') as fake_materialize:
            w.query(query, logger=Mock())
            fake_materialize.assert_called_once_with('p1vid')

    @patch('ambry.bundle.asql_parser.find_indexable_materializable')
    def test_indexes_vids_recognized_as_indexable(self, fake_find):
        query = 'INDEX p1vid (col1, col2);'
        indexes = [('p1vid', ('col1', 'col2'))]
        fake_find.return_value = FIMRecord(statement=query, indexes=indexes)
        w = Warehouse(self._my_library, dsn='sqlite:////tmp/temp1.db')
        with patch.object(w, 'index') as fake_index:
            w.query(query, logger=Mock())
            fake_index.assert_called_once_with('p1vid', ('col1', 'col2'))

    @patch('ambry.bundle.asql_parser.find_indexable_materializable')
    def test_sends_create_query_to_backend(self, fake_find):

        query = 'CREATE VIEW view1 AS SELECT * FROM p1vid;'
        fake_find.return_value = FIMRecord(statement=query)
        w = Warehouse(self._my_library, dsn='sqlite:////tmp/temp1.db')
        with patch.object(w._backend, 'query') as fake_query:
            w.query(query, logger=Mock())

            # second argument of the first call was create view query
            self.assertEqual(fake_query.mock_calls[0][1][1], query)

    @patch('ambry.bundle.asql_parser.find_indexable_materializable')
    def test_sends_select_query_to_backend(self, fake_find):

        query = 'SELECT * FROM p1vid;'
        fake_find.return_value = FIMRecord(statement=query)
        w = Warehouse(self._my_library, dsn='sqlite:////tmp/temp1.db')
        with patch.object(w._backend, 'query') as fake_query:
            w.query(query, logger=Mock())

            # second argument of the first call was create view query
            self.assertEqual(fake_query.mock_calls[0][1][1], query)

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
