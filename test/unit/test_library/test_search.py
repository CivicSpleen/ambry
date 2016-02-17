# -*- coding: utf-8 -*-
from unittest import TestCase

try:
    # py2, mock is external lib.
    from mock import MagicMock, Mock, patch
except ImportError:
    # py3, mock is included
    from unittest.mock import MagicMock, Mock, patch

from ambry.library import Library
from ambry.library.search import Search
from ambry.library.search_backends import WhooshSearchBackend, SQLiteSearchBackend,\
    PostgreSQLSearchBackend
from ambry.orm import Dataset, Partition


class SearchTest(TestCase):
    def setUp(self):
        self._my_library = MagicMock(spec=Library)

    def test_uses_library_driver_backend(self):
        self._my_library.config.services.search = None

        # switch to sqlite.
        self._my_library.database.driver = 'sqlite'
        search = Search(self._my_library)
        self.assertIsInstance(search.backend, SQLiteSearchBackend)

        # switch to postgres.
        self._my_library.database.driver = 'postgres'
        search = Search(self._my_library)
        self.assertIsInstance(search.backend, PostgreSQLSearchBackend)

    @patch('ambry.library.search_backends.whoosh_backend.WhooshSearchBackend.__init__')
    def test_uses_backend_from_config(self, fake_init):
        # Disable backend initialization to reduce amount of mocks.
        fake_init.return_value = None

        self._my_library.config.services.search = 'whoosh'
        search = Search(self._my_library)
        self.assertIsInstance(search.backend, WhooshSearchBackend)

    def test_raises_missing_backend_exception_if_config_contains_invalid_backend(self):
        # services.search
        try:
            Search(self._my_library)
        except Exception as exc:
            self.assertIn('Missing backend', str(exc))

    @patch('ambry.library.search_backends.whoosh_backend.WhooshSearchBackend.__init__')
    def test_uses_default_backend_if_library_database_search_is_not_implemented(self, fake_init):
        # Disable backend initialization to reduce amount of mocks.
        fake_init.return_value = None
        self._my_library.config.services.search = None
        with patch.object(self._my_library.database, 'driver', 'mysql'):
            search = Search(self._my_library)
            self.assertIsInstance(search.backend, WhooshSearchBackend)

    # index_library_datasets tests
    def test_indexes_library_datasets(self):
        ds1 = MagicMock(spec=Dataset)
        ds2 = MagicMock(spec=Dataset)
        ds3 = MagicMock(spec=Dataset)
        self._my_library.datasets = [ds1, ds2, ds3]

        fake_backend = MagicMock(spec=SQLiteSearchBackend)
        fake_backend.dataset_index = Mock()
        fake_backend.partition_index = Mock()
        fake_backend.identifier_index = Mock()
        search = Search(self._my_library, backend=fake_backend)
        search.index_library_datasets()
        self.assertEqual(len(fake_backend.dataset_index.index_one.mock_calls), 3)

    def test_indexes_library_datasets_partitions(self):

        ds1 = MagicMock(spec=Dataset)
        ds2 = MagicMock(spec=Dataset)

        ds1.partitions = [MagicMock(spec=Partition), MagicMock(spec=Partition)]
        ds2.partitions = [MagicMock(spec=Partition)]

        self._my_library.datasets = [ds1, ds2]

        fake_backend = MagicMock(spec=SQLiteSearchBackend)
        fake_backend.dataset_index = Mock()
        fake_backend.partition_index = Mock()
        fake_backend.identifier_index = Mock()

        search = Search(self._my_library, backend=fake_backend)
        search.index_library_datasets()
        self.assertEqual(len(fake_backend.partition_index.index_one.mock_calls), 3)

    def test_feeds_tick_function_with_indexed_dataset(self):
        # prepare mocks
        fake_backend = MagicMock(spec=SQLiteSearchBackend)
        fake_backend.dataset_index = Mock()
        fake_backend.partition_index = Mock()
        fake_backend.identifier_index = Mock()

        tick_f = Mock()

        fake_library = MagicMock(spec=Library)
        fake_dataset = MagicMock(spec=Dataset)
        fake_library.datasets = [fake_dataset]

        # run
        search = Search(fake_library, backend=fake_backend)
        search.index_library_datasets(tick_f=tick_f)

        # test
        tick_f.assert_called_once_with('datasets: 1 partitions: 0')

class SearchTermParserTest(TestCase):


    def test_basic(self):
        from library.search_backends.base import SearchTermParser

        print SearchTermParser().parse('table2 from 1978 to 1979 in california')

        print SearchTermParser().parse('about food from 2012 to 2015')

