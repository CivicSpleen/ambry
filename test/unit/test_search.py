# -*- coding: utf-8 -*-
import fudge
from fudge.inspector import arg

from test.test_base import TestBase

from ambry.library.search import Search
from ambry.library.search_backends.whoosh_backend import DatasetWhooshIndex, PartitionWhooshIndex
from ambry.library.search_backends.whoosh_backend import WhooshSearchBackend
from ambry.library.search_backends.sqlite_backend import SQLiteSearchBackend
from ambry.library import new_library

from test.unit.orm_factories import PartitionFactory, DatasetFactory


class SearchTest(TestBase):
    def setUp(self):
        super(self.__class__, self).setUp()
        rc = self.get_rc()
        self.library = new_library(rc)
        self.backend = WhooshSearchBackend(self.library)

    def tearDown(self):
        super(self.__class__, self).tearDown()
        if hasattr(self, 'backend'):
            self.backend.reset()

    def test_uses_library_driver_backend(self):
        assert self.library.database.driver == 'sqlite'
        self.library.config.services.search = None
        search = Search(self.library)
        self.assertIsInstance(search.backend, SQLiteSearchBackend)

    def test_uses_backend_from_config(self):
        self.library.config.services.search = 'whoosh'
        search = Search(self.library)
        self.assertIsInstance(search.backend, WhooshSearchBackend)

    def test_raises_missing_backend_exception_if_config_contains_invalid_backend(self):
        # services.search
        self.library.config.services.search = 'foo'
        try:
            Search(self.library)
        except Exception as exc:
            self.assertIn('Missing backend', str(exc))

    def test_uses_default_backend_if_library_database_search_is_not_implemented(self):
        with fudge.patched_context(self.library.database, 'driver', 'mysql'):
            search = Search(self.library)
            self.assertIsInstance(search.backend, WhooshSearchBackend)

    # index_library_datasets tests
    def test_indexes_library_datasets(self):
        DatasetFactory._meta.sqlalchemy_session = self.library.database.session
        ds1 = DatasetFactory()
        ds2 = DatasetFactory()
        ds3 = DatasetFactory()
        self.library.database.session.commit()
        self.assertEqual(len(self.library.datasets), 3)

        fake_index_one = fudge.Fake().is_callable()\
            .expects_call().with_args(arg.passes_test(lambda x: x.vid == ds1.vid)).returns(True)\
            .next_call().with_args(arg.passes_test(lambda x: x.vid == ds2.vid)).returns(True)\
            .next_call().with_args(arg.passes_test(lambda x: x.vid == ds3.vid)).returns(True)

        with fudge.patched_context(DatasetWhooshIndex, 'index_one', fake_index_one):
            search = Search(self.library)
            search.index_library_datasets()

    def test_indexes_library_datasets_partitions(self):
        DatasetFactory._meta.sqlalchemy_session = self.library.database.session
        PartitionFactory._meta.sqlalchemy_session = self.library.database.session
        ds1 = DatasetFactory()
        self.assertEqual(len(self.library.datasets), 1)

        partition1 = PartitionFactory(dataset=ds1)
        self.library.database.session.commit()

        fake_index_one = fudge.Fake().is_callable()\
            .expects_call().with_args(arg.passes_test(lambda x: x.vid == partition1.vid)).returns(True)

        with fudge.patched_context(PartitionWhooshIndex, 'index_one', fake_index_one):
            search = Search(self.library)
            search.index_library_datasets()

    def test_feeds_tick_function_with_indexed(self):
        ds1 = self.new_db_dataset(self.library.database, n=1)
        self.assertEqual(len(self.library.datasets), 1)
        PartitionFactory._meta.sqlalchemy_session = self.library.database.session

        PartitionFactory(dataset=ds1)
        self.library.database.session.commit()

        fake_index_one = fudge.Fake().is_callable().returns(True)
        tick_f = fudge.Fake()\
            .expects_call().with_args('datasets: 1 partitions: 0')\
            .next_call().with_args('datasets: 1 partitions: 1')

        with fudge.patched_context(PartitionWhooshIndex, 'index_one', fake_index_one):
            with fudge.patched_context(DatasetWhooshIndex, 'index_one', fake_index_one):
                search = Search(self.library)
                search.index_library_datasets(tick_f=tick_f)
