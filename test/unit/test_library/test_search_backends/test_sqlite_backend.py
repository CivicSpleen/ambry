# -*- coding: utf-8 -*-

import unittest

from sqlalchemy.sql.expression import text

from ambry.library.search_backends.sqlite_backend import SQLiteSearchBackend
from ambry.library.search_backends.base import DatasetSearchResult, IdentifierSearchResult, \
    PartitionSearchResult

from test.factories import PartitionFactory, DatasetFactory
from test.proto import TestBase


class SQLiteSearchTestBase(TestBase):

    @classmethod
    def setUpClass(cls):
        super(SQLiteSearchTestBase, cls).setUpClass()
        if cls._db_type != 'sqlite':
            raise unittest.SkipTest('SQLite tests are disabled.')

    def setUp(self):
        super(SQLiteSearchTestBase, self).setUp()
        self.my_library = self.library(use_proto=False)
        self.backend = SQLiteSearchBackend(self.my_library)


class SQLiteSearchBackendTest(SQLiteSearchTestBase):

    # _and_join tests
    def test_joins_terms(self):
        self.assertEqual(
            self.backend._and_join(['term1', 'term2']),
            'term1 term2')

    def test_joins_one_term(self):
        self.assertEqual(
            self.backend._and_join(['term1']),
            'term1')


class DatasetSQLiteIndexTest(SQLiteSearchTestBase):

    def test_initializes_index(self):
        _assert_table_exists(self.backend, 'dataset_index')

    # reset tests
    def test_drops_dataset_index(self):
        _assert_resets_index(self.backend, 'dataset_index')

    # search tests
    def test_returns_found_dataset(self):

        # add dataset to backend.
        DatasetFactory._meta.sqlalchemy_session = self.my_library.database.session
        dataset = DatasetFactory()
        self.my_library.database.session.commit()
        self.backend.dataset_index.index_one(dataset)

        # search just added document.
        found = list(self.backend.dataset_index.search(dataset.vid))
        all_vids = [x.vid for x in found]
        self.assertIn(dataset.vid, all_vids)
        self.assertIsInstance(found[0], DatasetSearchResult)

    # _index_document tests
    def test_adds_dataset_document_to_the_index(self):
        DatasetFactory._meta.sqlalchemy_session = self.my_library.database.session
        dataset = DatasetFactory()
        self.backend.dataset_index.index_one(dataset)

        # search just added document.
        query = """
            SELECT vid
            FROM dataset_index;
        """
        result = self.backend.library.database.connection.execute(query).fetchall()
        self.assertEqual(result[0][0], dataset.vid)

    # _delete tests
    def test_deletes_dataset_from_index(self):
        DatasetFactory._meta.sqlalchemy_session = self.my_library.database.session
        dataset = DatasetFactory()
        self.backend.dataset_index.index_one(dataset)

        query = """
            SELECT vid
            FROM dataset_index
            WHERE vid = :vid;
        """

        # assert document added.
        result = self.backend.library.database.connection.execute(query, vid=dataset.vid).fetchall()
        self.assertEqual(dataset.vid, result[0][0])

        self.backend.dataset_index._delete(vid=dataset.vid)

        # assert document is deleted.
        result = self.backend.library.database.connection.execute(query, vid=dataset.vid).fetchall()
        self.assertEqual(result, [])


class IdentifierSQLiteIndexTest(SQLiteSearchTestBase):

    def test_initializes_index(self):
        _assert_table_exists(self.backend, 'identifier_index')

    # reset tests
    def test_resets_identifier_index(self):
        _assert_resets_index(self.backend, 'identifier_index')

    # search tests
    def test_returns_found_identifier(self):
        # add identifier to the index.
        identifier = dict(
            identifier='gvid', type='type',
            name='name1')
        self.backend.identifier_index.index_one(identifier)

        # assert it is added.
        query = text("""
            SELECT identifier
            FROM identifier_index
            WHERE identifier = :identifier;
        """)

        result = self.backend.library.database.connection.execute(query, identifier='gvid').fetchall()
        self.assertEqual(result, [('gvid',)])

        # search and found result.
        found = list(self.backend.identifier_index.search('name'))
        self.assertIsInstance(found[0], IdentifierSearchResult)
        names = [x.name for x in found]
        self.assertIn('name1', names)

    # _index_document tests
    def test_adds_identifier_document_to_the_index(self):
        identifier = dict(
            identifier='gvid', type='type',
            name='name1')
        self.backend.identifier_index.index_one(identifier)

        # assert it is added.
        query = text("""
            SELECT identifier
            FROM identifier_index
            WHERE identifier = :identifier;
        """)

        result = self.backend.library.database.connection.execute(query, identifier='gvid').fetchall()
        self.assertEqual(result, [('gvid',)])

    # _delete tests
    def test_deletes_identifier_from_index(self):
        identifier = dict(
            identifier='gvid', type='type',
            name='name1')
        self.backend.identifier_index.index_one(identifier)

        # assert document exists.
        query = text("""
            SELECT identifier
            FROM identifier_index
            WHERE identifier = :identifier;
        """)

        result = self.backend.library.database.connection.execute(query, identifier='gvid').fetchall()
        self.assertEqual(result, [('gvid',)])

        # deleting
        self.backend.identifier_index._delete(identifier='gvid')

        # assert document is deleted.
        result = self.backend.library.database.connection.execute(query, identifier='gvid').fetchall()
        self.assertEqual(result, [])


class PartitionSQLiteIndexTest(SQLiteSearchTestBase):

    def test_initializes_index(self):
        _assert_table_exists(self.backend, 'partition_index')

    # reset tests
    def test_drops_dataset_index(self):
        _assert_resets_index(self.backend, 'partition_index')

    # search tests
    def test_returns_found_partition(self):
        # create partition and add it to the index to backend.
        PartitionFactory._meta.sqlalchemy_session = self.my_library.database.session
        partition1 = PartitionFactory()
        self.my_library.database.session.commit()

        self.backend.partition_index.index_one(partition1)

        # assert partition is added to index.
        query = """
            SELECT vid
            FROM partition_index;
        """
        result = self.backend.library.database.connection.execute(query).fetchall()
        self.assertEqual(result[0][0], partition1.vid)

        # search and check found result.
        found = list(self.backend.partition_index.search(partition1.vid))
        self.assertIsInstance(found[0], PartitionSearchResult)
        all_vids = [x.vid for x in found]
        self.assertIn(partition1.vid, all_vids)

    # _index_document tests
    def test_adds_partition_document_to_the_index(self):
        PartitionFactory._meta.sqlalchemy_session = self.my_library.database.session
        partition1 = PartitionFactory()
        self.my_library.database.session.commit()
        self.backend.partition_index.index_one(partition1)

        # search just added document.
        query = """
            SELECT vid
            FROM partition_index;
        """
        result = self.backend.library.database.connection.execute(query).fetchall()
        self.assertEqual(result[0][0], partition1.vid)

    # _delete tests
    def test_deletes_partition_from_index(self):
        PartitionFactory._meta.sqlalchemy_session = self.my_library.database.session
        partition1 = PartitionFactory()
        self.my_library.database.session.commit()

        self.backend.partition_index.index_one(partition1)

        query = text("""
            SELECT vid
            FROM partition_index
            WHERE vid = :vid;
        """)

        result = self.backend.library.database.connection.execute(query, vid=partition1.vid).fetchall()
        self.assertEqual(result, [(partition1.vid,)])

        # deleting
        self.backend.partition_index._delete(vid=partition1.vid)

        # assert document is deleted.
        result = self.backend.library.database.connection.execute(query, vid=partition1.vid).fetchall()
        self.assertEqual(result, [])


def _assert_resets_index(backend, index_name):
    """ Resets index and asserts index table dropped.

    Args:
        backend (BaseSearchBackend subclass instance).
        index_name (str): name of the index to reset.

    Raises:
        AssertionError if table does not exist before reset.
        AssertionError if table exists after reset.

    """

    query = text("""
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name=:index_name;
    """)
    execute = backend.library.database.connection.execute

    # Table exists before drop.
    result = execute(query, index_name=index_name).fetchall()
    mismatch_msg = 'Inapropriate result found while looking for created {} index.'.format(index_name)
    assert result == [(index_name,)], mismatch_msg

    # drop
    getattr(backend, index_name).reset()

    # table does not exist after drop
    result = execute(query, index_name=index_name).fetchall()
    assert result == [], 'Inapropriate result found while looking for deleted {} index.'.format(index_name)


def _assert_table_exists(backend, table_name):
    """ Asserts that table with given name exists.

    Args:
        backend (BaseSearchBackend subclass instance).
        table_name (str): name of the table to search.

    Raises:
        AssertionError if table does not exist.

    """
    query = """
        select name from sqlite_master where type='table' and name=:table_name;
    """
    result = backend.library.database.connection.execute(query, table_name=table_name).fetchall()
    mismatch_msg = 'Result mismatch while looking for {} table existance.'.format(table_name)
    assert result == [(table_name,)], mismatch_msg
