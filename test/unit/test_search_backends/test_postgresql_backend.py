# -*- coding: utf-8 -*-

from ambry.library.search_backends.postgres_backend import PostgreSQLSearchBackend
from ambry.library import new_library
from ambry.util import AttrDict

from test.test_base import PostgreSQLTestBase
from test.unit.orm_factories import PartitionFactory

from sqlalchemy.exc import ProgrammingError


class PostgreSQLBackendBaseTest(PostgreSQLTestBase):
    def setUp(self):
        super(PostgreSQLBackendBaseTest, self).setUp()

        # create test database
        rc = self.get_rc()
        self._real_test_database = rc.config['database']['test-database']
        rc.config['database']['test-database'] = self.dsn
        self.library = new_library(rc)
        self.backend = PostgreSQLSearchBackend(self.library)

    def tearDown(self):
        super(PostgreSQLBackendBaseTest, self).tearDown()

        # restore database config
        rc = self.get_rc()
        rc.config['database']['test-database'] = self._real_test_database


class PostgreSQLSearchBackendTest(PostgreSQLBackendBaseTest):

    # _or_join tests
    def test_joins_list_with_or(self):
        ret = self.backend._or_join(['term1', 'term2'])
        self.assertEqual(ret, 'term1 | term2')

    def test_returns_string_as_is(self):
        ret = self.backend._or_join('term1')
        self.assertEqual(ret, 'term1')

    # _and_join tests
    def test_joins_string_with_and(self):
        ret = self.backend._or_join(['term1', 'term2'])
        self.assertEqual(ret, 'term1 | term2')

    # _join_keywords tests
    def test_joins_keywords_with_and(self):
        ret = self.backend._join_keywords(['keyword1', 'keyword2'])
        self.assertEqual(ret, '(keyword1 & keyword2)')


class DatasetPostgreSQLIndexTest(PostgreSQLBackendBaseTest):

    def test_creates_dataset_index(self):
        with self.library.database._engine.connect() as conn:
            query = """
                SELECT * from dataset_index;
            """
            result = conn.execute(query).fetchall()
            self.assertEqual(result, [])

    # search() tests
    def test_returns_found_datasets(self):
        dataset1 = self.new_db_dataset(self.library.database, n=0)
        dataset2 = self.new_db_dataset(self.library.database, n=1)
        dataset1.config.metadata.about.title = 'title'
        dataset2.config.metadata.about.title = 'title'
        self.backend.dataset_index.index_one(dataset1)
        self.backend.dataset_index.index_one(dataset2)

        # testing.
        ret = self.backend.dataset_index.search('title')
        self.assertEqual(len(ret), 2)
        self.assertEqual(
            sorted([dataset1.vid, dataset2.vid]),
            sorted([x.vid for x in ret]))

    def test_returns_limited_datasets(self):
        for n in range(4):
            ds = self.new_db_dataset(self.library.database, n=n)
            ds.config.metadata.about.title = 'title'
            self.backend.dataset_index.index_one(ds)

        ret = self.backend.dataset_index.search('title')
        self.assertEqual(len(ret), 4)

        # testing
        ret = self.backend.dataset_index.search('title', limit=2)
        self.assertEqual(len(ret), 2)

    # reset tests
    def test_drops_dataset_index_table(self):
        self.backend.dataset_index.reset()
        with self.assertRaises(ProgrammingError):
            self.backend.library.database._engine.execute('SELECT * FROM dataset_index;')

    # is_indexed tests
    def test_returns_true_if_dataset_is_indexed(self):
        ds = self.new_db_dataset(self.library.database)
        self.backend.dataset_index.index_one(ds)
        ret = self.backend.dataset_index.is_indexed(ds)
        self.assertTrue(ret)

    def test_returns_false_if_dataset_is_not_indexed(self):
        ds = self.new_db_dataset(self.library.database)
        ret = self.backend.dataset_index.is_indexed(ds)
        self.assertFalse(ret)

    # all() tests
    def test_returns_list_with_all_indexed_datasets(self):
        ds1 = self.new_db_dataset(self.library.database, n=0)
        ds2 = self.new_db_dataset(self.library.database, n=1)
        self.backend.dataset_index.index_one(ds1)
        self.backend.dataset_index.index_one(ds2)
        ret = self.backend.dataset_index.all()
        self.assertEqual(len(ret), 2)

    # _make_query_from_terms tests
    def test_extends_query_with_limit(self):
        query, query_params = self.backend.dataset_index._make_query_from_terms('term1', limit=10)
        self.assertIn('LIMIT :limit', str(query))
        self.assertIn('limit', query_params)
        self.assertEqual(query_params['limit'], 10)

    # _delete tests
    def test_deletes_given_dataset_from_index(self):
        ds1 = self.new_db_dataset(self.library.database, n=0)
        self.backend.dataset_index.index_one(ds1)

        # was it really added?
        ret = self.backend.dataset_index.all()
        self.assertEqual(len(ret), 1)

        # delete and test
        self.backend.dataset_index._delete(vid=ds1.vid)
        ret = self.backend.dataset_index.all()
        self.assertEqual(len(ret), 0)


class IdentifierPostgreSQLIndexTest(PostgreSQLBackendBaseTest):

    def test_creates_identifier_index(self):
        with self.library.database._engine.connect() as conn:
            query = """
                SELECT * from identifier_index;
            """
            result = conn.execute(query).fetchall()
            self.assertEqual(result, [])

    # search() tests
    def test_returns_found_identifiers(self):
        self.backend.identifier_index.index_one({
            'identifier': 'id1',
            'type': 'dataset',
            'name': 'name1'})
        self.backend.identifier_index.index_one({
            'identifier': 'id2',
            'type': 'dataset',
            'name': 'name2'})

        # testing.
        ret = list(self.backend.identifier_index.search('name'))
        self.assertEqual(len(ret), 2)
        self.assertListEqual(['id1', 'id2'], [x.vid for x in ret])

    def test_returns_limited_identifiers(self):
        for n in range(4):
            self.backend.identifier_index.index_one({
                'identifier': 'id{}'.format(n),
                'type': 'dataset',
                'name': 'name-{}'.format(n)})

        ret = list(self.backend.identifier_index.search('name'))
        self.assertEqual(len(ret), 4)

        # testing
        ret = list(self.backend.identifier_index.search('name', limit=2))
        self.assertEqual(len(ret), 2)

    # reset tests
    def test_drops_identifier_index_table(self):
        self.backend.identifier_index.reset()
        with self.assertRaises(ProgrammingError):
            self.backend.library.database._engine.execute('SELECT * FROM identifier_index;')

    # is_indexed tests
    def test_returns_true_if_identifier_is_indexed(self):
        identifier = {
            'identifier': 'id1',
            'type': 'dataset',
            'name': 'name1'}
        self.backend.identifier_index.index_one(identifier)
        ret = self.backend.identifier_index.is_indexed(identifier)
        self.assertTrue(ret)

    def test_returns_false_if_identifier_is_not_indexed(self):
        ret = self.backend.identifier_index.is_indexed({'identifier': 'id1'})
        self.assertFalse(ret)

    # all() tests
    def test_returns_list_with_all_indexed_identifiers(self):
        identifier1 = {
            'identifier': 'id1',
            'type': 'dataset',
            'name': 'name1'}
        identifier2 = {
            'identifier': 'id2',
            'type': 'dataset',
            'name': 'name2'}
        self.backend.identifier_index.index_one(identifier1)
        self.backend.identifier_index.index_one(identifier2)
        ret = self.backend.identifier_index.all()
        self.assertEqual(len(ret), 2)

    # _delete tests
    def test_deletes_given_identifier_from_index(self):
        identifier2 = {
            'identifier': 'id2',
            'type': 'dataset',
            'name': 'name2'}
        self.backend.identifier_index.index_one(identifier2)

        # was it really added?
        ret = self.backend.identifier_index.all()
        self.assertEqual(len(ret), 1)

        # delete and test
        self.backend.identifier_index._delete(identifier='id2')
        ret = self.backend.identifier_index.all()
        self.assertEqual(len(ret), 0)


class PartitionPostgreSQLIndexTest(PostgreSQLBackendBaseTest):

    def test_creates_partition_index(self):
        with self.library.database._engine.connect() as conn:
            query = """
                SELECT * from partition_index;
            """
            result = conn.execute(query).fetchall()
            self.assertEqual(result, [])

    # search() tests
    def test_returns_found_partitions(self):
        dataset = self.new_db_dataset(self.library.database, n=0)
        PartitionFactory._meta.sqlalchemy_session = self.library.database.session
        partition = PartitionFactory(dataset=dataset)
        self.backend.partition_index.index_one(partition)

        # testing.
        ret = list(self.backend.partition_index.search(partition.vid))
        self.assertEqual(len(ret), 1)
        self.assertListEqual([partition.vid], [x.vid for x in ret])

    # reset tests
    def test_drops_partition_index_table(self):
        self.backend.partition_index.reset()
        with self.assertRaises(ProgrammingError):
            self.backend.library.database._engine.execute('SELECT * FROM partition_index;')

    # is_indexed tests
    def test_returns_true_if_partition_is_indexed(self):
        PartitionFactory._meta.sqlalchemy_session = self.library.database.session
        dataset = self.new_db_dataset(self.library.database, n=0)
        partition = PartitionFactory(dataset=dataset)
        self.backend.partition_index.index_one(partition)
        ret = self.backend.partition_index.is_indexed(partition)
        self.assertTrue(ret)

    def test_returns_false_if_partition_is_not_indexed(self):
        # to test we need just vid from the given object. So do not create partition, create partition like
        # instead. It makes that test more quick.
        partition = AttrDict(vid='vid1')
        ret = self.backend.partition_index.is_indexed(partition)
        self.assertFalse(ret)

    # _delete tests
    def test_deletes_given_partition_from_index(self):
        PartitionFactory._meta.sqlalchemy_session = self.library.database.session
        dataset = self.new_db_dataset(self.library.database, n=0)
        partition = PartitionFactory(dataset=dataset)
        self.backend.partition_index.index_one(partition)

        # was it really added?
        ret = self.backend.partition_index.all()
        self.assertEqual(len(ret), 1)

        # delete and test
        self.backend.partition_index._delete(vid=partition.vid)
        ret = self.backend.partition_index.all()
        self.assertEqual(len(ret), 0)
