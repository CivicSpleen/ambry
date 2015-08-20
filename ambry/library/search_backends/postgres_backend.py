# -*- coding: utf-8 -*-

from sqlalchemy.sql.expression import text

from ambry.library.search_backends.base import BaseDatasetIndex, BasePartitionIndex,\
    BaseIdentifierIndex, BaseSearchBackend, IdentifierSearchResult,\
    DatasetSearchResult, PartitionSearchResult

from ambry.util import get_logger

logger = get_logger(__name__)


class PostgreSQLSearchBackend(BaseSearchBackend):

    def _get_dataset_index(self):
        """ Returns initialized dataset index. """
        return DatasetPostgreSQLIndex(backend=self)

    def _get_partition_index(self):
        """ Returns partition index. """
        return PartitionPostgreSQLIndex(backend=self)

    def _get_identifier_index(self):
        """ Returns identifier index. """
        return IdentifierPostgreSQLIndex(backend=self)

    def _or_join(self, terms):
        """ Joins terms using OR operator.

        Args:
            terms (list): terms to join

        Examples:
            self._or_join(['term1', 'term2']) -> 'term1 | term2'

        Returns:
            str
        """

        if isinstance(terms, (tuple, list)):
            if len(terms) > 1:
                return ' | '.join(terms)
            else:
                return terms[0]
        else:
            return terms

    def _and_join(self, terms):
        """ AND join of the terms.

        Args:
            terms (list):

        Examples:
            self._and_join(['term1', 'term2']) -> 'term1 & term2'

        Returns:
            str
        """
        if len(terms) > 1:
            return ' & '.join([self._or_join(t) for t in terms])
        else:
            return self._or_join(terms[0])

    def _join_keywords(self, keywords):
        if isinstance(keywords, (list, tuple)):
            return '(' + self._and_join(keywords) + ')'
        return keywords


class DatasetPostgreSQLIndex(BaseDatasetIndex):

    def __init__(self, backend=None):
        assert backend is not None, 'backend argument can not be None.'
        super(self.__class__, self).__init__(backend=backend)

        logger.debug('Creating dataset FTS table and index.')

        # create table for dataset documents. Create special table for search to make it easy to replace one
        # FTS engine with another.
        query = """\
            CREATE TABLE dataset_index (
                vid VARCHAR(256) NOT NULL,
                title TEXT,
                keywords VARCHAR(256)[],
                doc tsvector
            );
        """
        self.backend.library.database.connection.execute(query)

        # create FTS index on doc field. # FIXME:
        query = """\
            CREATE INDEX dataset_index_doc_idx ON dataset_index USING gin(doc);
        """
        self.backend.library.database.connection.execute(query)

        # Create index on keyword field
        query = """\
            CREATE INDEX dataset_index_keywords_idx on dataset_index USING gin(keywords);
        """
        self.backend.library.database.connection.execute(query)

    def search(self, search_phrase, limit=None):
        """ Finds datasets by search phrase.

        Args:
            search_phrase (str or unicode):
            limit (int, optional): how many results to return. None means without limit.

        Returns:
            list of DatasetSearchResult instances.

        """
        # FIXME: Implement limit.
        query, query_params = self._make_query_from_terms(search_phrase, limit=limit)
        results = self.backend.library.database.connection.execute(query, **query_params)
        datasets = {}
        for result in results:
            vid = result[0]
            b_score = 0
            p_score = 0
            partitions = set()
            res = DatasetSearchResult()
            res.b_score = b_score
            res.p_score = p_score
            res.partitions = partitions
            res.vid = vid
            datasets[vid] = res

        logger.debug('Extending datasets with partitions.')
        for partition in self.backend.partition_index.search(search_phrase):
            datasets[partition.dataset_vid].p_score += partition.score
            datasets[partition.dataset_vid].partitions.add(partition.vid)

        return list(datasets.values())

    def reset(self):
        """ Drops index table. """
        query = """
            DROP TABLE dataset_index;
        """
        self.backend.library.database.connection.execute(query)

    def is_indexed(self, dataset):
        """ Returns True if dataset is already indexed. Otherwise returns False.

        Args:
            dataset (orm.Dataset):

        Returns:
            bool: True if dataset is indexed, False otherwise.
        """
        query = text("""
            SELECT vid
            FROM dataset_index
            WHERE vid = :vid;
        """)
        result = self.backend.library.database.connection.execute(query, vid=dataset.vid)
        return bool(result.fetchall())

    def all(self):
        """ Returns list with all indexed datasets. """
        datasets = []

        query = text("""
            SELECT vid
            FROM dataset_index;""")

        for result in self.backend.library.database.connection.execute(query):
            res = DatasetSearchResult()
            res.vid = result[0]
            res.b_score = 1
            datasets.append(res)
        return datasets

    def _index_document(self, document, force=False):
        """ Adds dataset document to the index. """
        query = text("""
            INSERT INTO dataset_index(vid, title, keywords, doc)
            VALUES(:vid, :title, string_to_array(:keywords, ' '), to_tsvector('english', :doc));
        """)
        self.backend.library.database.connection.execute(query, **document)

    def _make_query_from_terms(self, terms, limit=None):
        """ Creates a query for dataset from decomposed search terms.

        Args:
            terms (dict or unicode or string):

        Returns:
            tuple of (str, dict): First element is str with FTS query, second is parameters of the query.

        """

        expanded_terms = self._expand_terms(terms)

        query_parts = [
            'SELECT vid',
            'FROM dataset_index'
        ]
        query_params = {}
        where_counter = 0

        if expanded_terms['doc']:
            where_counter += 1
            query_parts.append('WHERE doc @@ to_tsquery(:doc)')
            query_params['doc'] = self.backend._and_join(expanded_terms['doc'])

        if expanded_terms['keywords']:
            query_params['keywords'] = self.backend._and_join(expanded_terms['keywords'])
            if where_counter:
                query_parts.append('AND keywords::text[] @> string_to_array(:keywords, \' \')')
            else:
                query_parts.append('WHERE keywords::text[] @> string_to_array(:keywords, \' \')')

        if limit:
            query_parts.append('LIMIT :limit')
            query_params['limit'] = limit

        query_parts.append(';')
        deb_msg = 'Dataset terms conversion: `{}` terms converted to `{}` with `{}` params query.'\
            .format(terms, query_parts, query_params)
        logger.debug(deb_msg)
        return text('\n'.join(query_parts)), query_params

    def _delete(self, vid=None):
        """ Deletes given dataset from index.

        Args:
            vid (str): dataset vid.

        """
        assert vid is not None
        query = text("""
            DELETE FROM dataset_index
            WHERE vid = :vid;
        """)
        self.backend.library.database.connection.execute(query, vid=vid)


class IdentifierPostgreSQLIndex(BaseIdentifierIndex):

    def __init__(self, backend=None):
        assert backend is not None, 'backend argument can not be None.'
        super(self.__class__, self).__init__(backend=backend)

        logger.debug('Creating identifier FTS table.')

        query = """
            CREATE TABLE identifier_index (
                identifier VARCHAR(256) NOT NULL,
                type VARCHAR(256) NOT NULL,
                name TEXT);
        """
        self.backend.library.database.connection.execute(query)

        # create index for name.
        query = """
            CREATE INDEX identifier_index_name_idx ON identifier_index USING gist (name gist_trgm_ops);
        """
        self.backend.library.database.connection.execute(query)

    def search(self, search_phrase, limit=None):
        """ Finds identifiers by search phrase.

        Args:
            search_phrase (str or unicode):
            limit (int, optional): how many results to return. None means without limit.

        Returns:
            list of IdentifierSearchResult instances.

        """
        query_parts = [
            'SELECT identifier, type, name, similarity(name, :word) AS sml',
            'FROM identifier_index',
            'WHERE name % :word',
            'ORDER BY sml DESC, name']
        query_params = {
            'word': search_phrase}

        if limit:
            query_parts.append('LIMIT :limit')
            query_params['limit'] = limit

        query_parts.append(';')

        query = text('\n'.join(query_parts))

        results = self.backend.library.database.connection.execute(query, **query_params).fetchall()
        for result in results:
            vid, type, name, score = result
            yield IdentifierSearchResult(
                score=score, vid=vid,
                type=type, name=name)

    def _index_document(self, identifier, force=False):
        """ Adds identifier document to the index. """

        query = text("""
            INSERT INTO identifier_index(identifier, type, name)
            VALUES(:identifier, :type, :name);
        """)
        self.backend.library.database.connection.execute(query, **identifier)

    def reset(self):
        """ Drops identifier index table. """
        query = """
            DROP TABLE identifier_index;
        """
        self.backend.library.database.connection.execute(query)

    def _delete(self, identifier=None):
        """ Deletes given identifier from index.

        Args:
            identifier (str): identifier of the document to delete.

        """
        query = text("""
            DELETE FROM identifier_index
            WHERE identifier = :identifier;
        """)
        self.backend.library.database.connection.execute(query, identifier=identifier)

    def is_indexed(self, identifier):
        """ Returns True if identifier is already indexed. Otherwise returns False. """
        query = text("""
            SELECT identifier
            FROM identifier_index
            WHERE identifier = :identifier;
        """)
        result = self.backend.library.database.connection.execute(query, identifier=identifier['identifier'])
        return bool(result.fetchall())

    def all(self):
        """ Returns list with all indexed identifiers. """
        identifiers = []

        query = text("""
            SELECT identifier, type, name
            FROM identifier_index;""")

        for result in self.backend.library.database.connection.execute(query):
            vid, type_, name = result
            res = IdentifierSearchResult(
                score=1, vid=vid, type=type_, name=name)
            identifiers.append(res)
        return identifiers


class PartitionPostgreSQLIndex(BasePartitionIndex):

    def __init__(self, backend=None):
        assert backend is not None, 'backend argument can not be None.'
        super(self.__class__, self).__init__(backend=backend)

        logger.debug('Creating partition FTS table.')

        # create table for partition documents. Create special table for search to make it easy to replace one
        # FTS engine with another.
        query = """\
            CREATE TABLE partition_index (
                vid VARCHAR(256) NOT NULL,
                dataset_vid VARCHAR(256) NOT NULL,
                from_year INTEGER,
                to_year INTEGER,
                title TEXT,
                keywords VARCHAR(256)[],
                doc tsvector
            );
        """
        self.backend.library.database.connection.execute(query)

        # create FTS index on doc field. # FIXME:
        query = """\
            CREATE INDEX partition_index_doc_idx ON partition_index USING gin(doc);
        """
        self.backend.library.database.connection.execute(query)

        # Create index on keywords field
        query = """\
            CREATE INDEX partition_index_keywords_idx on partition_index USING gin(keywords);
        """
        self.backend.library.database.connection.execute(query)

    def _make_query_from_terms(self, terms):
        """ Creates a query for partition from decomposed search terms.

        Args:
            terms (dict or unicode or string):

        Returns:
            tuple of (str, dict): First element is str with FTS query, second is parameters of the query.

        """
        expanded_terms = self._expand_terms(terms)

        # FIXME: add score, year_from, year_to to the query. Order by score.

        query_parts = [
            'SELECT vid, dataset_vid, 1, 1, 1',
            'FROM partition_index'
        ]
        query_params = {}
        where_count = 0

        if expanded_terms['doc']:
            query_parts.append('WHERE doc @@ to_tsquery(:doc)')
            query_params['doc'] = self.backend._and_join(expanded_terms['doc'])
            where_count += 1

        if expanded_terms['keywords']:
            query_params['keywords'] = ' '.join(expanded_terms['keywords'])
            if where_count:
                # FIXME: test me.
                query_parts.append('AND keywords::text[] @> string_to_array(:keywords, \' \')')
            else:
                query_parts.append('WHERE keywords::text[] @> string_to_array(:keywords, \' \')')
            where_count += 1

        if expanded_terms['from']:
            if where_count:
                # at least one WHERE exists
                query_parts.append('AND from_year >= :from_year')
            else:
                query_parts.append('WHERE from_year >= :from_year')
            query_params['from_year'] = expanded_terms['from']
            where_count += 1

        if expanded_terms['to']:
            if where_count:
                # at least one WHERE exists
                query_parts.append('AND to_year <= :to_year')
            else:
                query_parts.append('to_year <= :to_year')
            query_params['to_year'] = expanded_terms['to']
            where_count += 1

        query_parts.append(';')
        deb_msg = 'Dataset terms conversion: `{}` terms converted to `{}` with `{}` params query.'\
            .format(terms, query_parts, query_params)
        logger.debug(deb_msg)
        return text('\n'.join(query_parts)), query_params

    def search(self, search_phrase, limit=None):
        """ Finds partitions by search phrase.

        Args:
            search_phrase (str or unicode):
            limit (int, optional): how many results to generate. None means without limit.

        Generates:
            PartitionSearchResult instances.
        """
        query, query_params = self._make_query_from_terms(search_phrase)
        results = self.backend.library.database.connection.execute(query, **query_params)

        for result in results:
            vid, dataset_vid, score, db_from_year, db_to_year = result
            yield PartitionSearchResult(
                vid=vid, dataset_vid=dataset_vid, score=score)

    def _as_document(self, partition):
        """ Converts partition to document indexed by to FTS index.

        Args:
            partition (orm.Partition): partition to convert.

        Returns:
            dict with structure matches to BasePartitionIndex._schema.

        """
        doc = super(self.__class__, self)._as_document(partition)

        # pass time_coverage to the _index_document.
        doc['time_coverage'] = partition.time_coverage
        return doc

    def _index_document(self, document, force=False):
        """ Adds parition document to the index. """

        time_coverage = document.pop('time_coverage', [])
        from_year = None
        to_year = None
        if time_coverage:
            from_year = int(time_coverage[0])
            to_year = int(time_coverage[-1])

        query = text("""
            INSERT INTO partition_index(vid, dataset_vid, title, keywords, doc, from_year, to_year)
            VALUES(
                :vid, :dataset_vid, :title,
                string_to_array(:keywords, ' '),
                to_tsvector('english', :doc),
                :from_year, :to_year); """)

        self.backend.library.database.connection.execute(
            query, from_year=from_year, to_year=to_year, **document)

    def reset(self):
        """ Drops index table. """
        query = """
            DROP TABLE partition_index;
        """
        self.backend.library.database.connection.execute(query)

    def _delete(self, vid=None):
        """ Deletes partition with given vid from index.

        Args:
            vid (str): vid of the partition document to delete.

        """
        assert vid is not None
        query = text("""
            DELETE FROM partition_index
            WHERE vid = :vid;
        """)
        self.backend.library.database.connection.execute(query, vid=vid)

    def is_indexed(self, partition):
        """ Returns True if partition is already indexed. Otherwise returns False. """
        query = text("""
            SELECT vid
            FROM partition_index
            WHERE vid = :vid;
        """)
        result = self.backend.library.database.connection.execute(query, vid=partition.vid)
        return bool(result.fetchall())

    def all(self):
        """ Returns list with vids of all indexed partitions. """
        # FIXME: Test
        partitions = []

        query = text("""
            SELECT dataset_vid, vid
            FROM partition_index;""")

        for result in self.backend.library.database.connection.execute(query):
            dataset_vid, vid = result
            partitions.append(PartitionSearchResult(dataset_vid=dataset_vid, vid=vid, score=1))
        return partitions
