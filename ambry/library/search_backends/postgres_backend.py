# -*- coding: utf-8 -*-

import logging

from sqlalchemy.sql.expression import text

from ambry.library.search_backends.base import BaseDatasetIndex, BasePartitionIndex,\
    BaseIdentifierIndex, BaseSearchBackend, IdentifierSearchResult,\
    DatasetSearchResult, PartitionSearchResult, SearchTermParser

from ambry.util import get_logger

logger = get_logger(__name__, level=logging.INFO, propagate=False)

# FIXME: Implement.


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


class DatasetPostgreSQLIndex(BaseDatasetIndex):

    def __init__(self, backend=None):
        assert backend is not None, 'backend argument can not be None.'
        super(self.__class__, self).__init__(backend=backend)

        logger.debug('Creating dataset FTS table.')
        '''
        query = """\
            CREATE VIRTUAL TABLE dataset_index USING fts3(
                vid VARCHAR(256) NOT NULL,
                title TEXT,
                keywords TEXT,
                doc TEXT
            );
        """
        self.backend.library.database.connection.execute(query)
        '''

    def search(self, search_phrase, limit=None):
        """ Finds datasets by search phrase.

        Args:
            search_phrase (str or unicode):
            limit (int, optional): how many results to return. None means without limit.

        Returns:
            list of DatasetSearchResult instances.

        """
        # PostgreSQL FTS can't find terms with `-`, therefore all hyphens replaced with underscore before save.
        # Now to make proper query we need to replace all hypens in the search phrase.
        # See http://stackoverflow.com/questions/3865733/how-do-i-escape-the-character-in-PostgreSQL-fts3-queries
        search_phrase = search_phrase.replace('-', '_')
        match_query = self._make_query_from_terms(search_phrase)
        datasets = {}
        # TODO: Implement
        return datasets.values()

    def _index_document(self, document, force=False):
        """ Adds document to the index. """
        # TODO: Implement
        pass
        '''
        query = text("""
            INSERT INTO dataset_index(vid, title, keywords, doc)
            VALUES(:vid, :title, :keywords, :doc);
        """)
        self.backend.library.database.connection.execute(query, **document)
        '''

    def reset(self):
        """ Drops index table. """
        # FIXME: test
        query = """
            DROP TABLE dataset_index;
        """
        self.backend.library.database.connection.execute(query)

    def _delete(self, vid=None):
        """ Deletes given dataset from index.

        Args:
            vid (str): dataset vid.

        """
        # FIXME: test
        query = text("""
            DELETE FROM dataset_index
            WHERE vid = :vid;
        """)
        self.backend.library.database.connection.execute(query, vid=vid)

    def is_indexed(self, dataset):
        """ Returns True if dataset is already indexed. Otherwise returns False. """
        # FIXME: Test
        query = text("""
            SELECT vid
            FROM dataset_index
            WHERE vid = :vid;
        """)
        result = self.backend.library.database.connection.execute(query, vid=dataset.vid)
        return bool(result.fetchall())

    def all(self):
        """ Returns list with all indexed datasets. """
        # FIXME: Test
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


class IdentifierPostgreSQLIndex(BaseIdentifierIndex):

    def __init__(self, backend=None):
        assert backend is not None, 'backend argument can not be None.'
        super(self.__class__, self).__init__(backend=backend)

        logger.debug('Creating identifier FTS table.')
        # FIXME: Implement
        '''

        query = """\
            CREATE VIRTUAL TABLE identifier_index USING fts3(
                identifier VARCHAR(256) NOT NULL,
                type VARCHAR(256) NOT NULL,
                name TEXT
            );
        """
        self.backend.library.database.connection.execute(query)
        '''

    def search(self, search_phrase, limit=None):
        """ Finds identifiers by search phrase.

        Args:
            search_phrase (str or unicode):
            limit (int, optional): how many results to return. None means without limit.

        Returns:
            list of IdentifierSearchResult instances.

        """
        # FIXME: Implement
        return []

    def _index_document(self, identifier, force=False):
        """ Adds identifier document to the index. """
        # FIXME:
        '''
        query = text("""
            INSERT INTO identifier_index(identifier, type, name)
            VALUES(:identifier, :type, :name);
        """)
        self.backend.library.database.connection.execute(query, **identifier)
        '''
        pass

    def reset(self):
        """ Drops index table. """
        # FIXME: Test
        query = """
            DROP TABLE identifier_index;
        """
        self.backend.library.database.connection.execute(query)

    def _delete(self, identifier=None):
        """ Deletes given identifier from index.

        Args:
            identifier (str): identifier of the document to delete.

        """
        # FIXME: Test
        query = text("""
            DELETE FROM identifier_index
            WHERE identifier = :identifier;
        """)
        self.backend.library.database.connection.execute(query, identifier=identifier)

    def is_indexed(self, identifier):
        """ Returns True if identifier is already indexed. Otherwise returns False. """
        # FIXME: Test
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
        # FIXME: test

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
        # FIXME:
        '''
        query = """\
            CREATE VIRTUAL TABLE IF NOT EXISTS partition_index USING fts3(
                vid VARCHAR(256) NOT NULL,
                dataset_vid VARCHAR(256) NOT NULL,
                from_year INTEGER,
                to_year INTEGER,
                title TEXT,
                keywords TEXT,
                doc TEXT
            );
        """
        self.backend.library.database.connection.execute(query)
        '''

    def search(self, search_phrase, limit=None):
        """ Finds partitions by search phrase.

        Args:
            search_phrase (str or unicode):
            limit (int, optional): how many results to generate. None means without limit.

        Generates:
            PartitionSearchResult instances.
        """

        search_phrase = search_phrase.replace('-', '_')
        terms = SearchTermParser().parse(search_phrase)
        match_query = self._make_query_from_terms(terms)
        results = []
        # FIXME:

        for result in results:
            vid, dataset_vid, score, db_from_year, db_to_year = result
            yield PartitionSearchResult(
                vid=vid, dataset_vid=dataset_vid, score=score)

    def _index_document(self, document, force=False):
        """ Adds parition document to the index. """

        '''
        query = text("""
            INSERT INTO partition_index(vid, dataset_vid, title, keywords, doc, from_year, to_year)
            VALUES(:vid, :dataset_vid, :title, :keywords, :doc, :from_year, :to_year); """)
        self.backend.library.database.connection.execute(
            query, from_year=from_year, to_year=to_year, **document)
        '''
        # FIXME:
        pass

    def reset(self):
        """ Drops index table. """
        # FIXME: Test
        query = """
            DROP TABLE partition_index;
        """
        self.backend.library.database.connection.execute(query)

    def _delete(self, vid=None):
        """ Deletes partition with given vid from index.

        Args:
            vid (str): vid of the partition document to delete.

        """
        # FIXME: Test
        query = text("""
            DELETE FROM partition_index
            WHERE vid = :vid;
        """)
        self.backend.library.database.connection.execute(query, vid=vid)

    def is_indexed(self, partition):
        """ Returns True if partition is already indexed. Otherwise returns False. """
        # FIXME: Test
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
