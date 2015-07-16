# -*- coding: utf-8 -*-

import logging
from sqlalchemy.sql.expression import text
import struct

from ambry.library.search_backends.base import BaseDatasetIndex, BasePartitionIndex,\
    BaseIdentifierIndex, BaseSearchBackend, IdentifierSearchResult,\
    DatasetSearchResult, PartitionSearchResult

from ambry.library.search_backends.base import SearchTermParser
from ambry.util import get_logger

logger = get_logger(__name__, level=logging.DEBUG)


class SQLiteSearchBackend(BaseSearchBackend):

    def _get_dataset_index(self):
        """ Returns dataset index. """
        # returns initialized dataset index
        return DatasetSQLiteIndex(backend=self)

    def _get_partition_index(self):
        """ Returns partition index. """
        return PartitionSQLiteIndex(backend=self)

    def _get_identifier_index(self):
        """ Returns identifier index. """
        return IdentifierSQLiteIndex(backend=self)


class DatasetSQLiteIndex(BaseDatasetIndex):

    def __init__(self, backend=None):
        assert backend is not None, 'backend argument can not be None.'
        super(self.__class__, self).__init__(backend=backend)

        logger.debug('sqlitesearch: creating dataset FTS table.')

        query = """\
            CREATE VIRTUAL TABLE dataset_index USING fts3(
                vid VARCHAR(256) NOT NULL,
                title TEXT,
                keywords TEXT,
                doc TEXT
            );
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

        raw_connection = self.backend.library.database.engine.raw_connection()
        raw_connection.create_function('rank', 1, _make_rank_func((1., .1, 0, 0)))

        query = ("""
            SELECT vid, rank(matchinfo(dataset_index)) AS score
            FROM dataset_index
            WHERE vid MATCH :part;
        """)  # FIXME: ordery by rank.
        results = self.backend.library.database.connection.execute(query, part=search_phrase).fetchall()
        datasets = {}
        for result in results:
            vid, score = result
            datasets[vid] = DatasetSearchResult()
            datasets[vid].vid = vid
            datasets[vid].b_score = score

        # FIXME: extend with partitions.
        return datasets.values()

    def _index_document(self, document, force=False):
        """ Adds document to the index. """
        query = text("""
            INSERT INTO dataset_index(vid, title, keywords, doc)
            VALUES(:vid, :title, :keywords, :doc);
        """)
        self.backend.library.database.connection.execute(query, **document)

    def reset(self):
        """ Drops index table. """
        query = """
            DROP TABLE dataset_index;
        """
        self.backend.library.database.connection.execute(query)

    def _delete(self, vid=None):
        """ Deletes given dataset from index.

        Args:
            vid (str): dataset vid.

        """
        query = text("""
            DELETE FROM dataset_index
            WHERE vid = :vid;
        """)
        self.backend.library.database.connection.execute(query, vid=vid)


class IdentifierSQLiteIndex(BaseIdentifierIndex):

    def __init__(self, backend=None):
        assert backend is not None, 'backend argument can not be None.'
        super(self.__class__, self).__init__(backend=backend)

        logger.debug('sqlitesearch: creating identifier FTS table.')

        query = """\
            CREATE VIRTUAL TABLE identifier_index USING fts3(
                identifier VARCHAR(256) NOT NULL,
                type VARCHAR(256) NOT NULL,
                name TEXT
            );
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

        query = ("""
            SELECT identifier, type, name, 0
            FROM identifier_index
            WHERE identifier MATCH :part;
        """)  # FIXME: Add score. Do we really need scores for Identifiers?

        results = self.backend.library.database.connection.execute(query, part=search_phrase).fetchall()
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
        """ Drops index table. """
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


class PartitionSQLiteIndex(BasePartitionIndex):

    def __init__(self, backend=None):
        assert backend is not None, 'backend argument can not be None.'
        super(self.__class__, self).__init__(backend=backend)

        logger.debug('sqlitesearch: creating partition FTS table.')

        query = """\
            CREATE VIRTUAL TABLE partition_index USING fts3(
                vid VARCHAR(256) NOT NULL,
                dataset_vid VARCHAR(256) NOT NULL,
                title TEXT,
                keywords TEXT,
                doc TEXT
            );
        """
        self.backend.library.database.connection.execute(query)

    def search(self, search_phrase, limit=None):
        """ Finds partitions by search phrase.

        Args:
            search_phrase (str or unicode):
            limit (int, optional): how many results to generate. None means without limit.

        Generates:
            PartitionSearchResult instances.
        """

        raw_connection = self.backend.library.database.engine.raw_connection()
        raw_connection.create_function('rank', 1, _make_rank_func((1., .1, 0, 0)))

        query = ("""
            SELECT vid, dataset_vid, rank(matchinfo(partition_index)) AS score
            FROM partition_index
            WHERE vid MATCH :part;
        """)  # FIXME: order by rank.
        results = self.backend.library.database.connection.execute(query, part=search_phrase).fetchall()
        for result in results:
            vid, dataset_vid, score = result
            yield PartitionSearchResult(
                vid=vid, dataset_vid=dataset_vid, score=score)

    def _index_document(self, document, force=False):
        """ Adds parition document to the index. """
        query = text("""
            INSERT INTO partition_index(vid, dataset_vid, title, keywords)
            VALUES(:vid, :dataset_vid, :title, :keywords);
        """)
        self.backend.library.database.connection.execute(query, **document)

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
        query = text("""
            DELETE FROM partition_index
            WHERE vid = :vid;
        """)
        self.backend.library.database.connection.execute(query, vid=vid)


def _make_rank_func(weights):
    def rank(matchinfo):
        # matchinfo is defined as returning 32-bit unsigned integers
        # in machine byte order
        # http://www.sqlite.org/fts3.html#matchinfo
        # and struct defaults to machine byte order
        matchinfo = struct.unpack("I"*(len(matchinfo)/4), matchinfo)
        it = iter(matchinfo[2:])
        return sum(x[0]*w/x[1]
                   for x, w in zip(zip(it, it, it), weights)
                   if x[1])
    return rank
