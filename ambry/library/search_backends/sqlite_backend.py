# -*- coding: utf-8 -*-

from collections import defaultdict
import logging
import struct

from sqlalchemy.sql.expression import text

from ambry.orm.dataset import Dataset

from ambry.library.search_backends.base import BaseDatasetIndex, BasePartitionIndex,\
    BaseIdentifierIndex, BaseSearchBackend, IdentifierSearchResult,\
    DatasetSearchResult, PartitionSearchResult, SearchTermParser

from ambry.util import get_logger

logger = get_logger(__name__, level=logging.DEBUG, propagate=False)


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

    def _and_join(self, terms):
        """ AND join of the terms.

        Args:
            terms (list):

        Examples:
            self._and_join(['term1', 'term2'])

        Returns:
            str
        """
        if len(terms) > 1:
            return ' '.join([self._or_join(t) for t in terms])
        else:
            return self._or_join(terms[0])


class DatasetSQLiteIndex(BaseDatasetIndex):

    def __init__(self, backend=None):
        assert backend is not None, 'backend argument can not be None.'
        super(self.__class__, self).__init__(backend=backend)

        logger.debug('Creating dataset FTS table.')

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
        # SQLite FTS can't find terms with `-`, therefore all hyphens replaced with underscore before save.
        # Now to make proper query we need to replace all hypens in the search phrase.
        # See http://stackoverflow.com/questions/3865733/how-do-i-escape-the-character-in-sqlite-fts3-queries
        search_phrase = search_phrase.replace('-', '_')
        match_query = self._make_query_from_terms(search_phrase)

        raw_connection = self.backend.library.database.engine.raw_connection()
        raw_connection.create_function('rank', 1, _make_rank_func((1., .1, 0, 0)))

        query = ("""
            SELECT vid, rank(matchinfo(dataset_index)) AS score
            FROM dataset_index
            WHERE dataset_index MATCH :match_query
            ORDER BY score DESC;
        """)

        logger.debug('Searching datasets using `{}` query.'.format(match_query))
        results = self.backend.library.database.connection.execute(query, match_query=match_query).fetchall()
        datasets = defaultdict(DatasetSearchResult)
        for result in results:
            vid, score = result
            datasets[vid] = DatasetSearchResult()
            datasets[vid].vid = vid
            datasets[vid].b_score = score

        logger.debug('Extending datasets with partitions.')
        for partition in self.backend.partition_index.search(search_phrase):
            datasets[partition.dataset_vid].p_score += partition.score
            datasets[partition.dataset_vid].partitions.add(partition.vid)
        return list(datasets.values())

    def list_documents(self, limit = None):
        """
        List document vids.

        :param limit: If not empty, the maximum number of results to return
        :return:
        """
        limit_str = 'LIMIT {}'.format(limit) if limit else ''

        query = ("SELECT vid FROM dataset_index "+limit_str)

        for row in self.backend.library.database.connection.execute(query).fetchall():
            yield row['vid']


    def _as_document(self, dataset):
        """ Converts dataset to document indexed by to FTS index.

        Args:
            dataset (orm.Dataset): dataset to convert.

        Returns:
            dict with structure matches to BaseDatasetIndex._schema.

        """
        assert isinstance(dataset, Dataset)

        doc = super(self.__class__, self)._as_document(dataset)

        # SQLite FTS can't find terms with `-`, replace it with underscore here and while searching.
        # See http://stackoverflow.com/questions/3865733/how-do-i-escape-the-character-in-sqlite-fts3-queries
        doc['keywords'] = doc['keywords'].replace('-', '_')
        doc['doc'] = doc['doc'].replace('-', '_')
        doc['title'] = doc['title'].replace('-', '_')
        return doc

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

    def is_indexed(self, dataset):
        """ Returns True if dataset is already indexed. Otherwise returns False. """
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


class IdentifierSQLiteIndex(BaseIdentifierIndex):

    def __init__(self, backend=None):
        assert backend is not None, 'backend argument can not be None.'
        super(self.__class__, self).__init__(backend=backend)

        logger.debug('Creating identifier FTS table.')

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
            WHERE identifier MATCH :part; """)

        results = self.backend.library.database.connection.execute(query, part=search_phrase).fetchall()
        for result in results:
            vid, type, name, score = result
            yield IdentifierSearchResult(
                score=score, vid=vid,
                type=type, name=name)

    def list_documents(self, limit=None):
        """
        List document vids.

        :param limit: If not empty, the maximum number of results to return
        :return:
        """
        limit_str = 'LIMIT {}'.format(limit) if limit else ''

        query = ("SELECT identifier FROM identifier_index " + limit_str)

        for row in self.backend.library.database.connection.execute(query).fetchall():
            yield row['identifier']

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


class PartitionSQLiteIndex(BasePartitionIndex):

    def __init__(self, backend=None):
        assert backend is not None, 'backend argument can not be None.'
        super(self.__class__, self).__init__(backend=backend)

        logger.debug('Creating partition FTS table.')

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

    def search(self, search_phrase, limit=None):
        """ Finds partitions by search phrase.

        Args:
            search_phrase (str or unicode):
            limit (int, optional): how many results to generate. None means without limit.

        Generates:
            PartitionSearchResult instances.
        """

        # SQLite FTS can't find terms with `-`, therefore all hyphens replaced with underscore before save.
        # Now to make proper query we need to replace all hypens in the search phrase.
        # See http://stackoverflow.com/questions/3865733/how-do-i-escape-the-character-in-sqlite-fts3-queries
        search_phrase = search_phrase.replace('-', '_')
        terms = SearchTermParser().parse(search_phrase)
        from_year = terms.pop('from', None)
        to_year = terms.pop('to', None)

        match_query = self._make_query_from_terms(terms)

        raw_connection = self.backend.library.database.engine.raw_connection()
        raw_connection.create_function('rank', 1, _make_rank_func((1., .1, 0, 0)))


        # SQLite FTS implementation does not allow to create indexes on FTS tables.
        # see https://sqlite.org/fts3.html 1.5. Summary, p 1:
        # ... it is not possible to create indices ...
        #
        # So, filter years range here.
        if match_query:
            query = text("""
                SELECT vid, dataset_vid, rank(matchinfo(partition_index)) AS score, from_year, to_year
                FROM partition_index
                WHERE partition_index MATCH :match_query
                ORDER BY score DESC;
            """)
            results = self.backend.library.database.connection\
                .execute(query, match_query=match_query)\
                .fetchall()
        else:
            query = text("""
                SELECT vid, dataset_vid, rank(matchinfo(partition_index)), from_year, to_year AS score
                FROM partition_index""")
            results = self.backend.library.database.connection\
                .execute(query)\
                .fetchall()

        for result in results:
            vid, dataset_vid, score, db_from_year, db_to_year = result
            if from_year and from_year < db_from_year:
                continue
            if to_year and to_year > db_to_year:
                continue
            yield PartitionSearchResult(
                vid=vid, dataset_vid=dataset_vid, score=score)

    def list_documents(self, limit=None):
        """
        List document vids.

        :param limit: If not empty, the maximum number of results to return
        :return:
        """
        limit_str = 'LIMIT {}'.format(limit) if limit else ''

        query = ("SELECT vid FROM partition_index " + limit_str)

        for row in self.backend.library.database.connection.execute(query).fetchall():
            yield row['vid']

    def _as_document(self, partition):
        """ Converts partition to document indexed by to FTS index.

        Args:
            partition (orm.Partition): partition to convert.

        Returns:
            dict with structure matches to BasePartitionIndex._schema.

        """
        doc = super(self.__class__, self)._as_document(partition)

        # SQLite FTS can't find terms with `-`, replace it with underscore here and while searching.
        # See http://stackoverflow.com/questions/3865733/how-do-i-escape-the-character-in-sqlite-fts3-queries
        doc['keywords'] = doc['keywords'].replace('-', '_')
        doc['doc'] = doc['doc'].replace('-', '_')
        doc['title'] = doc['title'].replace('-', '_')

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
            VALUES(:vid, :dataset_vid, :title, :keywords, :doc, :from_year, :to_year); """)
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
        partitions = []

        query = text("""
            SELECT dataset_vid, vid
            FROM partition_index;""")

        for result in self.backend.library.database.connection.execute(query):
            dataset_vid, vid = result
            partitions.append(PartitionSearchResult(dataset_vid=dataset_vid, vid=vid, score=1))
        return partitions


def _make_rank_func(weights):
    def rank(matchinfo):
        # matchinfo is defined as returning 32-bit unsigned integers
        # in machine byte order
        # http://www.sqlite.org/fts3.html#matchinfo
        # and struct defaults to machine byte order
        matchinfo = struct.unpack('I' * (len(matchinfo) / 4), matchinfo)
        it = iter(matchinfo[2:])
        return sum(x[0] * w / x[1]
                   for x, w in zip(list(zip(it, it, it)), weights)
                   if x[1])
    return rank
