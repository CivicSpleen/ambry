# -*- coding: utf-8 -*-

from collections import defaultdict
import logging
from shutil import rmtree
import os

from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, KEYWORD, ID, NGRAMWORDS, NGRAM
from whoosh import scoring
from whoosh.qparser import QueryParser
from whoosh.query import Term

from fs.opener import fsopendir

from ambry.library.search_backends.base import BaseDatasetIndex, BasePartitionIndex,\
    BaseIdentifierIndex, BaseSearchBackend, IdentifierSearchResult,\
    DatasetSearchResult, PartitionSearchResult

from ambry.util import get_logger

logger = get_logger(__name__, level=logging.INFO, propagate=False)


class WhooshSearchBackend(BaseSearchBackend):

    def __init__(self, library):
        # each whoosh index requires root directory.
        self.root_dir = fsopendir(library._fs.search()).getsyspath('/')
        super(self.__class__, self).__init__(library)

    def _get_dataset_index(self):
        """ Returns dataset index. """
        # returns initialized dataset index
        return DatasetWhooshIndex(backend=self)

    def _get_partition_index(self):
        """ Returns partition index. """
        return PartitionWhooshIndex(backend=self)

    def _get_identifier_index(self):
        """ Returns identifier index. """
        return IdentifierWhooshIndex(backend=self)


class DatasetWhooshIndex(BaseDatasetIndex):

    def __init__(self, backend=None):
        assert backend is not None, 'backend can not be None'
        super(self.__class__, self).__init__(backend)
        self.index, self.index_dir = _init_index(
            self.backend.root_dir, self._get_generic_schema(), 'datasets')

    def reset(self):
        """ Resets index by removing index directory. """
        if os.path.exists(self.index_dir):
            rmtree(self.index_dir)
        self.index = None

    def all(self):
        """ Returns list with all indexed datasets. """
        datasets = []
        for dataset in self.index.searcher().documents():
            res = DatasetSearchResult()
            res.vid = dataset['vid']
            res.b_score = 1
            datasets.append(res)
        return datasets

    def search(self, search_phrase, limit=None):
        """ Finds datasets by search phrase.

        Args:
            search_phrase (str or unicode):
            limit (int, optional): how many results to return. None means without limit.

        Returns:
            list of DatasetSearchResult instances.

        """
        query_string = self._make_query_from_terms(search_phrase)
        self._parsed_query = query_string
        schema = self._get_generic_schema()

        parser = QueryParser('doc', schema=schema)

        query = parser.parse(query_string)

        datasets = defaultdict(DatasetSearchResult)

        # collect all datasets
        logger.debug('Searching datasets using `{}` query.'.format(query))
        with self.index.searcher() as searcher:
            results = searcher.search(query, limit=limit)
            for hit in results:
                vid = hit['vid']
                datasets[vid].vid = hit['vid']
                datasets[vid].b_score += hit.score

        # extend datasets with partitions
        logger.debug('Extending datasets with partitions.')
        for partition in self.backend.partition_index.search(search_phrase):
            datasets[partition.dataset_vid].p_score += partition.score
            datasets[partition.dataset_vid].partitions.add(partition.vid)
        return list(datasets.values())

    def _index_document(self, document, force=False):
        """ Adds dataset document to the index. """
        # Assuming document does not exist in the index, because existance is checked in the index_one method.
        if force:
            self._delete(vid=document['vid'])

        writer = self.index.writer()
        writer.add_document(**document)
        writer.commit()

    def _get_generic_schema(self):
        """ Returns whoosh's generic schema of the dataset. """
        schema = Schema(
            vid=ID(stored=True, unique=True),  # Object id
            title=NGRAMWORDS(),
            keywords=KEYWORD,  # Lists of coverage identifiers, ISO time values and GVIDs, source names, source abbrev
            doc=TEXT)  # Generated document for the core of the topic search
        return schema

    def _delete(self, vid=None):
        """ Deletes given dataset from index.

        Args:
            vid (str): dataset vid.

        """

        assert vid is not None, 'vid argument can not be None.'
        writer = self.index.writer()
        writer.delete_by_term('vid', vid)
        writer.commit()

    def is_indexed(self, dataset):
        """ Returns True if dataset is already indexed. Otherwise returns False. """
        with self.index.searcher() as searcher:
            result = searcher.search(Term('vid', dataset.vid))
            return bool(result)


class IdentifierWhooshIndex(BaseIdentifierIndex):

    def __init__(self, backend=None):
        super(self.__class__, self).__init__(backend=backend)
        self.index, self.index_dir = _init_index(
            self.backend.root_dir, self._get_generic_schema(), 'identifiers')

    def reset(self):
        if os.path.exists(self.index_dir):
            rmtree(self.index_dir)
        self.index = None

    def search(self, search_phrase, limit=None):
        """ Finds identifier by search phrase. """
        self._parsed_query = search_phrase
        schema = self._get_generic_schema()
        parser = QueryParser('name', schema=schema)
        query = parser.parse(search_phrase)

        class PosSizeWeighting(scoring.WeightingModel):

            def scorer(self, searcher, fieldname, text, qf=1):
                return self.PosSizeScorer(searcher, fieldname, text, qf=qf)

            class PosSizeScorer(scoring.BaseScorer):
                def __init__(self, searcher, fieldname, text, qf=1):
                    self.searcher = searcher
                    self.fieldname = fieldname
                    self.text = text
                    self.qf = qf
                    self.bmf25 = scoring.BM25F()

                def max_quality(self):
                    return 40

                def score(self, matcher):
                    poses = matcher.value_as('positions')
                    return (2.0 / (poses[0] + 1) + 1.0 / (len(self.text) / 4 + 1) +
                            self.bmf25.scorer(searcher, self.fieldname, self.text).score(matcher))

        with self.index.searcher(weighting=PosSizeWeighting()) as searcher:
            results = searcher.search(query, limit=limit)
            for hit in results:
                vid = hit['identifier']
                yield IdentifierSearchResult(
                    score=hit.score, vid=vid,
                    type=hit.get('type', False),
                    name=hit.get('name', ''))

    def _index_document(self, identifier, force=False):
        """ Adds identifier document to the index. """
        writer = self.index.writer()
        all_names = set([x['name'] for x in self.index.searcher().documents()])
        if identifier['name'] not in all_names:
            writer.add_document(**identifier)
            writer.commit()

    def _get_generic_schema(self):
        """ Returns whoosh's generic schema. """
        schema = Schema(
            identifier=ID(stored=True),  # Partition versioned id
            type=ID(stored=True),
            name=NGRAM(phrase=True, stored=True, minsize=2, maxsize=8))
        return schema

    def _delete(self, identifier=None):
        """ Deletes given identifier from index.

        Args:
            identifier (str): identifier of the document to delete.

        """
        assert identifier is not None, 'identifier argument can not be None.'
        writer = self.index.writer()
        writer.delete_by_term('identifier', identifier)
        writer.commit()

    def is_indexed(self, identifier):
        """ Returns True if identifier is already indexed. Otherwise returns False. """
        with self.index.searcher() as searcher:
            result = searcher.search(Term('identifier', identifier['identifier']))
            return bool(result)


class PartitionWhooshIndex(BasePartitionIndex):

    def __init__(self, backend=None):
        super(self.__class__, self).__init__(backend=backend)
        self.index, self.index_dir = _init_index(
            self.backend.root_dir, self._get_generic_schema(), 'partitions')

    def reset(self):
        if os.path.exists(self.index_dir):
            rmtree(self.index_dir)
        self.index = None

    def all(self):
        """ Returns list with all indexed partitions. """
        partitions = []
        for partition in self.index.searcher().documents():
            partitions.append(
                PartitionSearchResult(dataset_vid=partition['dataset_vid'], vid=partition['vid'], score=1))
        return partitions

    def search(self, search_phrase, limit=None):
        """ Finds partitions by search phrase.

        Args:
            search_phrase (str or unicode):
            limit (int, optional): how many results to generate. None means without limit.

        Yields:
            PartitionSearchResult instances.

        """

        query_string = self._make_query_from_terms(search_phrase)
        self._parsed_query = query_string
        schema = self._get_generic_schema()
        parser = QueryParser('doc', schema=schema)
        query = parser.parse(query_string)
        logger.debug('Searching partitions using `{}` query.'.format(query))
        with self.index.searcher() as searcher:
            results = searcher.search(query, limit=limit)
            for hit in results:
                yield PartitionSearchResult(
                    vid=hit['vid'], dataset_vid=hit['dataset_vid'], score=hit.score)

    def is_indexed(self, partition):
        """ Returns True if partition is already indexed. Otherwise returns False. """
        with self.index.searcher() as searcher:
            result = searcher.search(Term('vid', partition.vid))
            return bool(result)

    def _index_document(self, document, force=False):
        """ Adds parition document to the index. """
        if force:
            self._delete(vid=document['vid'])

        writer = self.index.writer()
        writer.add_document(**document)
        writer.commit()

    def _get_generic_schema(self):
        """ Returns whoosh's generic schema of the partition document. """
        schema = Schema(
            vid=ID(stored=True, unique=True),
            dataset_vid=ID(stored=True),  # dataset_vid? Convert if so.
            title=NGRAMWORDS(),
            keywords=KEYWORD,
            doc=TEXT)  # Generated document for the core of the topic search
        return schema

    def _delete(self, vid=None):
        """ Deletes given partition with given vid from index.

        Args:
            vid (str): vid of the partition document to delete.

        """
        assert vid is not None, 'vid argument can not be None.'
        writer = self.index.writer()
        writer.delete_by_term('vid', vid)
        writer.commit()


def _init_index(root_dir, schema, index_name):
    """ Creates new index or opens existing.

    Args:
        root_dir (str): root dir where to find or create index.
        schema (whoosh.fields.Schema): schema of the index to create or open.
        index_name (str): name of the index.

    Returns:
        tuple ((whoosh.index.FileIndex, str)): first element is index, second is index directory.
    """

    index_dir = os.path.join(root_dir, index_name)
    try:
        if not os.path.exists(index_dir):
            os.makedirs(index_dir)
            return create_in(index_dir, schema), index_dir
        else:
            return open_dir(index_dir), index_dir
    except Exception as e:
        logger.error("Init error: failed to open search index at: '{}': {} ".format(index_dir, e))
        raise
