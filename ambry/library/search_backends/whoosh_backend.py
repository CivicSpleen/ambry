# -*- coding: utf-8 -*-

from collections import defaultdict
import itertools
import logging
from shutil import rmtree
import os

from geoid.civick import GVid
from geoid.util import iallval

from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, KEYWORD, ID, NGRAMWORDS, NGRAM
from whoosh import scoring
from whoosh.qparser import QueryParser

from fs.opener import fsopendir

from ambry.library.search_backends.base import BaseDatasetIndex, BasePartitionIndex,\
    BaseIdentifierIndex, BaseSearchBackend, IdentifierSearchResult,\
    DatasetSearchResult, PartitionSearchResult

from ambry.library.search_backends.base import SearchTermParser
from ambry.util import get_logger

logger = get_logger(__name__, level=logging.DEBUG)


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

    def _or_join(self, terms):

        if isinstance(terms, (tuple, list)):
            if len(terms) > 1:
                return '(' + ' OR '.join(terms) + ')'
            else:
                return terms[0]
        else:
            return terms

    def _and_join(self, terms):
        if len(terms) > 1:
            return ' AND '.join([self._or_join(t) for t in terms])
        else:
            return self._or_join(terms[0])

    def _kwd_term(self, keyword, terms):
        if terms:
            return keyword + ':(' + self._and_join(terms) + ')'
        else:
            return None


class DatasetWhooshIndex(BaseDatasetIndex):

    def __init__(self, backend=None):
        assert backend is not None, 'backend can not be None'
        super(self.__class__, self).__init__(backend)
        self.index_dir = os.path.join(self.backend.root_dir, 'datasets')
        self.all_datasets = []  # FIXME: Implement.
        try:
            schema = self._get_generic_schema()
            if not os.path.exists(self.index_dir):
                os.makedirs(self.index_dir)
                self.index = create_in(self.index_dir, schema)
            else:
                self.index = open_dir(self.index_dir)
        except Exception as e:
            logger.error("Failed to open search index at: '{}': {} ".format(dir, e))
            raise

    def reset(self):
        """ Resets index by removing index directory. """
        if os.path.exists(self.index_dir):
            rmtree(self.index_dir)
        self.index = None

    def search(self, search_phrase, limit=None):
        """ Finds datasets by search phrase.

        Args:
            search_phrase (str or unicode):
            limit (int, optional): how many results to return. None means without limit.

        Returns:
            list of DatasetSearchResult instances.

        """
        query_string = self._make_query_from_terms(search_phrase)
        schema = self._get_generic_schema()

        parser = QueryParser('doc', schema=schema)

        query = parser.parse(query_string)

        datasets = defaultdict(DatasetSearchResult)

        # collect all datasets
        with self.index.searcher() as searcher:
            results = searcher.search(query, limit=limit)
            for hit in results:
                vid = hit['vid']
                datasets[vid].vid = hit['vid']
                datasets[vid].b_score += hit.score

        # extend datasets with partitions
        for partition in self.backend.partition_index.search(search_phrase):
            datasets[partition.dataset_vid].p_score += partition.score
            datasets[partition.dataset_vid].partitions.add(partition.vid)
        return datasets.values()

    def _index_document(self, document, force=False):
        """ Adds document to the index. """
        # FIXME:
        #if document['dvid'] in self.all_datasets and not force:
        #    # dataset already indexed.
        #    return

        writer = self.index.writer()
        writer.add_document(**document)
        writer.commit()

        #if force:
        #    self.dataset_writer.delete_by_term( 'vid', unicode( bundle.identity.vid))
        # self.all_datasets.add(bundle.identity.vid)

    def _make_query_from_terms(self, terms):
        """ Creates a Whoosh query for dataset from decomposed search terms.

        Args:
            terms (dict or unicode or string):

        Returns:
            str with Whoosh query.
        """

        if not isinstance(terms, dict):
            stp = SearchTermParser()
            terms = stp.parse(terms)

        keywords = list()
        doc = list()

        source = None

        # The top level ( title, names, keywords, doc ) will get ANDed together

        if 'about' in terms:
            doc.append(terms['about'])

        if 'source' in terms:
            source = terms['source']

        cterms = None
        # FIXME: need more tests.

        if doc:
            cterms = self.backend._and_join(doc)

        if keywords:
            keywords_terms = 'keywords:(' + self.backend._and_join(keywords) + ')'
            if cterms:
                cterms = self.backend._and_join(cterms, keywords_terms)
            else:
                cterms = keywords_terms

        if source:
            # FIXME: test that.
            source_terms = 'keywords:{} AND '.format(source, cterms)
            if cterms:
                cterms = self.backend._and_join(cterms, source_terms)
            else:
                cterms = source_terms

        return cterms

    def _get_generic_schema(self):
        """ Returns whoosh's generic schema of the dataset. """
        schema = Schema(
            vid=ID(stored=True, unique=True),  # Object id
            bvid=ID(stored=True),  # bundle vid
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


class IdentifierWhooshIndex(BaseIdentifierIndex):

    def __init__(self, backend=None):
        super(self.__class__, self).__init__(backend=backend)
        self.index_dir = os.path.join(self.backend.root_dir, 'identifiers')
        self.all_identifiers = []  # FIXME: Implement.
        try:
            schema = self._get_generic_schema()
            if not os.path.exists(self.index_dir):
                os.makedirs(self.index_dir)
                self.index = create_in(self.index_dir, schema)
            else:
                self.index = open_dir(self.index_dir)
        except Exception as e:
            logger.error('Failed to open search index at: {}: {}'.format(dir, e))
            raise

    def reset(self):
        if os.path.exists(self.index_dir):
            rmtree(self.index_dir)
        self.index = None

    def search(self, search_phrase, limit=None):
        schema = self._get_generic_schema()
        parser = QueryParser('name', schema=schema)
        query = parser.parse(search_phrase)  # query_string)

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


class PartitionWhooshIndex(BasePartitionIndex):

    def __init__(self, backend=None):
        super(self.__class__, self).__init__(backend=backend)

        self.index_dir = os.path.join(self.backend.root_dir, 'partitions')
        self.all_partitions = []  # FIXME: Implement.
        try:
            schema = self._get_generic_schema()
            if not os.path.exists(self.index_dir):
                os.makedirs(self.index_dir)
                self.index = create_in(self.index_dir, schema)
            else:
                self.index = open_dir(self.index_dir)
        except Exception as e:
            logger.error('Failed to open search index at: {}: {}'.format(dir, e))
            raise

    def _from_to_as_term(self, frm, to):
        """ Turns from and to into the query format.

        Args:
            frm (str): from year
            to (str): to year

        Returns:
            Whoosh query str with years range.

        """

        # The wackiness with the conversion to int and str, and adding ' ', is because there
        # can't be a space between the 'TO' and the brackets in the time range
        # when one end is open
        from_year = ''
        to_year = ''

        def year_or_empty(prefix, year, suffix):
            try:
                return prefix + str(int(year)) + suffix
            except (ValueError, TypeError):
                return ''

        if frm:
            from_year = year_or_empty('', frm, ' ')

        if to:
            to_year = year_or_empty(' ', to, '')

        if bool(from_year) or bool(to_year):
            return '[{}TO{}]'.format(from_year, to_year)
        else:
            return None

    def reset(self):
        if os.path.exists(self.index_dir):
            rmtree(self.index_dir)
        self.index = None

    def search(self, search_phrase, limit=None):
        """ Finds partitions by search phrase.

        Args:
            search_phrase (str or unicode):
            limit (int, optional): how many results to generate. None means without limit.

        Generates:
            PartitionSearchResult instances.
        """

        query_string = self._make_query_from_terms(search_phrase)
        schema = self._get_generic_schema()
        parser = QueryParser('doc', schema=schema)
        query = parser.parse(query_string)
        with self.index.searcher() as searcher:
            results = searcher.search(query, limit=limit)
            for hit in results:
                yield PartitionSearchResult(
                    vid=hit['vid'], dataset_vid=hit['bvid'], score=hit.score)

    def _index_document(self, document, force=False):
        """ Adds parition document to the index. """
        # FIXME:
        # if p.identity.vid in self.all_partitions and not force:
        #    return

        writer = self.index.writer()
        writer.add_document(**document)
        writer.commit()

        # FIXME:
        # self.all_partitions.add(p.identity.vid)

    def _expand_place_ids(self, terms):
        """ Lookups all of the place identifiers to get gvids

        Args:
            terms (FIXME:): terms to lookup

        Returns:
            FIXME:
        """

        place_vids = []
        first_type = None

        for result in self.backend.identifier_index.search(terms):

            if not first_type:
                first_type = result.type

            if result.type != first_type:
                # Ignore ones that aren't the same type as the best match
                continue

            place_vids.append(result.vid)

        if place_vids:
            # Add the 'all region' gvids for the higher level
            all_set = set(itertools.chain.from_iterable(iallval(GVid.parse(x)) for x in place_vids))
            place_vids += list(str(x) for x in all_set)
            return place_vids
        else:
            return terms

    def _make_query_from_terms(self, terms):
        """ Returns a Whoosh query for partition created from decomposed search terms.

        Args:
            terms (dict or str):

        Returns:
            str containing Whoosh query.

        """

        if not isinstance(terms, dict):
            stp = SearchTermParser()
            terms = stp.parse(terms)

        keywords = list()
        doc = list()

        # The top level ( title, names, keywords, doc ) will get ANDed together

        if 'about' in terms:
            doc.append(terms['about'])

        if 'with' in terms:
            doc.append(terms['with'])

        if 'in' in terms:
            place_vids = self._expand_place_ids(terms['in'])
            keywords.append(place_vids)

        if 'by' in terms:
            keywords.append(terms['by'])

        frm_to = self._from_to_as_term(terms.get('from', None), terms.get('to', None))

        if frm_to:
            keywords.append(frm_to)

        cterms = None
        if doc:
            cterms = self.backend._or_join(doc)

        if keywords:
            if cterms:
                cterms = '{} AND {}'.format(cterms, self.backend._kwd_term('keywords', keywords))
            else:
                cterms = self.backend._kwd_term('keywords', keywords)

        return cterms

    def _get_generic_schema(self):
        """ Returns whoosh's generic schema. """
        schema = Schema(
            vid=ID(stored=True, unique=True),
            bvid=ID(stored=True),  # dataset_vid? Convert if so.
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
