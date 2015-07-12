
import os
from collections import defaultdict

from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, KEYWORD, ID, NGRAMWORDS, NGRAM  # , STORED, DATETIME
from ambry.library.search_backends.base import BaseDatasetIndex, BasePartitionIndex, BaseIdentifierIndex, BaseSearchBackend, IdentifierSearchResult, DatasetSearchResult

from whoosh import scoring
from whoosh.qparser import QueryParser

from ambry.util import get_logger
import logging

logger = get_logger(__name__, level=logging.DEBUG)


class WhooshSearchBackend(BaseSearchBackend):

    def __init__(self, library):

        # initialize backend.
        from fs.opener import fsopendir

        # each whoosh index requires root directory.
        self.root_dir = fsopendir(library._fs.search()).getsyspath('/')
        super(self.__class__, self).__init__(library)

    def _get_dataset_index(self):
        """ Returns dataset index. """
        # returns initialized dataset index
        return DatasetWhooshIndex(self)

    def _get_partition_index(self):
        """ Returns partition index. """
        # FIXME:
        pass

    def _get_identifier_index(self):
        """ Returns identifier index. """
        # FIXME:
        # return IdentifierWhooshIndex(self)
        return IdentifierWhooshIndex(backend=self)

    def _make_query_from_terms(self, terms):
        """ Create a Whoosh query from decomposed search terms. """
        from ambry.library.search import SearchTermParser

        # Moved to WhooshBackend.

        if not isinstance(terms, dict):
            stp = SearchTermParser()
            terms = stp.parse(terms)

        b_keywords = list()
        p_keywords = list()
        b_doc = list()
        p_doc = list()

        source = None

        # The top level ( title, names, keywords, doc ) will get ANDed together

        if terms.get('about', False):
            b_doc.append(terms['about'])
            p_doc.append(terms['about'])

        if terms.get('with', False):
            p_doc.append(terms['with'])

        if terms.get('in', False):
            place_vids = self._expand_place_ids(terms['in'])
            p_keywords.append(place_vids)

        if terms.get('by', False):
            p_keywords.append(terms['by'])

        if terms.get('source', False):
            source = terms['source']

        frm_to = self._from_to_as_term(terms.get('from', None), terms.get('to', None))

        if frm_to:
            p_keywords.append(frm_to)

        def or_join(terms):

            if isinstance(terms, (tuple, list)):
                if len(terms) > 1:
                    return '(' + ' OR '.join(terms) + ')'
                else:
                    return terms[0]
            else:
                return terms

        def and_join(terms):
            if len(terms) > 1:
                return ' AND '.join([or_join(t) for t in terms])
            else:
                return or_join(terms[0])

        def kwd_term(keyword, terms):
            if terms:
                return keyword + ':(' + and_join(terms) + ')'
            else:
                return None

        def per_type_terms(ttype, *terms):

            terms = [x for x in terms if bool(x)]

            if not terms:
                return ''

            return '( type:{} AND {} )'.format(ttype, ' AND '.join(terms))

        def bp_terms(*terms):
            return ' OR '.join([x for x in terms if bool(x)])

        cterms = bp_terms(
            per_type_terms('dataset', kwd_term('keywords', b_keywords), kwd_term('doc', b_doc)),
            per_type_terms('partition', kwd_term('keywords', p_keywords), kwd_term('doc', p_doc))
        )

        # If the source is specified, it qualifies the whole query, if we don't pull it out, partitions
        # that aren't from the source will get through, because the source is not applied to the partitions.
        # However, this could probalby be handled mroe simply by adding the source to
        # the partitions.
        # FIXME. This doesn't work if the orig cterms does not include a bundle term.
        # So 'counties with counties source oshpd' is OK, but 'with counties source oshpd' fails
        if source:
            cterms = ' (type:dataset AND keywords:{} ) AND {}'.format(source, cterms)

        return cterms

    def _expand_place_ids(self, terms):
        """ Lookup all of the place identifiers to get gvids

        :param terms:
        :return:
        """
        # Moved to the WhooshBackend.
        from geoid.civick import GVid
        from geoid.util import iallval
        import itertools

        place_vids = []
        first_type = None

        for score, vid, t, name in self.backend.identifier_index.search(terms):

            if not first_type:
                first_type = t

            if t != first_type:  # Ignore ones that aren't the same type as the best match
                continue

            place_vids.append(vid)

        if place_vids:
            # Add the 'all region' gvids for the higher level
            all_set = set(itertools.chain.from_iterable(iallval(GVid.parse(x)) for x in place_vids))

            place_vids += list(str(x) for x in all_set)

            return place_vids

        else:
            return terms

    def _from_to_as_term(self, frm, to):
        """ Turn from and to into the query format.
        :param frm:
        :param to:
        :return:
        """
        # Moved to WhooshBackend

        # The wackiness with the convesion to int and str, and adding ' ', is because there
        # can't be a space between the 'TO' and the brackets in the time range
        # when one end is open
        from_year = ''
        to_year = ''

        if frm:
            try:
                from_year = str(int(frm)) + ' '
            except ValueError:
                pass
        else:
            from_year = ''

        if to:
            try:
                to_year = ' ' + str(int(to))
            except ValueError:
                pass
        else:
            to_year = ''

        if bool(from_year) or bool(to_year):
            return '[{}TO{}]'.format(from_year, to_year)
        else:
            return None


class DatasetWhooshIndex(BaseDatasetIndex):
    # FIXME: This is newer version. Implement it.

    def __init__(self, backend):
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
        from shutil import rmtree
        if os.path.exists(self.index_dir):
            rmtree(self.index_dir)
        self.index = None

    def search(self, search_phrase, limit=None):
        # FIXME: convert search_phrase from string to phrase
        query_string = self.backend._make_query_from_terms(search_phrase)
        schema = self._get_generic_schema()

        parser = QueryParser('doc', schema=schema)

        query = parser.parse(query_string)

        datasets = defaultdict(DatasetSearchResult)

        with self.index.searcher() as searcher:

            results = searcher.search(query, limit=limit)

            for hit in results:

                vid = hit.get('vid')
                bvid = hit.get('bvid')
                type = hit.get('type')

                datasets[bvid].vid = bvid
                # FIXME: Can't find bundle_found usage? Do we need to distinguish bundle and dataset?
                if type == 'dataset':
                    datasets[bvid].bundle_found = True
                    datasets[bvid].b_score += hit.score
                else:
                    datasets[bvid].p_score += hit.score
                    datasets[bvid].partitions.add(vid)
        return datasets

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

    def _get_generic_schema(self):
        """ Returns whoosh's generic schema. """
        schema = Schema(
            vid=ID(stored=True, unique=True),  # Object id
            bvid=ID(stored=True),  # bundle vid
            type=ID(stored=True),
            title=NGRAMWORDS(),
            keywords=KEYWORD,  # Lists of coverage identifiers, ISO time values and GVIDs, source names, source abbrev
            doc=TEXT)  # Generated document for the core of the topic search
        return schema

    def _delete(self, dataset_vid):
        """ Deletes given dataset from index. """
        self.index.writer().delete_by_term('vid', dataset_vid)


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
        from shutil import rmtree
        if os.path.exists(self.index_dir):
            rmtree(self.index_dir)
        self.index = None

    def search(self, search_phrase, limit=None):
        # FIXME: convert search_phrase from string to phrase
        # query_string = self.backend._make_query_from_terms(search_phrase)
        schema = self._get_generic_schema()
        parser = QueryParser('name', schema=schema)
        query = parser.parse(search_phrase)  # query_string)

        class PosSizeWeighting(scoring.WeightingModel):

            def __init__(self):
                # FIXME: remove.
                pass

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
                vid = hit.get('identifier', False)
                if vid:
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

    def _delete(self, identifier):
        """ Deletes given identifier from index. """
        self.index.writer().delete_by_term('identifier', identifier)


class PartitionWhooshIndex(BasePartitionIndex):
    pass
