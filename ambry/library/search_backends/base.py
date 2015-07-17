# -*- coding: utf-8 -*-

from math import log
import re

from nltk.stem.lancaster import LancasterStemmer

from sqlalchemy.orm import object_session
from sqlalchemy.sql import text

from geoid.civick import GVid


class DatasetSearchResult(object):
    def __init__(self):
        self.vid = None
        self.b_score = 0
        self.p_score = 0
        self.bundle_found = False  # FIXME: Find usage. If there is now such, remove.
        self.partitions = set()

    @property
    def score(self):
        """Compute a total score using the log of the partition score, to reduce the include of bundles
        with a lot of partitions """
        return self.b_score + (log(self.p_score) if self.p_score else 0)


class IdentifierSearchResult(object):
    """ Search result of the identifier search index. """
    def __init__(self, score=None, vid=None, type=None, name=None):
        assert score is not None, 'score argument requires value.'
        assert vid and type and name, 'vid, type and name arguments require values.'
        self.score = score
        self.vid = vid
        self.type = type
        self.name = name


class PartitionSearchResult(object):
    """ Search result of the partition search index. """
    def __init__(self, dataset_vid=None, vid=None, score=None):
        """ Initalizes partition search result fields.

        Args:
            dataset_vid (str): vid of the partition's dataset.
            vid (str): partition vid.
            score (int): score of the search result.
        """
        assert vid is not None, 'vid can not be None.'
        assert dataset_vid is not None, 'dataset_vid can not be None.'
        assert score is not None, 'score can not be None.'
        self.dataset_vid = dataset_vid
        self.vid = vid
        self.score = score


class BaseSearchBackend(object):
    """
    Base class for full text search backends implementations.

    Subclasses must overwrite at least add_document. FIXME:
    """

    def __init__(self, library):
        self.library = library
        self.dataset_index = self._get_dataset_index()
        self.partition_index = self._get_partition_index()
        self.identifier_index = self._get_identifier_index()

    def reset(self):
        """ Resets (deletes?) all indexes. """
        self.dataset_index.reset()
        self.partition_index.reset()
        self.identifier_index.reset()

    def _get_dataset_index(self):
        """ Initializes and returns dataset index. """
        raise NotImplementedError(
            'subclasses of BaseSearchBackend must provide a _get_dataset_index() method')

    def _get_partition_index(self):
        """ Initializes and returns partition index. """
        raise NotImplementedError(
            'subclasses of BaseSearchBackend must provide a _get_partition_index() method')

    def _get_identifier_index(self):
        """ Initializes and returns identifier index. """
        raise NotImplementedError(
            'subclasses of BaseSearchBackend must provide a _get_identifier_index() method')


class BaseIndex(object):
    """
    Base class for full text search indexes implementations.
    """

    def __init__(self, backend=None):
        """ Inializes index.

        Args:
            backend (BaseBackend subclass):

        """
        assert backend is not None, 'backend argument can not be None.'
        self.backend = backend

    def index_one(self, instance):
        """ Indexes exactly one object of the Ambry system.

        Args:
            instance (any): instance to index.

        """
        doc = self._as_document(instance)
        self._index_document(doc)

    def index_many(self, instances, tick_f=None):
        """ Index all given instances.

        Args:
            instances (list): instances to index.
            tick_f (callable): FIXME:

        """
        # FIXME: Implement.
        pass

    def search(self, search_phrase):
        """ Search index by given query. Return result list.

        Args:
            search_phrase (str or unicode):

        """
        raise NotImplementedError('subclasses of BaseIndex must provide a search() method')

    def all(self):
        """ Returns all documents of the index. """
        raise NotImplementedError('subclasses of BaseIndex must provide a all() method')

    def is_indexed(self, instance):
        """ Returns True if instance is already indexed. Otherwise returns False. """
        raise NotImplementedError('subclasses of BaseIndex must provide an is_indexed() method')

    def _index_document(self, document):
        """ Adds document to the index.

        Args:
            document (dict):

        """
        raise NotImplementedError('subclasses of BaseIndex must provide an _index_document() method')

    def _as_document(self, instance):
        """ Converts ambry object to the document ready to add to index. """
        raise NotImplementedError('subclasses of BaseIndex must provide an _as_document() method')


class BaseDatasetIndex(BaseIndex):
    # FIXME: add other specs for fields.

    _schema = {
        'vid': 'id',
        'title': 'ngramwords',
        'keywords': 'keyword',
        'doc': 'text'}

    def _as_document(self, dataset):
        """ Converts dataset to document indexed by to FTS index.

        Args:
            dataset (orm.Dataset): dataset to convert.

        Returns:
            dict with structure matches to BaseDatasetIndex._schema.

        """

        # find tables.
        execute = object_session(dataset).connection().execute

        query = text("""
            SELECT t_name, c_name, c_description
            FROM columns
            JOIN tables ON c_t_vid = t_vid WHERE t_d_vid = :dataset_vid;""")

        columns = '\n'.join(
            [' '.join(list(t)) for t in execute(query, dataset_vid=str(dataset.identity.vid))])

        doc = u'\n'.join([unicode(x) for x in [dataset.config.metadata.about.title,
                                               dataset.config.metadata.about.summary,
                                               dataset.identity.id_,
                                               dataset.identity.vid,
                                               dataset.identity.source,
                                               dataset.identity.name,
                                               dataset.identity.vname,
                                               # bundle.metadata.documentation.main, # FIXME: Can't find such field in new implementation.
                                               columns]])

        # From the source, make a varity of combinations for keywords:
        # foo.bar.com -> "foo foo.bar foo.bar.com bar.com"
        parts = unicode(dataset.identity.source).split('.')
        sources = (['.'.join(g) for g in [parts[-i:] for i in range(2, len(parts) + 1)]]
                   + ['.'.join(g) for g in [parts[:i] for i in range(0, len(parts))]])

        # Re-calculate the summarization of grains, since the geoid 0.0.7 package had a bug where state level
        # summaries had the same value as state-level allvals
        def resum(g):
            try:
                return str(GVid.parse(g).summarize())
            except KeyError:
                return g

        # FIXME: old keywords contain list(dataset.config.metadata.coverage.geo). Do not know which field it is now.
        keywords = (
            list(dataset.config.metadata.about.groups) + list(dataset.config.metadata.about.tags) +
            [resum(g) for g in dataset.config.metadata.about.grain] +
            list(dataset.config.metadata.about.time) + sources)

        document = dict(
            vid=unicode(dataset.identity.vid),
            title=unicode(dataset.identity.name) + u' ' + unicode(dataset.config.metadata.about.title),
            doc=unicode(doc),
            keywords=u' '.join(unicode(x) for x in keywords)
        )

        return document


class BasePartitionIndex(BaseIndex):
    # FIXME: add other specs for fields.

    _schema = {
        'vid': 'id',
        'dataset_vid': 'id',
        'title': 'ngramwords',
        'keywords': 'keyword',
        'doc': 'text'}

    def _as_document(self, partition):
        """ Converts given partition to the document indexed by FTS backend.

        Args:
            partition (orm.Partition): partition to convert.

        Returns:
            dict with structure matches to BasePartitionIndex._schema.

        """

        schema = ' '.join(
            '{} {} {} {} {}'.format(
                c.id_,
                c.vid,
                c.name,
                c.altname,
                c.description) for c in partition.table.columns)

        values = ''

        for stat in partition.stats:
            if stat.uvalues:
                values += ' '.join(stat.uvalues) + '\n'

        # Re-calculate the summarization of grains, since the geoid 0.0.7 package had a bug where state level
        # summaries had the same value as state-level allvals
        def resum(g):
            try:
                return str(GVid.parse(g).summarize())
            except KeyError:
                return g

        keywords = (
            ' '.join(partition.space_coverage) + ' ' +
            ' '.join([resum(g) for g in partition.grain_coverage]) + ' ' +
            ' '.join(str(x) for x in partition.time_coverage)
        )

        doc_field = unicode(
            values + ' ' + schema + ' '
            u' '.join([
                unicode(partition.identity.vid),
                unicode(partition.identity.id_),
                unicode(partition.identity.name),
                unicode(partition.identity.vname)]))

        document = dict(
            vid=unicode(partition.identity.vid),
            dataset_vid=unicode(partition.identity.as_dataset().vid),
            title=unicode(partition.table.description),
            keywords=unicode(keywords),
            doc=doc_field)

        return document


class BaseIdentifierIndex(BaseIndex):
    # FIXME: add other specs for fields.

    _schema = {
        'identifier': 'id',
        'type': 'id',
        'name': 'ngram',
    }

    def _as_document(self, identifier):
        """ Converts given identifier to the document indexed by FTS backend.

        Args:
            identifier (dict): identifier to convert. Dict contains at
                least 'identifier', 'type' and 'name' keys.

        Returns:
            dict with structure matches to BaseIdentifierIndex._schema.

        """
        return {
            'identifier': unicode(identifier['identifier']),
            'type': unicode(identifier['type']),
            'name': unicode(identifier['name'])
        }


class SearchTermParser(object):
    """Decompose a search term in to conceptual parts, according to the Ambry search model."""
    TERM = 0
    QUOTEDTERM = 1
    LOGIC = 2
    MARKER = 3
    YEAR = 4

    types = {
        TERM: 'TERM',
        QUOTEDTERM: 'TERM',
        LOGIC: 'LOGIC',
        MARKER: 'MARKER',
        YEAR: 'YEAR'
    }

    marker_terms = {
        'about': 'about',
        'in': ('coverage', 'grain'),
        'by': 'grain',
        'with': 'with',
        'from': ('year', 'source'),
        'to': 'year',
        'source': 'source'}

    by_terms = 'state county zip zcta tract block blockgroup place city cbsa msa'.split()

    @staticmethod
    def s_quotedterm(scanner, token):
        return SearchTermParser.QUOTEDTERM, token.lower().strip()

    @staticmethod
    def s_term(scanner, token):
        return SearchTermParser.TERM, token.lower().strip()

    @staticmethod
    def s_domainname(scanner, token):
        return SearchTermParser.TERM, token.lower().strip()

    @staticmethod
    def s_markerterm(scanner, token):
        return SearchTermParser.MARKER, token.lower().strip()

    @staticmethod
    def s_year(scanner, token):
        return SearchTermParser.YEAR, int(token.lower().strip())

    def __init__(self):
        mt = '|'.join(
            [r'\b' + x.upper() + r'\b' for x in self.marker_terms.keys()])

        self.scanner = re.Scanner([
            (r'\s+', None),
            (r"['\"](.*?)['\"]", self.s_quotedterm),
            (mt.lower(), self.s_markerterm),
            (mt, self.s_markerterm),
            (r'19[789]\d|20[012]\d', self.s_year),  # From 1970 to 2029
            (r'(?:[\w\-\.?])+', self.s_domainname),
            (r'.+?\b', self.s_term),
        ])

        self.stemmer = LancasterStemmer()

        self.by_terms = [self.stem(x) for x in self.by_terms]

    def scan(self, s):
        s = ' '.join(s.splitlines())  # make a single line
        # Returns one item per line, but we only have one line
        return self.scanner.scan(s)[0]

    def stem(self, w):
        return self.stemmer.stem(w)

    def parse(self, s):

        toks = self.scan(s)

        # Assume the first term is ABOUT, if it is not marked with a marker.
        if toks[0][0] == self.TERM or toks[0][0] == self.QUOTEDTERM:
            toks = [(self.MARKER, 'about')] + toks

        # Group the terms by their marker.
        # last_marker = None
        bymarker = []
        for t in toks:
            if t[0] == self.MARKER:
                bymarker.append((t[1], []))
            else:
                bymarker[-1][1].append(t)

        # Convert some of the markers based on their contents
        comps = []
        for t in bymarker:
            t = list(t)
            if t[0] == 'in' and len(t[1]) == 1 and isinstance(t[1][0][1], basestring) and self.stem(
                    t[1][0][1]) in self.by_terms:
                t[0] = 'by'

            # If the from term isn't an integer, then it is really a source.
            if t[0] == 'from' and len(t[1]) == 1 and t[1][0][0] != self.YEAR:
                t[0] = 'source'

            comps.append(t)

        # Join all of the terms into single marker groups
        groups = {marker: [] for marker, _ in comps}

        for marker, terms in comps:
            groups[marker] += [term for marker, term in terms]

        for marker, terms in groups.items():

            if len(terms) > 1:
                if marker in 'in':
                    groups[marker] = ' '.join(terms)
                else:
                    groups[marker] = '(' + ' OR '.join(terms) + ')'
            elif len(terms) == 1:
                groups[marker] = terms[0]
            else:
                pass

        return groups
