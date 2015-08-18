# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import itertools
import logging
from math import log
from pprint import pformat
import re

from six import iterkeys, iteritems

from nltk.stem.lancaster import LancasterStemmer

from sqlalchemy.orm import object_session
from sqlalchemy.sql import text

from geoid.civick import GVid
from geoid.util import iallval

from ambry.util import get_logger

from ambry.orm.dataset import Dataset

logger = get_logger(__name__, level=logging.INFO, propagate=False)


class DatasetSearchResult(object):
    def __init__(self):
        self.vid = None
        self.b_score = 0
        self.p_score = 0
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


class IndexField(object):
    """ Base class for all fields in the schema. """

    def __init__(self, name):
        self.name = name


class Id(IndexField):
    """ Indexes the entire value of the field as one token.

    Note: This is useful for data you don’t want to tokenize, such as the path of a file or vid.

    Examples:
        Whoosh Id - http://pythonhosted.org/Whoosh/api/fields.html#whoosh.fields.ID
        Sqlite ?
        Postgresql ?
    """
    pass


class NGram(IndexField):
    """ Field to chop the words into N-grams.

    Note: Is helpfull for autocomplete feature. For example, the set of ngrams in the
        string "cat" is " c", " ca", "cat", and "at ".

    Examples:
        whoosh - NGRAMWORDS, http://pythonhosted.org/Whoosh/api/fields.html#whoosh.fields.NGRAMWORDS
        sqlite - ?
        postgresql - pg_trgm, http://www.postgresql.org/docs/current/static/pgtrgm.html

    """
    pass


class Keyword(IndexField):
    """ Field for space- or comma-separated keywords.

    Note:
        This type is indexed and searchable (and optionally stored). Used to search for exact match of any
        keyword FIXME: is it really exact match?.

    Examples:
        Whoosh - KEYWORD, http://pythonhosted.org/Whoosh/api/fields.html#whoosh.fields.KEYWORD
        SQLite - FIXME:
        PostgreSQL - FIXME:
    """


class Text(IndexField):
    """ Field for text data (for example, the body text of an article). Allows phrase searching.

    Note:
        This field type is always scorable.

    Examples:
        Whoosh - TEXT, http://pythonhosted.org/Whoosh/api/fields.html#whoosh.fields.TEXT
        SQLite - FIXME:
        PostgreSQL - FIXME:
    """
    pass


class BaseSearchBackend(object):
    """
    Base class for full text search backends implementations.
    """

    def __init__(self, library):
        self.library = library
        self.dataset_index = self._get_dataset_index()
        self.partition_index = self._get_partition_index()
        self.identifier_index = self._get_identifier_index()

    def reset(self):
        """ Resets (deletes) all indexes. """
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

    def _or_join(self, terms):
        """ Joins terms using OR operator.

        Args:
            terms (list): terms to join

        Examples:
            self._or_join(['term1', 'term2']) -> 'term1 OR term2'

        Returns:
            str
        """

        if isinstance(terms, (tuple, list)):
            if len(terms) > 1:
                return '(' + ' OR '.join(terms) + ')'
            else:
                return terms[0]
        else:
            return terms

    def _join_keywords(self, keywords):
        if isinstance(keywords, (list, tuple)):
            return 'keywords:(' + self._and_join(keywords) + ')'
        return 'keywords:{}'.format(keywords)

    def _and_join(self, terms):
        """ Joins terms using AND operator.

        Args:
            terms (list): terms to join

        Examples:
            self._and_join(['term1']) -> 'term1'
            self._and_join(['term1', 'term2']) -> 'term1 AND term2'
            self._and_join(['term1', 'term2', 'term3']) -> 'term1 AND term2 AND term3'

        Returns:
            str
        """
        if len(terms) > 1:
            return ' AND '.join([self._or_join(t) for t in terms])
        else:
            return self._or_join(terms[0])

    def _field_term(self, field, terms):
        """ AND join of the terms of the field.

        Args:
            field (str): name of the field
            terms (list): list of the terms

        Examples:
            self._field_term('keywords', ['term1', 'term2']) -> 'keywords:(term1 AND term2)'

        Returns:
            str
        """
        if terms:
            return field + ':(' + self._and_join(terms) + ')'
        else:
            return None


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
        self._parsed_query = None

    def index_one(self, instance, force=False):
        """ Indexes exactly one object of the Ambry system.

        Args:
            instance (any): instance to index.
            force (boolean): if True replace document in the index.

        Returns:
            boolean: True if document added to index, False if document already exists in the index.
        """
        if not self.is_indexed(instance) and not force:
            doc = self._as_document(instance)
            self._index_document(doc, force=force)
            logger.debug('{} indexed as\n {}'.format(instance.__class__, pformat(doc)))
            return True
        logger.debug('{} already indexed.'.format(instance.__class__))
        return False

    def index_many(self, instances, tick_f=None):
        """ Index all given instances.

        Args:
            instances (list): instances to index.
            tick_f (callable): callable of one argument. Gets amount of indexed documents.

        """
        for instance in instances:
            self.index_one(instance)

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

    def get_parsed_query(self):
        """ Returns string with last query parsed. """
        assert self._parsed_query is not None, 'Probably your forget to run search.'
        return self._parsed_query

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
    _schema = [
        Id('vid'),
        NGram('title'),
        Keyword('keywords'),
        Text('doc')]

    def _as_document(self, dataset):
        """ Converts dataset to document indexed by to FTS index.

        Args:
            dataset (orm.Dataset): dataset to convert.

        Returns:
            dict with structure matches to BaseDatasetIndex._schema.

        """

        # find tables.

        assert isinstance(dataset, Dataset)

        execute = object_session(dataset).connection().execute

        query = text("""
            SELECT t_name, c_name, c_description
            FROM columns
            JOIN tables ON c_t_vid = t_vid WHERE t_d_vid = :dataset_vid;""")

        columns = '\n'.join(
            [' '.join(list(str(e) for e in t)) for t in execute(query, dataset_vid=str(dataset.identity.vid))])

        doc = '\n'.join([unicode(x) for x in [dataset.config.metadata.about.title,
                                              dataset.config.metadata.about.summary,
                                              dataset.identity.id_,
                                              dataset.identity.vid,
                                              dataset.identity.source,
                                              dataset.identity.name,
                                              dataset.identity.vname,
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
            except (KeyError, ValueError):
                return g

        try:
            about_time = list(dataset.config.metadata.about.time)
        except TypeError:
            about_time = [dataset.config.metadata.about.time]

        try:
            about_grain = list(dataset.config.metadata.about.grain)
        except TypeError:
            about_grain = [dataset.config.metadata.about.grain]

        keywords = (
            list(dataset.config.metadata.about.groups) +
            list(dataset.config.metadata.about.tags) +
            about_time +
            [resum(g) for g in about_grain] +
            sources)

        document = dict(
            vid=unicode(dataset.identity.vid),
            title=unicode(dataset.identity.name) + ' ' + unicode(dataset.config.metadata.about.title),
            doc=unicode(doc),
            keywords=' '.join(unicode(x) for x in keywords)
        )

        return document

    def _expand_terms(self, terms):
        """ Expands terms to the appropriate fields.

        Args:
            terms (dict or str):

        Returns:
            dict: keys are field names, values are query strings
        """

        ret = {
            'keywords': list(),
            'doc': list()}

        if not isinstance(terms, dict):
            stp = SearchTermParser()
            terms = stp.parse(terms, or_join=self.backend._or_join)

        if 'about' in terms:
            ret['doc'].append(terms['about'])

        if 'source' in terms:
            ret['keywords'].append(terms['source'])
        return ret


class BasePartitionIndex(BaseIndex):

    _schema = [
        Id('vid'),
        Id('dataset_vid'),
        NGram('title'),
        Keyword('keywords'),
        Text('doc')]

    def _as_document(self, partition):
        """ Converts given partition to the document indexed by FTS backend.

        Args:
            partition (orm.Partition): partition to convert.

        Returns:
            dict with structure matches to BasePartitionIndex._schema.

        """

        schema = ' '.join(
            '{} {} {} {} {}'.format(
                c.id,
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
            values + ' ' + schema + ' ' +
            ' '.join([
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

    def _expand_terms(self, terms):
        """ Expands partition terms to the appropriate fields.

        Args:
            terms (dict or str):

        Returns:
            dict: keys are field names, values are query strings
        """
        ret = {
            'keywords': list(),
            'doc': list(),
            'from': None,
            'to': None}

        if not isinstance(terms, dict):
            stp = SearchTermParser()
            terms = stp.parse(terms)

        # The top level (title, names, keywords, doc) will get ANDed together

        if 'about' in terms:
            ret['doc'].append(terms['about'])

        if 'with' in terms:
            ret['doc'].append(terms['with'])

        if 'in' in terms:
            place_vids = self._expand_place_ids(terms['in'])
            ret['keywords'].append(place_vids)

        if 'by' in terms:
            ret['keywords'].append(terms['by'])
        ret['from'] = terms.get('from', None)
        ret['to'] = terms.get('to', None)
        return ret

    def _expand_place_ids(self, terms):
        """ Lookups all of the place identifiers to get gvids

        Args:
            terms (str or unicode): terms to lookup

        Returns:
            str or list: given terms if no identifiers found, otherwise list of identifiers.
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


class BaseIdentifierIndex(BaseIndex):

    _schema = [
        Id('identifier'),  # Partition versioned id (partition vid)
        Id('type'),  # Type. FIXME: What is type? Add examples.
        NGram('name'),
    ]

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
            [r'\b' + x.upper() + r'\b' for x in iterkeys(self.marker_terms)])

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

    def parse(self, s, or_join=None):
        """ FIXME: """
        if not or_join:
            or_join = lambda x: '(' + ' OR '.join(x) + ')'

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

        for marker, terms in iteritems(groups):

            if len(terms) > 1:
                if marker in 'in':
                    groups[marker] = ' '.join(terms)
                else:
                    groups[marker] = or_join(terms)
            elif len(terms) == 1:
                groups[marker] = terms[0]
            else:
                pass

        return groups
