""""""

from math import log


class DatasetSearchResult(object):
    def __init__(self):
        self.vid = None
        self.b_score = 0
        self.p_score = 0
        self.bundle_found = False
        self.partitions = set()

    @property
    def score(self):
        """Compute a total score using the log of the partition score, to reduce the include of bundles
        with a lot of partitions """
        return self.b_score + (log(self.p_score) if self.p_score else 0)


class BaseSearchBackend(object):
    """
    Base class for full text search backends implementations.

    Subclasses must overwrite at least add_document. FIXME:
    """


class Index(object):

    """
    Base class for full text search indexes implementations.
    """

    def __init__(self, backend):
        self.backend = backend

    def add_document(self, document):
        pass

    def search(self, field, query):
        pass

    def commit(self):
        """ Commits all added documents. """
        pass

    def all(self):
        """ Returns all documents of the index. """
        pass


class DatasetIndex(Index):
    # FIXME: add other specs for fields.

    _schema = {
        'vid': 'id',
        'bvid': 'id',
        'type': 'id',
        'title': 'ngramwords',
        'keywords': 'keyword',
        'doc': 'text'}


class IdentifierIndex(Index):
    # FIXME: add other specs for fields.

    _schema = {
        'identifier': 'id',
        'type': 'id',
        'name': 'ngram',
    }
