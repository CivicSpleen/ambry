""""""

from math import log

from sqlalchemy.orm import object_session

from geoid.civick import GVid


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


class IdentifierSearchResult(object):
    def __init__(self):
        # FIXME:
        pass


class PartitionSearchResult(object):
    def __init__(self):
        # FIXME:
        pass


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
        """ Search index by given query. Return result list. """
        raise NotImplementedError(
            'subclasses of BaseSearchBackend must provide a _get_dataset_index() method')

    def _get_partition_index(self):
        """ Search index by given query. Return result list. """
        raise NotImplementedError(
            'subclasses of BaseSearchBackend must provide a _get_dataset_index() method')

    def _get_identifier_index(self):
        """ Search index by given query. Return result list. """
        raise NotImplementedError(
            'subclasses of BaseSearchBackend must provide a _get_dataset_index() method')


class BaseIndex(object):
    """
    Base class for full text search indexes implementations.
    """

    def __init__(self, backend):
        self.backend = backend

    def index_one(self, instance):
        """ Index Ambry system object. """
        doc = self._as_document(instance)
        self._index_document(doc)

    def index_many(self, instances, tick_f=None):
        """ Index given instances. """
        pass

    def search(self, query):
        """ Search index by given query. Return result list. """
        raise NotImplementedError('subclasses of BaseCache must provide a search() method')

    def all(self):
        """ Returns all documents of the index. """
        raise NotImplementedError('subclasses of BaseIndex must provide a all() method')

    def _index_document(self, document):
        """ Add document to the index. """
        raise NotImplementedError('subclasses of BaseIndex must provide a _index_document() method')

    def _as_document(self, instance):
        """ Converts ambry object to the document ready to add to index. """
        raise NotImplementedError('subclasses of BaseIndex must provide a _as_document() method')


class BaseDatasetIndex(BaseIndex):
    # FIXME: add other specs for fields.

    _schema = {
        'vid': 'id',
        'bvid': 'id',
        'type': 'id',
        'title': 'ngramwords',
        'keywords': 'keyword',
        'doc': 'text'}

    def _as_document(self, dataset):
        """ Converts dataset to doc indexed by to FTS index. """

        # find tables.
        execute = object_session(dataset).execute

        # FIXME: use query args instead string formatting.
        query = """SELECT t_name, c_name, c_description FROM columns
                JOIN tables ON c_t_vid = t_vid WHERE t_d_vid = '{}' """.format(str(dataset.identity.vid))

        columns = '\n'.join([' '.join(list(t)) for t in execute(query)])

        doc = u'\n'.join([unicode(x) for x in [dataset.config.metadata.about.title,
                                               dataset.config.metadata.about.summary,
                                               dataset.identity.id_,
                                               dataset.identity.vid,
                                               dataset.identity.source,
                                               dataset.identity.name,
                                               dataset.identity.vname,
                                               dataset.config.metadata.about.summary,  # FIXME: Is it valid replacement for documentation?
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
            type=u'dataset',
            vid=unicode(dataset.identity.vid),
            bvid=unicode(dataset.identity.vid),
            title=unicode(dataset.identity.name) + u' ' + unicode(dataset.config.metadata.about.title),
            doc=unicode(doc),
            keywords=u' '.join(unicode(x) for x in keywords)
        )

        return document


class BasePartitionIndex(BaseIndex):
    # FIXME: add other specs for fields.

    _schema = {
        'vid': 'id',
        'bvid': 'id',
        'type': 'id',
        'title': 'ngramwords',
        'keywords': 'keyword',
        'doc': 'text'}


class BaseIdentifierIndex(BaseIndex):
    # FIXME: add other specs for fields.

    _schema = {
        'identifier': 'id',
        'type': 'id',
        'name': 'ngram',
    }
