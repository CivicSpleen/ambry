
import os

from whoosh.fields import Schema, TEXT, KEYWORD, ID, NGRAMWORDS, NGRAM  # , STORED, DATETIME
from ambry.library.search_backends.base import DatasetIndex, IdentifierIndex, BaseSearchBackend


class DatasetWhooshIndex(DatasetIndex):

    def __init__(self, backend):
        self.index_dir = os.path.join(backend.root_dir, 'datasets')

    def add_document(self, document):
        pass

    def search(self, field, query):
        pass

    def get_generic_schema(self):
        """ Returns whoosh's generic schema. """
        schema = Schema(
            vid=ID(stored=True, unique=True),  # Object id
            bvid=ID(stored=True),  # bundle vid
            type=ID(stored=True),
            title=NGRAMWORDS(),
            keywords=KEYWORD,  # Lists of coverage identifiers, ISO time values and GVIDs, source names, source abbrev
            doc=TEXT)  # Generated document for the core of the topic search
        return schema


class IdentifierWhooshIndex(IdentifierIndex):

    def __init__(self, backend):
        self.index_dir = os.path.join(backend.root_dir, 'identifiers')

    def add_document(self, document):
        pass

    def search(self, field, query):
        pass

    def get_generic_schema(self):
        schema = Schema(
            identifier=ID(stored=True),  # Partition versioned id
            type=ID(stored=True),
            name=NGRAM(phrase=True, stored=True, minsize=2, maxsize=8))
        return schema


class WhooshSearchBackend(BaseSearchBackend):

    def __init__(self, library):

        # initialize backend.
        from fs.opener import fsopendir

        self.library = library

        self.root_dir = fsopendir(self.library._fs.search()).getsyspath('/')

        # initialize indexes
        self.dataset_index = DatasetWhooshIndex(self)
        self.identifier_index = IdentifierWhooshIndex(self)

    def reset(self):
        self.dataset_index.reset()
        self.identifier_index.reset()
