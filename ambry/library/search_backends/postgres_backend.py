
import os

from ambry.library.search_backends.base import DatasetIndex, IdentifierIndex, BaseSearchBackend


class DatasetPostgresIndex(DatasetIndex):

    def __init__(self, backend):
        # TODO: ensure index.
        query = '''\
            CREATE TABLE datasets (
            vid  char(5),
            bvid varchar(40) NOT NULL,
            type  char(5),
            title char(5),
            keywords char(5),
            doc TSVECTOR;
        );'''

    def add_document(self, document):
        query = '''\
            INSERT INTO datasets (vid, bvid, type, title, keywords, doc)
            VALUES ({vid}, {bvid}, {type}, {title}, {keywords}, {doc});
        '''

    def search(self, field, query):
        query = '''\
            SELECT * FROM datasets
            WHERE doc @@ to_tsquery('english', '{query}');
        '''
        pass

    def get_generic_schema(self):
        pass


class IdentifierPostgresIndex(IdentifierIndex):

    def __init__(self, backend):
        # FIXME: ensure index.
        pass

    def add_document(self, document):
        pass

    def search(self, field, query):
        pass

    def get_generic_schema(self):
        pass


class PostgresSearchBackend(BaseSearchBackend):

    def __init__(self, library):

        self.library = library

        # ensure database exists.

        # initialize indexes
        self.dataset_index = DatasetPostgresIndex(self)
        self.identifier_index = IdentifierPostgresIndex(self)

    def reset(self):
        self.dataset_index.reset()
        self.identifier_index.reset()
