# -*- coding: utf-8 -*-
import logging
from ambry.util import get_logger

from ambry.library.search_backends import WhooshSearchBackend, SQLiteSearchBackend, PostgreSQLSearchBackend

logger = get_logger(__name__, level=logging.INFO, propagate=False)

# All backends.
BACKENDS = {
    'whoosh': WhooshSearchBackend,
    'sqlite': SQLiteSearchBackend,
    'postgresql+psycopg2': PostgreSQLSearchBackend
}


class Search(object):

    def __init__(self, library, backend=None):

        if not backend:
            try:
                backend_name = library.config.services.search
                if not backend_name:
                    # config contains search key without value.
                    raise KeyError

                if backend_name not in BACKENDS:
                    raise Exception(
                        'Missing backend: search section of the config contains unknown backend {}'
                        .format(backend_name))
            except KeyError:
                # config does not contain search backend. Try to find backend by database driver.
                backend_name = library.database.driver

            if backend_name not in BACKENDS:
                logger.debug(
                    'Missing backend: ambry does not have {} search backend. Using whoosh search.'
                    .format(backend_name))
                backend_name = 'whoosh'

            backend = BACKENDS[backend_name](library)

        self.backend = backend
        self.library = library

    def reset(self):
        self.backend.reset()

    def index_dataset(self, dataset, force=False):
        """ Adds given dataset to the index. """
        self.backend.dataset_index.index_one(dataset, force=force)

    def index_partition(self, partition, force=False):
        """ Adds given partition to the index. """
        self.backend.partition_index.index_one(partition, force=force)

    def index_bundle(self, bundle, force=False):
        """
        Indexes a bundle/dataset and all of its partitions
        :param bundle: A bundle or dataset object
        :param force: If true, index the document even if it already exists
        :return:
        """
        from ambry.orm.dataset import Dataset

        dataset = bundle if isinstance(bundle, Dataset) else bundle.dataset

        self.index_dataset(dataset, force)

        for partition in dataset.partitions:
            self.index_partition(partition, force)

    def index_library_datasets(self, tick_f=None):
        """ Indexes all datasets of the library.

        Args:
            tick_f (callable, optional): callable of one argument. Gets string with index state.

        """

        dataset_n = 0
        partition_n = 0

        def tick(d, p):
            if tick_f:
                tick_f('datasets: {} partitions: {}'.format(d, p))

        for dataset in self.library.datasets:
            if self.backend.dataset_index.index_one(dataset):
                # dataset added to index
                dataset_n += 1
                tick(dataset_n, partition_n)
                for partition in dataset.partitions:
                    self.backend.partition_index.index_one(partition)
                    partition_n += 1
                    tick(dataset_n, partition_n)
            else:
                # dataset already indexed
                pass

    def search_datasets(self, search_phrase, limit=None):
        """ Search for datasets. """
        return self.backend.dataset_index.search(search_phrase, limit=limit)

    def search(self, search_phrase, limit=None):
        """Search for datasets, and expand to database records"""
        from ambry.identity import ObjectNumber

        results = self.search_datasets(search_phrase, limit)

        for r in results:
            vid = r.vid or ObjectNumber.parse(next(iter(r.partitions))).as_dataset

            r.vid = vid

            r.bundle = self.library.bundle(r.vid)

            yield r

    def list_documents(self, limit=None):
        """
        Return a list of the documents
        :param limit:
        :return:
        """
        from itertools import chain

        return chain(self.backend.dataset_index.list_documents(limit=limit),
                     self.backend.partition_index.list_documents(limit=limit),
                     self.backend.identifier_index.list_documents(limit=limit))

    def get_parsed_query(self):
        """ Returns string with last query parsed. Assuming called after search_datasets."""
        return '{} OR {}'.format(
            self.dataset_index.get_parsed_query(),
            self.partition_index.get_parsed_query())

    def index_identifiers(self, identifiers):
        """ Adds given identifiers to the index. """
        self.backend.identifier_index.index_many(identifiers)

    def search_identifiers(self, search_phrase, limit=10):
        return self.backend.identifier_index.search(search_phrase, limit=limit)
