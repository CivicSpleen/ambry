# -*- coding: utf-8 -*-

import logging

from ambry.library.search_backends.base import BaseDatasetIndex, BasePartitionIndex,\
    BaseIdentifierIndex, BaseSearchBackend, IdentifierSearchResult,\
    DatasetSearchResult, PartitionSearchResult

from ambry.library.search_backends.base import SearchTermParser
from ambry.util import get_logger

logger = get_logger(__name__, level=logging.DEBUG)


class SqliteSearchBackend(BaseSearchBackend):

    def _get_dataset_index(self):
        """ Returns dataset index. """
        # returns initialized dataset index
        return DatasetSqliteIndex(backend=self)

    def _get_partition_index(self):
        """ Returns partition index. """
        return PartitionSqliteIndex(backend=self)

    def _get_identifier_index(self):
        """ Returns identifier index. """
        return IdentifierSqliteIndex(backend=self)


class DatasetSqliteIndex(BaseDatasetIndex):

    def search(self, search_phrase, limit=None):
        """ Finds datasets by search phrase.

        Args:
            search_phrase (str or unicode):
            limit (int, optional): how many results to return. None means without limit.

        Returns:
            list of DatasetSearchResult instances.

        """
        # TODO: implement me.
        pass

    def _index_document(self, document, force=False):
        """ Adds document to the index. """
        # TODO: implement me.
        pass

    def _delete(self, vid=None):
        """ Deletes given dataset from index.

        Args:
            vid (str): dataset vid.

        """
        # TODO: Implement me.


class IdentifierSqliteIndex(BaseIdentifierIndex):

    def search(self, search_phrase, limit=None):
        # TODO: implement.
        pass

    def _index_document(self, identifier, force=False):
        """ Adds identifier document to the index. """
        # TODO: implement.
        pass

    def _delete(self, identifier=None):
        """ Deletes given identifier from index.

        Args:
            identifier (str): identifier of the document to delete.

        """
        # TODO: implement.
        pass


class PartitionSqliteIndex(BasePartitionIndex):

    def search(self, search_phrase, limit=None):
        """ Finds partitions by search phrase.

        Args:
            search_phrase (str or unicode):
            limit (int, optional): how many results to generate. None means without limit.

        Generates:
            PartitionSearchResult instances.
        """
        # TODO: Implement.
        pass

    def _index_document(self, document, force=False):
        """ Adds parition document to the index. """
        # TODO: Implement.
        pass

    def _delete(self, vid=None):
        """ Deletes given partition with given vid from index.

        Args:
            vid (str): vid of the partition document to delete.

        """
        # TODO: Implement.
        pass
