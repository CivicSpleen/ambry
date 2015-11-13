"""
Provides SQL access to datasets in the library, allowing users to issue SQL queries, either as SQL
or via Sqlalchemy, to return datasets.

Example:
    import ambry
    l = ambry.get_library()
    for row in l.warehouse.query('SELECT * FROM <partition id or vid> ... '):
        print row
"""

import logging

import sqlparse

from ambry_sources.med.sqlite import add_partition

from ambry.util import get_logger


logger = get_logger(__name__, level=logging.DEBUG, propagate=False)


class Warehouse(object):
    """ Provides SQL access to datasets in the library, allowing users to issue SQL queries, either as SQL
or via Sqlalchemy, to return datasets.
    """

    def __init__(self, library):
        self._library = library

    def _get_table_name(self, statement):
        """ Finds first identifier and returns it.

        Args:
            statement (sqlparse.sql.Statement): parsed by sqlparse sql statement.

        Returns:
            unicode:
        """
        for i, token in enumerate(statement.tokens):
            if token.value.lower() == 'from':
                # check rest for table name
                for elem in statement.tokens[i:]:
                    if isinstance(elem, sqlparse.sql.Identifier):
                        logger.debug('Returning `{}` table name found in `{}` statement.'
                                     .format(elem.get_name(), statement.to_unicode()))
                        return elem.get_name()
        raise Exception('Table name not found.')

    def query(self, sql=''):

        # create tables for all partitions found in the sql.
        logger.debug('Finding refs and create virtual table for each. query: {}'.format(sql))
        statements = sqlparse.parse(sql)
        for statement in statements:
            ref = self._get_table_name(statement)
            self._install(ref)

        # execute sql
        # FIXME:

    def _install(self, ref):
        """ Finds partition by reference and installs it.

        Args:
            ref: FIXME: describe with examples.

        """
        logger.debug('Creating virtual table for {}.'.format(ref))

        # FIXME: add_partition requires apsw connection.
        partition = self._library.partition(ref)
        add_partition(self._library.database.connection, partition, partition.vid)

    def _index(self, ref, columns):
        """ Create an index on the columns.

        Args:
            ref: FIXME:
            columns (list):

        """
        pass

    def _materialize(self, ref):
        """ Creates a table for given partition reference.

        Args:
            ref: FIXME:

        """
        pass
