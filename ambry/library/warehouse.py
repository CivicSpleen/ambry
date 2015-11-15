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

import apsw

import sqlparse

from ambry_sources.med.sqlite import add_partition, table_name

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

    def query(self, query=''):

        # create tables for all partitions found in the sql.
        logger.debug('Finding refs and create virtual table for each. query: {}'.format(query))
        statements = sqlparse.parse(query)

        # we need apsw connection to operate with virtual tables.
        connection = apsw.Connection(':memory:')
        new_query = []
        for statement in statements:
            ref = self._get_table_name(statement)
            self._install(ref, self._library.database, connection)
            new_query.append(statement.to_unicode().replace(ref, table_name(ref)))

        new_query = '\n'.join(new_query)

        # execute sql
        # FIXME:
        cursor = connection.cursor()
        result = cursor.execute(new_query).fetchall()
        return result

    # FIXME: classmethod
    def _install(self, ref, database, connection):
        """ Finds partition by reference and installs it.

        Args:
            ref: FIXME: describe with examples.

        """
        logger.debug('Creating virtual table for {}.'.format(ref))

        # FIXME: add_partition requires apsw connection.
        partition = self._library.partition(ref)
        add_partition(connection, partition.datafile, partition.vid)

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
