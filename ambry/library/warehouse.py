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

from ambry_sources.med import sqlite as sqlite_med
from ambry_sources.med import postgresql as postgres_med

from ambry.util import get_logger


logger = get_logger(__name__)  # , level=logging.DEBUG, propagate=False)


class WarehouseError(Exception):
    pass


class MissingTableError(Exception):
    """ raises when warehouse does not have table for the partition. """
    pass


class Warehouse(object):
    """ Provides SQL access to datasets in the library, allowing users to issue SQL queries, either as SQL
or via SQLAlchemy, to return datasets.
    """

    def __init__(self, library):
        self._library = library

    def _get_warehouse_table(self, partition):
        """ Returns table name which stores partition data.

        Args:
            partition FIXME:
            connection FIXME: connection to warehouse db.

        Returns:
            str:

        Raises:
            FIXME: if partition table not found in the warehouse db.

        """
        # FIXME: This is the first candidate for optimization. Add field to partition
        # with table name and update it while table creation.
        # Optimized version.
        #
        # return partition.warehouse_table or raise exception

        # Not optimized version.
        #
        # first check either partition has materialized view.
        fdw_table = postgres_med.table_name(partition.vid)
        view_table = '{}_v'.format(fdw_table)

        schema_name, table_name = fdw_table.split('.')
        exists_query = '''
            SELECT 1
                FROM   pg_catalog.pg_class c
                JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE  n.nspname = %s
                AND    c.relname = %s
                AND    (c.relkind = 'r' OR c.relkind = 'v' OR c.relkind = 'm')
                    -- r - table, v - view, m - materialized view.
        '''
        connection = self._library.database.engine.raw_connection()
        with connection.cursor() as cursor:
            # check for materialized view.
            view_exists = self._relation_exists(cursor, schema_name, '{}_v'.format(table_name))
            if view_exists:
                connection.close()
                return view_table

            # now check for fdw/virtual table
            fdw_exists = self._relation_exists(cursor, schema_name, table_name)
            if fdw_exists:
                connection.close()
                return fdw_table
        # FIXME: Add hint.
        connection.close()
        raise MissingTableError('warehouse database does not have table for {} partition.'
                                .format(partition.vid))

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
        logger.debug('Looking for refs and create virtual table for each. query: {}'.format(query))
        if self._library.database.engine.name == 'sqlite':
            return self._sqlite_execute(query)
        elif self._library.database.engine.name == 'postgresql':
            return self._postgres_execute(query)
        else:
            raise Exception(
                'Do not know how to execute query on {} database.'
                .format(self._library.database.engine.name))

    def _relation_exists(self, cursor, schema_name, table_name):
        """ Returns True if relation table_name exists in the schema_name schema. Otherwise returns False. """
        exists_query = '''
            SELECT 1
            FROM   pg_catalog.pg_class c
            JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE  n.nspname = %s
            AND    c.relname = %s
            AND    (c.relkind = 'r' OR c.relkind = 'v' OR c.relkind = 'm')
                -- r - table, v - view, m - materialized view.
        '''
        cursor.execute(exists_query, [schema_name, table_name])
        result = cursor.fetchall()
        return result == [(1,)]

    def _postgres_install(self, ref, materialize=False):
        """ Creates FDW for given partition.

        Returns:
            str: name of the created table.

        """

        # FIXME: connect to 'warehouse' schema.
        connection = self._library.database.engine.raw_connection()
        with connection.cursor() as cursor:
            self._install(ref, self._library.database, connection, cursor)
            fdw_table = postgres_med.table_name(ref)
            view_table = '{}_v'.format(fdw_table)

            if materialize:
                schema_name, table_name = view_table.split('.')
                view_exists = self._relation_exists(cursor, schema_name, table_name)
                if not view_exists:
                    query = 'CREATE MATERIALIZED VIEW {} AS SELECT * FROM {};'\
                        .format(view_table, fdw_table)
                    cursor.execute(query)
            cursor.execute('COMMIT;')
        connection.close()
        return view_table if materialize else fdw_table

    def _postgres_execute(self, query=''):
        statements = sqlparse.parse(query)

        # FIXME: connect to 'warehouse' schema.

        # install all partitions
        new_query = []
        for statement in statements:
            ref = self._get_table_name(statement)
            # FIXME: install method should return warehouse table.
            try:
                partition = self._library.partition(ref)
                warehouse_table = self._get_warehouse_table(partition)
            except MissingTableError:
                # FIXME: Do not catch all exceptions.
                warehouse_table = self._postgres_install(ref)
            new_query.append(statement.to_unicode().replace(ref, warehouse_table))
            new_query = '\n'.join(new_query)

        # execute query
        connection = self._library.database.engine.raw_connection()
        with connection.cursor() as cursor:
            cursor.execute(new_query)
            result = cursor.fetchall()
        connection.close()
        return result

    def _sqlite_execute(self, query=''):
        # we need apsw connection to operate with virtual tables.
        import apsw
        statements = sqlparse.parse(query)

        connection = apsw.Connection(':memory:')
        with connection:
            new_query = []
            for statement in statements:
                ref = self._get_table_name(statement)
                self._install(ref, self._library.database, connection, None)
                new_query.append(statement.to_unicode().replace(ref, sqlite_med.table_name(ref)))

            new_query = '\n'.join(new_query)

            # execute sql
            cursor = connection.cursor()
            result = cursor.execute(new_query).fetchall()
        connection.close()
        return result

    # FIXME: classmethod
    def _install(self, ref, database, connection, cursor):
        """ Finds partition by reference and installs it.

        Args:
            ref: FIXME: describe with examples.

        """
        # FIXME: Why do we need both - connection and cursor? Simplify interfaces.
        db_name = self._library.database.engine.name

        partition = self._library.partition(ref)
        if db_name == 'sqlite':
            logger.debug('Creating virtual table for {} partition.'.format(ref))
            sqlite_med.add_partition(connection, partition.datafile, partition.vid)
        elif db_name == 'postgresql':
            logger.debug('Creating foreign table for {} partition.'.format(ref))
            postgres_med.add_partition(cursor, partition.datafile, partition.vid)
        else:
            raise Exception('Do not know how to install partition into {} database.'.format(db_name))

    def _index(self, ref, columns):
        """ Create an index on the columns.

        Args:
            ref: FIXME:
            columns (list):

        """
        pass

    def materialize(self, ref):
        """ Creates a materialized view for given partition reference.

        Args:
            ref: FIXME:

        """
        if self._library.database.engine.name == 'sqlite':
            return self._sqlite_install(ref, materialize=True)
        elif self._library.database.engine.name == 'postgresql':
            return self._postgres_install(ref, materialize=True)
