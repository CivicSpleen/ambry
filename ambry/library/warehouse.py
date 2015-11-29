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


class MissingTableError(WarehouseError):
    """ Raises if warehouse does not have table for the partition. """
    pass


class Warehouse(object):
    """ Provides SQL access to datasets in the library, allowing users to issue SQL queries, either as SQL
or via SQLAlchemy, to return datasets.
    """

    def __init__(self, library):
        self._library = library
        if self._library.database.engine.name == 'sqlite':
            self._backend = SQLiteWrapper(library)
        elif self._library.database.engine.name == 'postgresql':
            self._backend = PostgreSQLWrapper(library)
        else:
            raise Exception(
                'Do not know how to handle {} db engine.'
                .format(self._library.database.engine.name))

    def query(self, query=''):
        """ Creates virtual tables for all partitions found in the query and executes query.

        Args:
            query (str): sql query

        """

        logger.debug('Looking for refs and create virtual table for each. query: {}'.format(query))
        return self._backend.execute(query)

    # FIXME: classmethod
    def install(self, ref, database, connection, cursor):
        """ Finds partition by reference and installs it.

        Args:
            ref: FIXME: describe with examples.

        """
        # FIXME: Why do we need both - connection and cursor? Simplify interfaces.
        partition = self._library.partition(ref)
        self.backend.add_partition(partition, database, connection, cursor)

    def index(self, ref, columns):
        """ Create an index on the columns.

        Args:
            ref: FIXME:
            columns (list):

        """
        self._backend.index(ref, columns)

    def materialize(self, ref):
        """ Creates a materialized view for given partition reference.

        Args:
            ref: FIXME:

        Returns:
            FIXME:

        """
        return self._backend.install(ref, materialize=True)

    def close(self):
        """ Closes warehouse database. """
        # FIXME: implement
        # self._backend.close()
        pass


class DatabaseWrapper(object):
    """ Base class for warehouse databases engines. """

    def __init__(self, library):
        self._library = library

    def install(self, ref, materialize=False):
        raise NotImplementedError

    def execute(self, query=''):
        raise NotImplementedError

    def index(self, ref, columns):
        """ Create an index on the columns.

        Args:
            ref: FIXME:
            columns (list):

        """
        raise NotImplementedError

    def close(self):
        # FIXME:
        pass

    def _get_warehouse_table(self, partition):
        raise NotImplementedError

    def _relation_exists(self, cursor, schema_name, table_name):
        raise NotImplementedError


class PostgreSQLWrapper(DatabaseWrapper):

    def add_partition(self, partition, database, connection, cursor):
        logger.debug('Creating foreign table for {} partition.'.format(partition.name))
        postgres_med.add_partition(cursor, partition.datafile, partition.vid)

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

    def install(self, ref, materialize=False):
        """ Creates FDW or materialize view for given partition.

        Args:
            ref (str): id, vid, name or versioned name of the partition.
            materialize (boolean): if True, create read-only table. If False create virtual table.

        Returns:
            str: name of the created table.

        """

        # FIXME: connect to 'warehouse' schema.
        connection = self._library.database.engine.raw_connection()
        partition = self._library.partition(ref)
        with connection.cursor() as cursor:
            self.add_partition(partition, self._library.database, connection, cursor)
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

    def execute(self, query=''):
        statements = sqlparse.parse(query)

        # install all partitions
        new_query = []
        for statement in statements:
            ref = _get_table_name(statement)
            # FIXME: install method should return warehouse table.
            try:
                partition = self._library.partition(ref)
                warehouse_table = self._get_warehouse_table(partition)
            except MissingTableError:
                # FDW is not created, create.
                warehouse_table = self.install(ref)
            new_query.append(statement.to_unicode().replace(ref, warehouse_table))
            new_query = '\n'.join(new_query)

        # execute query
        connection = self._library.database.engine.raw_connection()
        with connection.cursor() as cursor:
            cursor.execute(new_query)
            result = cursor.fetchall()
        connection.close()
        return result


class SQLiteWrapper(DatabaseWrapper):

    def add_partition(self, partition, database, connection, cursor):
        logger.debug('Creating virtual table for {} partition.'.format(partition.name))
        sqlite_med.add_partition(connection, partition.datafile, partition.vid)

    def install(self, ref, materialize=False):
        """ Creates virtual table or read-only table for given partition.

        Args:
            ref (str): id, vid, name or versioned name of the partition.
            materialize (boolean): if True, create read-only table. If False create virtual table.

        Returns:
            str: name of the created table.
        """
        # FIXME: Replace with sqlite code.
        connection = self._library.database.engine.raw_connection()
        with connection.cursor() as cursor:
            self.install(ref, self._library.database, connection, cursor)
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

    def execute(self, query=''):
        # we need apsw connection to operate with virtual tables.
        # FIXME:
        import apsw
        statements = sqlparse.parse(query)

        try:
            partition = self._library.partition(ref)
            warehouse_table = self._get_warehouse_table(partition)
        except MissingTableError:
            # FDW is not created, create.
            warehouse_table = self._postgres_install(ref)

        new_query = []
        for statement in statements:
            ref = _get_table_name(statement)
            self.install(ref, self._library.database, connection, None)
            new_query.append(statement.to_unicode().replace(ref, sqlite_med.table_name(ref)))
            new_query = '\n'.join(new_query)

        connection = apsw.Connection(':memory:')
        with connection:

            # execute sql
            cursor = connection.cursor()
            result = cursor.execute(new_query).fetchall()
        connection.close()
        return result


def _get_table_name(statement):
    """ Finds first identifier in the statement and returns it.

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
