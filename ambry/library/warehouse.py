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

import six

import apsw

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
        # If keep_connection is true, do not close the connection until close method call.
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
        connection = self._backend._get_connection()
        return self._backend.query(connection, query)

    # FIXME: classmethod
    def install(self, ref):
        """ Finds partition by reference and installs it to warehouse db.

        Args:
            ref: FIXME: describe with examples.

        """
        # FIXME: Why do we need both - connection and cursor? Simplify interfaces.
        partition = self._library.partition(ref)
        connection = self._backend._get_connection()
        self.backend.install(connection, partition)

    def index(self, ref, columns):
        """ Create an index on the columns.

        Args:
            ref: FIXME:
            columns (list):

        """
        connection = self._backend._get_connection()
        self._backend.index(connection, ref, columns)

    def materialize(self, ref):
        """ Creates a materialized view for given partition reference.

        Args:
            ref: FIXME:

        Returns:
            FIXME:

        """
        partition = self._library.partition(ref)
        connection = self._backend._get_connection()
        return self._backend.install(connection, partition, materialize=True)

    def close(self):
        """ Closes warehouse database. """
        self._backend.close()


class DatabaseWrapper(object):
    """ Base class for warehouse databases engines. """

    def __init__(self, library):
        self._library = library

    def install(self, connection, partition, materialize=False):
        raise NotImplementedError

    def _get_connection(self):
        raise NotImplementedError

    def query(self, connection, query):
        """ Creates virtual tables for all partitions found in the query and executes query.

        Args:
            query (str): sql query

        """
        statements = sqlparse.parse(query)

        # install all partitions and replace table names in the query.
        #
        new_query = []
        for statement in statements:
            ref = _get_table_name(statement)
            partition = self._library.partition(ref)
            try:
                # try to use existing fdw or materialized view.
                warehouse_table = self._get_warehouse_table(connection, partition)
            except MissingTableError:
                # FDW is not created, create.
                warehouse_table = self.install(connection, partition)
            new_query.append(statement.to_unicode().replace(ref, warehouse_table))
            new_query = '\n'.join(new_query)
        return self._execute(connection, new_query)

    def index(self, connection, partition, columns):
        """ Create an index on the columns.

        Args:
            ref: FIXME:
            columns (list):

        """
        raise NotImplementedError

    def close(self):
        # FIXME:
        pass

    def _get_warehouse_table(self, connection, partition):
        raise NotImplementedError

    def _execute(self, connection, query):
        """ FIXME: """
        raise NotImplementedError


class PostgreSQLWrapper(DatabaseWrapper):
    """ Warehouse wrapper over PostgreSQL database. """

    def _add_partition(self, connection, partition):
        """ Creates FDW for the partition.

        Args:
            FIXME:

        """
        logger.debug('Creating foreign table for {} partition.'.format(partition.name))
        with connection.cursor() as cursor:
            postgres_med.add_partition(cursor, partition.datafile, partition.vid)

    def install(self, connection, partition, materialize=False):
        """ Creates FDW or materialize view for given partition.

        Args:
            ref (str): id, vid, name or versioned name of the partition.
            materialize (boolean): if True, create read-only table. If False create virtual table.

        Returns:
            str: name of the created table.

        """

        # FIXME: connect to 'warehouse' schema.
        self._add_partition(connection, partition)
        fdw_table = postgres_med.table_name(partition.vid)
        view_table = '{}_v'.format(fdw_table)

        if materialize:
            with connection.cursor() as cursor:
                view_exists = self._relation_exists(connection, view_table)
                if not view_exists:
                    query = 'CREATE MATERIALIZED VIEW {} AS SELECT * FROM {};'\
                        .format(view_table, fdw_table)
                    cursor.execute(query)
                cursor.execute('COMMIT;')
        return view_table if materialize else fdw_table

    def close(self):
        if getattr(self, '_connection', None):
            self._connection.close()
            self._connection = None

    def _execute(self, connection, query):
        """ Executes given query and returns result. """
        # execute query
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
        return result

    def _get_connection(self):
        if not getattr(self, '_connection', None):
            self._connection = self._library.database.engine.raw_connection()
        return self._connection

    def _get_warehouse_table(self, connection, partition):
        """ Returns name of the table who stores partition data.

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
        view_exists = self._relation_exists(connection, view_table)
        if view_exists:
            return view_table

        # now check for fdw/virtual table
        fdw_exists = self._relation_exists(connection, fdw_table)
        if fdw_exists:
            return fdw_table
        raise MissingTableError('warehouse postgres database does not have table for {} partition.'
                                .format(partition.vid))

    def _relation_exists(self, connection, relation):
        """ Returns True if relation exists in the postgres db. Otherwise returns False. """
        schema_name, table_name = relation.split('.')

        exists_query = '''
            SELECT 1
            FROM   pg_catalog.pg_class c
            JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE  n.nspname = %s
            AND    c.relname = %s
            AND    (c.relkind = 'r' OR c.relkind = 'v' OR c.relkind = 'm')
                -- r - table, v - view, m - materialized view.
        '''
        with connection.cursor() as cursor:
            cursor.execute(exists_query, [schema_name, table_name])
            result = cursor.fetchall()
            return result == [(1,)]


class SQLiteWrapper(DatabaseWrapper):
    """ Warehouse wrapper over SQLite database. """

    def install(self, connection, partition, materialize=False):
        """ Creates virtual table or read-only table for given partition.

        Args:
            ref (str): id, vid,FIXME: name or versioned name of the partition.
            materialize (boolean): if True, create read-only table. If False create virtual table.

        Returns:
            str: name of the created table.
        """
        self._add_partition(connection, partition)
        virtual_table = sqlite_med.table_name(partition.vid)
        table = '{}_v'.format(virtual_table)

        if materialize:
            if not self._relation_exists(connection, table):
                cursor = connection.cursor()
                # create table
                create_query = self.__class__._get_create_query(partition.datafile, table)
                cursor.execute(create_query)

                # populate just created table with data from virtual table.
                copy_query = '''INSERT INTO {} SELECT * FROM {};'''.format(table, virtual_table)
                cursor.execute(copy_query)

                # FIXME: make the table read only.
                # ro_query = ''';'''
                # cursor.execute(ro_query)
                cursor.close()
        return table if materialize else virtual_table

    def close(self):
        if getattr(self, '_connection', None):
            self._connection.close()

    def _get_warehouse_table(self, connection, partition):
        """ Returns name of the table who stores partition data.

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
        # first check either partition has readonly table.
        virtual_table = sqlite_med.table_name(partition.vid)
        table = '{}_v'.format(virtual_table)
        table_exists = self._relation_exists(connection, virtual_table)
        if table_exists:
            return table

        # now check for virtual table
        virtual_exists = self._relation_exists(connection, virtual_table)
        if virtual_exists:
            return virtual_table
        raise MissingTableError('warehouse postgres database does not have table for {} partition.'
                                .format(partition.vid))

    def _relation_exists(self, connection, relation):
        query = 'SELECT 1 FROM sqlite_master WHERE type=\'table\' AND name=?;'
        cursor = connection.cursor()
        cursor.execute(query, [relation])
        result = cursor.fetchall()
        return result == [(1,)]

    @staticmethod
    def _get_create_query(mprows, tablename):
        """ Returns tuple FIXME: """
        TYPE_MAP = {
            'int': 'INTEGER',
            'float': 'REAL',
            six.binary_type.__name__: 'TEXT',
            six.text_type.__name__: 'TEXT',
            'date': 'DATE',
            'datetime': 'TIMESTAMP WITHOUT TIME ZONE'
        }
        columns_types = []
        for column in sorted(mprows.reader.columns, key=lambda x: x['pos']):
            sqlite_type = TYPE_MAP.get(column['type'])
            if not sqlite_type:
                raise Exception('Do not know how to convert {} to sql column.'.format(column['type']))
            columns_types.append('    {} {}'.format(column['name'], sqlite_type))
        columns_types_str = ',\n'.join(columns_types)
        query = 'CREATE TABLE IF NOT EXISTS {}(\n{})'.format(tablename, columns_types_str)
        return query

    def _get_connection(self):
        # FIXME: use connection from config or database if config is missed.
        if not getattr(self, '_connection', None):
            dsn = self._library.database.dsn
            if dsn == 'sqlite://':
                dsn = ':memory:'
            else:
                dsn = dsn.replace('sqlite:///', '')
            self._connection = apsw.Connection(dsn)
        return self._connection

    def _add_partition(self, connection, partition):
        """ Creates sqlite virtual table for given partition. """
        logger.debug('Creating virtual table for {} partition.'.format(partition.name))
        sqlite_med.add_partition(connection, partition.datafile, partition.vid)

    def _execute(self, connection, query):
        # FIXME: Assuming apsw.Connection
        cursor = connection.cursor()
        result = cursor.execute(query).fetchall()
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
