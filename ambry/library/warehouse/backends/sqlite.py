import six

import apsw

from ambry_sources.med import sqlite as sqlite_med

from ambry.util import get_logger

from .base import DatabaseBackend
from ..exceptions import MissingTableError, MissingViewError

logger = get_logger(__name__)

# debug logging
#
# import logging
# logger = get_logger(__name__, level=logging.DEBUG, propagate=False)


class SQLiteBackend(DatabaseBackend):
    """ Warehouse backend to SQLite database. """

    def install(self, connection, partition, materialize=False):
        """ Creates virtual table or read-only table for gion.

        Args:
            ref (str): id, vid, name or versioned name of the partition.
            materialize (boolean): if True, create read-only table. If False create virtual table.

        Returns:
            str: name of the created table.

        """
        virtual_table = sqlite_med.table_name(partition.vid)

        if not self._relation_exists(connection, virtual_table):
            self._add_partition(connection, partition)
        table = '{}_v'.format(virtual_table)

        if materialize:

            if self._relation_exists(connection, table):
                logger.debug(
                    'Materialized table of the partition already exists.\n    partition: {}, table: {}'
                    .format(partition.name, table))
            else:
                cursor = connection.cursor()

                # create table
                create_query = self.__class__._get_create_query(partition, table)
                logger.debug(
                    'Creating new materialized view of the partition.'
                    '\n    partition: {}, view: {}, query: {}'
                    .format(partition.name, table, create_query))
                cursor.execute(create_query)

                # populate just created table with data from virtual table.
                copy_query = '''INSERT INTO {} SELECT * FROM {};'''.format(table, virtual_table)
                logger.debug(
                    'Populating sqlite table of the partition.'
                    '\n    partition: {}, view: {}, query: {}'
                    .format(partition.name, table, copy_query))
                cursor.execute(copy_query)

                cursor.close()
        return table if materialize else virtual_table

    def index(self, connection, partition, columns):
        """ Create an index on the columns.

        Args:
            connection (apsw.Connection): connection to sqlite database who stores warehouse data.
            partition (orm.Partition):
            columns (list of str):
        """
        query_tmpl = '''
            CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column});
        '''
        table_name = '{}_v'.format(sqlite_med.table_name(partition.vid))
        for column in columns:
            query = query_tmpl.format(
                index_name='{}_{}_i'.format(partition.vid, column), table_name=table_name,
                column=column)
            logger.debug('Creating sqlite index.\n    column: {}, query: {}'.format(column, query))
            cursor = connection.cursor()
            cursor.execute(query)

    def close(self):
        """ Closes connection to sqlite database. """
        if getattr(self, '_connection', None):
            logger.debug('Closing sqlite connection.')
            self._connection.close()

    @staticmethod
    def get_view_name(table):
        return table.vid

    def _get_warehouse_view(self, connection, table):
        """ Finds and returns view name in the sqlite db represented by given connection.

        Args:
            connection: connection to sqlite db where to look for partition table.
            table (orm.Table):

        Raises:
            MissingViewError: if database does not have partition table.

        Returns:
            str: database table storing partition data.

        """
        logger.debug(
            'Looking for view of the table.\n    table: {}'.format(table.vid))
        view = self.get_view_name(table)
        view_exists = self._relation_exists(connection, view)
        if view_exists:
            logger.debug(
                'View of the table exists.\n    table: {}, view: {}'
                .format(table.vid, view))
            return view
        raise MissingViewError('postgres database of the warehouse does not have view for {} table.'
                               .format(table.vid))

    def _get_warehouse_table(self, connection, partition):
        """ Returns name of the sqlite table who stores partition data.

        Args:
            connection (apsw.Connection): connection to sqlite database who stores warehouse data.
            partition (orm.Partition):

        Returns:
            str:

        Raises:
            MissingTableError: if partition table not found in the warehouse db.

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
        logger.debug(
            'Looking for materialized table of the partition.\n    partition: {}'.format(partition.name))
        table_exists = self._relation_exists(connection, table)
        if table_exists:
            logger.debug(
                'Materialized table of the partition found.\n    partition: {}, table: {}'
                .format(partition.name, table))
            return table

        # now check for virtual table
        logger.debug(
            'Looking for a virtual table of the partition.\n    partition: {}'.format(partition.name))
        virtual_exists = self._relation_exists(connection, virtual_table)
        if virtual_exists:
            logger.debug(
                'Virtual table of the partition found.\n    partition: {}, table: {}'
                .format(partition.name, table))
            return virtual_table
        raise MissingTableError('warehouse postgres database does not have table for {} partition.'
                                .format(partition.vid))

    def _relation_exists(self, connection, relation):
        """ Returns True if relation (table or view) exists in the sqlite db. Otherwise returns False.

        Args:
            connection (apsw.Connection): connection to sqlite database who stores warehouse data.
            partition (orm.Partition):

        Returns:
            boolean: True if relation exists, False otherwise.

        """
        query = 'SELECT 1 FROM sqlite_master WHERE (type=\'table\' OR type=\'view\') AND name=?;'
        cursor = connection.cursor()
        cursor.execute(query, [relation])
        result = cursor.fetchall()
        return result == [(1,)]

    @staticmethod
    def _get_create_query(partition, tablename, include=None):
        """ Creates and returns `CREATE TABLE ...` sql statement for given mprows.

        Args:
            partition (orm.Partition):
            tablename (str): name of the table in the return create query.
            include (list of str, optional): list of columns to include to query.

        Returns:
            str: create table query.

        """
        TYPE_MAP = {
            'int': 'INTEGER',
            'float': 'REAL',
            six.binary_type.__name__: 'TEXT',
            six.text_type.__name__: 'TEXT',
            'date': 'DATE',
            'datetime': 'TIMESTAMP WITHOUT TIME ZONE'
        }
        columns_types = []
        if not include:
            include = []
        for column in sorted(partition.datafile.reader.columns, key=lambda x: x['pos']):
            if include and column['name'] not in include:
                continue
            sqlite_type = TYPE_MAP.get(column['type'])
            if not sqlite_type:
                raise Exception('Do not know how to convert {} to sql column.'.format(column['type']))
            columns_types.append('    {} {}'.format(column['name'], sqlite_type))
        columns_types_str = ',\n'.join(columns_types)
        query = 'CREATE TABLE IF NOT EXISTS {}(\n{})'.format(tablename, columns_types_str)
        return query

    def _get_connection(self):
        """ Returns connection to warehouse sqlite db.

        Returns:
            connection to the sqlite db who stores warehouse data.

        """
        if getattr(self, '_connection', None):
            logger.debug('Connection to warehouse db already exists. Using existing one.')
        else:
            dsn = self._dsn
            if dsn == 'sqlite://':
                dsn = ':memory:'
            else:
                dsn = dsn.replace('sqlite:///', '')
            logger.debug(
                'Creating new apsw connection.\n   dsn: {}, config_dsn: {}'
                .format(dsn, self._dsn))
            self._connection = apsw.Connection(dsn)
        return self._connection

    def _add_partition(self, connection, partition):
        """ Creates sqlite virtual table for given partition.

        Args:
            connection: connection to the sqlite db who stores warehouse data.
            partition (orm.Partition):

        """
        logger.debug('Creating virtual table for partition.\n    partition: {}'.format(partition.name))
        sqlite_med.add_partition(connection, partition.datafile, partition.vid)

    def _execute(self, connection, query, fetch=True):
        """ Executes given query using given connection.

        Args:
            connection (apsw.Connection): connection to the sqlite db who stores warehouse data.
            query (str): sql query
            fetch (boolean, optional): if True, fetch query result and return it. If False, do not fetch.

        Returns:
            iterable with query result.

        """
        cursor = connection.cursor()
        cursor.execute(query)
        if fetch:
            return cursor.fetchall()
