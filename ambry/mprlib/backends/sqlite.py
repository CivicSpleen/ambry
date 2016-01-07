import six

import apsw

from ambry_sources.med import sqlite as sqlite_med

from ambry.util import get_logger

from ambry.bundle.asql_parser import parse_view, parse_index

from .base import DatabaseBackend
from ..exceptions import MissingTableError, MissingViewError

logger = get_logger(__name__)

# debug logging
#
# import logging
# logger = get_logger(__name__, level=logging.DEBUG, propagate=False)


class SQLiteBackend(DatabaseBackend):
    """ Backend to install/query MPR files for SQLite database. """

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
                    'Creating new materialized view for partition mpr.'
                    '\n    partition: {}, view: {}, query: {}'
                    .format(partition.name, table, create_query))
                cursor.execute(create_query)

                # populate just created table with data from virtual table.
                copy_query = '''INSERT INTO {} SELECT * FROM {};'''.format(table, virtual_table)
                logger.debug(
                    'Populating sqlite table with rows from partition mpr.'
                    '\n    partition: {}, view: {}, query: {}'
                    .format(partition.name, table, copy_query))
                cursor.execute(copy_query)

                cursor.close()
        return table if materialize else virtual_table

    def index(self, connection, partition, columns):
        """ Create an index on the columns.

        Args:
            connection (apsw.Connection): connection to sqlite database who stores mpr table or view.
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

    def _get_mpr_view(self, connection, table):
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
        raise MissingViewError('sqlite database does not have view for {} table.'
                               .format(table.vid))

    def _get_mpr_table(self, connection, partition):
        """ Returns name of the sqlite table who stores mpr data.

        Args:
            connection (apsw.Connection): connection to sqlite database who stores mpr data.
            partition (orm.Partition):

        Returns:
            str:

        Raises:
            MissingTableError: if partition table not found in the db.

        """
        # FIXME: This is the first candidate for optimization. Add field to partition
        # with table name and update it while table creation.
        # Optimized version.
        #
        # return partition.mpr_table or raise exception

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
        raise MissingTableError('sqlite database does not have table for mpr of {} partition.'
                                .format(partition.vid))

    def _relation_exists(self, connection, relation):
        """ Returns True if relation (table or view) exists in the sqlite db. Otherwise returns False.

        Args:
            connection (apsw.Connection): connection to sqlite database who stores mpr data.
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
        """ Returns connection to sqlite db.

        Returns:
            connection to the sqlite db who stores mpr data.

        """
        if getattr(self, '_connection', None):
            logger.debug('Connection to sqlite db already exists. Using existing one.')
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
        """ Creates sqlite virtual table for mpr file of the given partition.

        Args:
            connection: connection to the sqlite db who stores mpr data.
            partition (orm.Partition):

        """
        logger.debug('Creating virtual table for partition.\n    partition: {}'.format(partition.name))
        sqlite_med.add_partition(connection, partition.datafile, partition.vid)

    def _execute(self, connection, query, fetch=True):
        """ Executes given query using given connection.

        Args:
            connection (apsw.Connection): connection to the sqlite db who stores mpr data.
            query (str): sql query
            fetch (boolean, optional): if True, fetch query result and return it. If False, do not fetch.

        Returns:
            iterable with query result.

        """
        cursor = connection.cursor()
        cursor.execute(query)
        if fetch:
            return cursor.fetchall()


def _preprocess_sqlite_view(asql_query, library, backend, connection):
    """ Finds materialized view and converts it to sqlite format.

    Note:
        Assume virtual tables for all partitions already created.

    Args:
        asql_query (str): asql query
        library (ambry.Library):
        backend (SQLiteBackend):
        connection (apsw.Connection):

    Returns:
        str: valid sql query containing create table and insert into queries if asql_query contains
            'create materialized view'. If asql_query does not contain 'create materialized view' returns
            asql_query as is.
    """
    new_query = None
    if 'create materialized view' in asql_query.lower():
        logger.debug(
            '_preprocess_sqlite_view: materialized view found.\n    asql query: {}'
            .format(asql_query))
        view = parse_view(asql_query)

        tablename = view.name.replace('-', '_').lower().replace('.', '_')
        create_query_columns = {}
        for column in view.columns:
            create_query_columns[column.name] = column.alias

        ref_to_partition_map = {}  # key is ref found in the query, value is Partition instance.
        alias_to_partition_map = {}  # key is alias of ref found in the query, value is Partition instance.

        # collect sources from select statement of the view.
        for source in view.sources:
            partition = library.partition(source.name)
            ref_to_partition_map[source.name] = partition
            if source.alias:
                alias_to_partition_map[source.alias] = partition

        # collect sources from joins of the view.
        for join in view.joins:
            partition = library.partition(join.source.name)
            ref_to_partition_map[join.source.name] = partition
            if join.source.alias:
                alias_to_partition_map[join.source.alias] = partition

        # collect and convert columns.
        TYPE_MAP = {
            'int': 'INTEGER',
            'float': 'REAL',
            six.binary_type.__name__: 'TEXT',
            six.text_type.__name__: 'TEXT',
            'date': 'DATE',
            'datetime': 'TIMESTAMP WITHOUT TIME ZONE'
        }
        column_types = []
        column_names = []
        for column in view.columns:
            if '.' in column.name:
                source_alias, column_name = column.name.split('.')
            else:
                # FIXME: Test that case.
                source_alias = None
                column_name = column.name

            # find column specification in the mpr file.
            if source_alias:
                partition = alias_to_partition_map[source_alias]
                for part_column in partition.datafile.reader.columns:
                    if part_column['name'] == column_name:
                        sqlite_type = TYPE_MAP.get(part_column['type'])
                        if not sqlite_type:
                            raise Exception(
                                'Do not know how to convert {} to sql column.'
                                .format(column['type']))

                        column_types.append(
                            '    {} {}'
                            .format(column.alias if column.alias else column.name, sqlite_type))
                        column_names.append(column.alias if column.alias else column.name)
        column_types_str = ',\n'.join(column_types)
        column_names_str = ', '.join(column_names)

        create_query = 'CREATE TABLE IF NOT EXISTS {}(\n{});'.format(tablename, column_types_str)

        # drop 'create materialized view' part
        _, select_part = asql_query.split(view.name)
        select_part = select_part.strip()
        assert select_part.lower().startswith('as')

        # drop 'as' keyword
        select_part = select_part.strip()[2:].strip()
        assert select_part.lower().strip().startswith('select')

        # Create query to copy data from mpr to just created table.
        copy_query = 'INSERT INTO {table}(\n{columns})\n  {select}'.format(
            table=tablename, columns=column_names_str, select=select_part)
        if not copy_query.strip().lower().endswith(';'):
            copy_query = copy_query + ';'
        new_query = '{}\n\n{}'.format(create_query, copy_query)
    logger.debug(
        '_preprocess_sqlite_view: preprocess finished.\n    asql query: {}\n\n    new query: {}'
        .format(asql_query, new_query))
    return new_query or asql_query


def _preprocess_sqlite_index(asql_query, library, backend, connection):
    """ Creates materialized view for each indexed partition found in the query.

    Args:
        asql_query (str): asql query
        library (ambry.Library):
        backend (SQLiteBackend):
        connection (apsw.Connection):

    Returns:
        str: converted asql if it contains index query. If not, returns asql_query as is.
    """
    new_query = None
    if asql_query.strip().lower().startswith('index'):
        logger.debug(
            '_preprocess_index: create index query found.\n    asql query: {}'
            .format(asql_query))
        index = parse_index(asql_query)
        partition = library.partition(index.source)
        table = backend.install(connection, partition, materialize=True)
        index_name = '{}_{}_ind'.format(partition.vid, '_'.join(index.columns))
        new_query = 'CREATE INDEX IF NOT EXISTS {index} ON {table} ({columns});'.format(
            index=index_name, table=table, columns=','.join(index.columns))

    logger.debug(
        '_preprocess_index: preprocess finished.\n    asql query: {}\n    new query: {}'
        .format(asql_query, new_query))
    return new_query or asql_query
