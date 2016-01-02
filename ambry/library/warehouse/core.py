"""
Provides SQL access to datasets in the library, allowing users to issue SQL queries, either as SQL
or via Sqlalchemy, to return datasets.

Example:
    import ambry
    l = ambry.get_library()
    w = Warehouse(l)
    for row in Warehouse(l).query('SELECT * FROM <partition id or vid> ... '):
        print row
    w.close()
"""

import sqlparse

import six

from ambry.bundle.asql_parser import parse_view, parse_index
from ambry.identity import ObjectNumber, NotObjectNumberError, TableNumber
from ambry.util import get_logger

from .backends.sqlite import SQLiteBackend
from .backends.postgresql import PostgreSQLBackend


logger = get_logger(__name__)

# debug logging
#
# import logging
# logger = get_logger(__name__, level=logging.DEBUG, propagate=False)


class Warehouse(object):
    """ Provides SQL access to datasets in the library, allowing users to issue SQL queries, either as SQL
or via SQLAlchemy, to return datasets.
    """

    def __init__(self, library):
        self._library = library

        warehouse_dsn = library.config.library.get('warehouse')
        if not warehouse_dsn:
            warehouse_dsn = library.config.library.database

        # Initialize appropriate backend.
        if warehouse_dsn.startswith('sqlite:'):
            logger.debug('Initializing sqlite warehouse.')
            self._backend = SQLiteBackend(library, warehouse_dsn)
        elif warehouse_dsn.startswith('postgresql'):
            logger.debug('Initializing postgres warehouse.')
            self._backend = PostgreSQLBackend(library, warehouse_dsn)
        else:
            raise Exception(
                'Do not know how to handle {} dsn.'
                .format(warehouse_dsn))

    def query(self, query=''):
        """ Creates virtual tables for all partitions found in the query and executes query.

        Args:
            query (str): sql query

        """
        # FIXME: If query is empty, return a Sqlalchemy query or select object
        logger.debug(
            'Executing warehouse query using {} backend.\n    query: {}'
            .format(self._backend._dsn, query))
        connection = self._backend._get_connection()
        return self._backend.query(connection, query)

    def install(self, ref):
        """ Finds partition by reference and installs it to warehouse db.

        Args:
            ref (str): id, vid, name or versioned name of the partition.

        """
        try:
            obj_number = ObjectNumber.parse(ref)
            if isinstance(obj_number, TableNumber):
                table = self._library.table(ref)
                connection = self._backend._get_connection()
                self._backend.install_table(connection, table)
            else:
                # assume partition
                raise NotObjectNumberError
        except NotObjectNumberError:
            # assume partition.
            partition = self._library.partition(ref)
            connection = self._backend._get_connection()
            self._backend.install(connection, partition)

    def materialize(self, ref):
        """ Creates materialized table for given partition reference.

        Args:
            ref (str): id, vid, name or versioned name of the partition.

        Returns:
            str: name of the partition table in the database.

        """
        logger.debug(
            'Materializing warehouse partition.\n    partition: {}'.format(ref))
        partition = self._library.partition(ref)
        connection = self._backend._get_connection()
        return self._backend.install(connection, partition, materialize=True)

    def index(self, ref, columns):
        """ Create an index on the columns.

        Args:
            ref (str): id, vid, name or versioned name of the partition.
            columns (list of str): names of the columns needed indexes.

        """
        logger.debug(
            'Creating index for partition.\n    ref: {}, columns: {}'.format(ref, columns))
        connection = self._backend._get_connection()
        partition = self._library.partition(ref)
        self._backend.index(connection, partition, columns)

    def close(self):
        """ Closes warehouse database. """
        self._backend.close()


# bundle.sql processing implementation.
# FIXME: Move to the better place.

def execute_sql(bundle, asql):
    """ Executes all sql statements from asql.

    Args:
        bundle (FIXME:):
        asql (str): unified sql query - see https://github.com/CivicKnowledge/ambry/issues/140 for details.
    """
    engine_name = bundle.library.database.engine.name
    if engine_name == 'sqlite':
        backend = SQLiteBackend(bundle.library, bundle.library.database.dsn)
        pipe = [_preprocess_sqlite_view, _preprocess_sqlite_index]
    elif engine_name == 'postgresql':
        backend = PostgreSQLBackend(bundle.library, bundle.library.database.dsn)
        pipe = [_preprocess_postgres_index]
    else:
        raise Exception('Do not know backend for {} database.'.format(engine_name))
    connection = backend._get_connection()
    if engine_name == 'postgresql':
        with connection.cursor() as cursor:
            # TODO: Move to backend methods.
            cursor.execute('SET search_path TO {};'.format(bundle.library.database._schema))
    try:
        statements = sqlparse.parse(sqlparse.format(asql, strip_comments=True))
        for statement in statements:
            statement_str = statement.to_unicode()
            for preprocessor in pipe:
                statement_str = preprocessor(statement_str, bundle.library, backend, connection)
            if statement_str.strip():
                backend.query(connection, statement_str, fetch=False)
    finally:
        backend.close()


def _get_table_names1(statement):
    # Simplified version - is more appropriate for ambry queryes
    parts = statement.to_unicode().split()
    tables = set()
    for i, token in enumerate(parts):
        if token.lower() == 'from' or token.lower().endswith('join'):
            tables.add(parts[i + 1].rstrip(';'))
    return list(tables)


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


def _preprocess_postgres_index(asql_query, library, backend, connection):
    """ Creates materialized view for each indexed partition found in the query.

    Args:
        asql_query (str): asql query
        library (ambry.Library):
        backend (PostgreSQLBackend):
        connection ():

    Returns:
        str: converted asql if it contains index query. If not, returns asql_query as is.
    """
    new_query = None
    if asql_query.strip().lower().startswith('index'):
        logger.debug(
            '_preprocess_postgres_index: create index query found.\n    asql query: {}'
            .format(asql_query))
        index = parse_index(asql_query)
        partition = library.partition(index.source)
        table = backend.install(connection, partition, materialize=True)
        # do not give index name to allow postgres care about name of the index. Postgres will do it better.
        new_query = 'CREATE INDEX my_ind ON {table} ({columns});'.format(
            table=table, columns=','.join(index.columns))

    logger.debug(
        '_preprocess_postgres_index: preprocess finished.\n    asql query: {}\n    new query: {}'
        .format(asql_query, new_query))
    return new_query or asql_query
