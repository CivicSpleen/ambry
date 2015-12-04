"""
Provides SQL access to datasets in the library, allowing users to issue SQL queries, either as SQL
or via Sqlalchemy, to return datasets.

Example:
    import ambry
    l = ambry.get_library()
    for row in l.warehouse.query('SELECT * FROM <partition id or vid> ... '):
        print row
"""

import sqlparse

import six

import apsw

import psycopg2

from ambry_sources.med import sqlite as sqlite_med, postgresql as postgres_med

from ambry.util import get_logger, parse_url_to_dict
from ambry.identity import ObjectNumber, NotObjectNumberError, TableNumber


logger = get_logger(__name__)

# debug logging
#
# import logging
# logger = get_logger(__name__, level=logging.DEBUG, propagate=False)


class WarehouseError(Exception):
    """ Base class for all warehouse errors. """
    pass


class MissingTableError(WarehouseError):
    """ Raises if warehouse does not have table for the partition. """
    pass


class MissingViewError(WarehouseError):
    """ Raises if warehouse does not have view associated with the table. """
    pass


class Warehouse(object):
    """ Provides SQL access to datasets in the library, allowing users to issue SQL queries, either as SQL
or via SQLAlchemy, to return datasets.
    """

    def __init__(self, library):
        # If keep_connection is true, do not close the connection until close method call.
        self._library = library

        warehouse_dsn = library.config.library.get('warehouse')
        if not warehouse_dsn:
            warehouse_dsn = library.config.library.database
        if warehouse_dsn.startswith('sqlite:'):
            logger.debug('Initializing sqlite warehouse.')
            self._backend = SQLiteWrapper(library, warehouse_dsn)
        elif warehouse_dsn.startswith('postgresql'):
            logger.debug('Initializing postgres warehouse.')
            self._backend = PostgreSQLWrapper(library, warehouse_dsn)
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
            'Executing warehouse query using {} backend. \n    query: {}'
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


class DatabaseWrapper(object):
    """ Base class for warehouse databases engines. """

    def __init__(self, library, dsn):
        self._library = library
        self._dsn = dsn

    def install(self, connection, partition, materialize=False):
        """ Installs partition's mpr to the database to allow to execute sql queries over mpr.

        Args:
            connection:
            partition (orm.Partition):
            materialize (boolean): if True, create generic table. If False create MED over mpr.

        Returns:
            str: name of the created table.

        """
        raise NotImplementedError

    def install_table(self, connection, table):
        """ Installs all partitons of the table and create view with union of all partitons.

        Args:
            connection: connection to database who stores warehouse data.
            table (orm.Table):
        """
        # first install all partitions of the table
        queries = []
        query_tmpl = 'SELECT * FROM {}'
        for partition in table.partitions:
            installed_name = self.install(connection, partition)
            queries.append(query_tmpl.format(installed_name))

        # now create view with union of all partitions.
        query = 'CREATE VIEW {} AS {} '.format(
            self.get_view_name(table), '\nUNION ALL\n'.join(queries))
        logger.debug('Creating view for table.\n    table: {}\n    query: {}'.format(table.vid, query))
        self._execute(connection, query, fetch=False)

    def query(self, connection, query):
        """ Creates virtual tables for all partitions found in the query and executes query.

        Args:
            query (str): sql query

        """
        statements = sqlparse.parse(query)

        # install all partitions and replace table names in the query.
        #
        logger.debug('Findind and installing all partitions from query. \n    query: {}'.format(query))
        new_query = []
        for statement in statements:
            logger.debug(
                'Searching statement for partition ref.\n    statement: {}'.format(statement.to_unicode()))
            ref = _get_table_name(statement)
            if ref:
                partition = None
                table = None
                try:
                    obj_number = ObjectNumber.parse(ref)
                    if isinstance(obj_number, TableNumber):
                        table = self._library.table(ref)
                    else:
                        # Do not care about other object numbers. Assume partition.
                        raise NotObjectNumberError
                except NotObjectNumberError:
                    # assume partition
                    partition = self._library.partition(ref)

                if partition:
                    logger.debug(
                        'Searching partition table in the warehouse database. \n    partition: {}'
                        .format(partition.name))
                    try:
                        # try to use existing fdw or materialized view.
                        warehouse_table = self._get_warehouse_table(connection, partition)
                        logger.debug(
                            'Partition already installed. \n    partition: {}'.format(partition.vid))
                    except MissingTableError:
                        # FDW is not created, create.
                        logger.debug(
                            'Partition is not installed. Install now. \n    partition: {}'
                            .format(partition.vid))
                        warehouse_table = self.install(connection, partition)
                    new_query.append(statement.to_unicode().replace(ref, warehouse_table))

                if table:
                    logger.debug(
                        'Searching table view in the warehouse database. \n    table: {}'
                        .format(table.vid))
                    try:
                        # try to use existing fdw or materialized view.
                        warehouse_table = self._get_warehouse_view(connection, table)
                        logger.debug(
                            'Table view already exists. \n    table: {}'.format(table.vid))
                    except MissingTableError:
                        # View is not created, create.
                        logger.debug(
                            'Table view does not exist. Create now. \n    table: {}'.format(table.vid))
                        self.install_table(connection, table)
                    new_query.append(statement.to_unicode().replace(table.vid, self.get_view_name(table)))
            else:
                new_query.append(statement.to_unicode())

        new_query = '\n'.join(new_query)
        logger.debug(
            'Executing updated query after partition install.'
            '\n    query before update: {}\n    query to execute (updated query): {}'
            .format(query, new_query))
        return self._execute(connection, new_query)

    def index(self, connection, partition, columns):
        """ Create an index on the columns.

        Args:
            connection:
            partition (orm.Partition):
            columns (list of str): names of the columns needed indexes.

        """
        raise NotImplementedError

    def close(self):
        """ Closes connection to database. """
        raise NotImplementedError

    def _get_warehouse_table(self, connection, partition):
        """ Finds and returns partition table in the db represented by given connection.

        Args:
            connection: connection to db where to look for partition table.
            partition (orm.Partition):

        Raises:
            MissingTableError: if database does not have partition table.

        Returns:
            str: database table storing partition data.

        """
        raise NotImplementedError

    def _get_warehouse_view(self, connection, table):
        """ Finds and returns table view name in the db represented by given connection.

        Args:
            connection: connection to db where to look for partition table.
            table (orm.Table):

        Raises:
            MissingViewError: if database does not have partition table.

        Returns:
            str: database table storing partition data.

        """
        raise NotImplementedError

    def _execute(self, connection, query, fetch=True):
        """ Executes sql query using given connection.

        Args:
            connection: connection to db
            query (str): sql query.
            fetch (boolean, optional): if True, fetch query result and return it. If False, do not fetch.

        Returns:
            iterable: result of the query.

        """
        raise NotImplementedError

    def _get_connection(self):
        """ Returns connection to database. """
        raise NotImplementedError


class PostgreSQLWrapper(DatabaseWrapper):
    """ Warehouse wrapper over PostgreSQL database. """

    def install(self, connection, partition, materialize=False):
        """ Creates FDW or materialize view for given partition.

        Args:
            connection: connection to postgresql
            partition (orm.Partition):
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
                if view_exists:
                    logger.debug(
                        'Materialized view of the partition already exists.\n    partition: {}, view: {}'
                        .format(partition.name, view_table))
                else:
                    query = 'CREATE MATERIALIZED VIEW {} AS SELECT * FROM {};'\
                        .format(view_table, fdw_table)
                    logger.debug(
                        'Creating new materialized view of the partition.'
                        '\n    partition: {}, view: {}, query: {}'
                        .format(partition.name, view_table, query))
                    cursor.execute(query)
                    cursor.execute('COMMIT;')
        return view_table if materialize else fdw_table

    @staticmethod
    def get_view_name(table):
        """ Returns view name of the table.

        Args:
            table (orm.Table):

        Returns:
            str:

        """
        return 'partitions.{}'.format(table.vid)

    def index(self, connection, partition, columns):
        """ Create an index on the columns.

        Args:
            connection:
            partition (orm.Partition):
            columns (list of str):

        """
        query_tmpl = '''
            CREATE INDEX {index_name} ON {table_name} ({column});
        '''
        table_name = '{}_v'.format(postgres_med.table_name(partition.vid))
        for column in columns:
            query = query_tmpl.format(
                index_name='{}_{}_i'.format(partition.vid, column), table_name=table_name,
                column=column)
            logger.debug('Creating postgres index.\n    column: {}, query: {}'.format(column, query))
            with connection.cursor() as cursor:
                cursor.execute(query)
                cursor.execute('COMMIT;')

    def close(self):
        """ Closes connection to database. """
        if getattr(self, '_connection', None):
            logger.debug('Closing postgresql connection.')
            self._connection.close()
            self._connection = None
        if getattr(self, '_engine', None):
            self._engine.dispose()

    def _get_warehouse_view(self, connection, table):
        """ Finds and returns table view name in the db represented by given connection.

        Args:
            connection: connection to db where to look for partition table.
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
                'View of the table found.\n    table: {}, view: {}'
                .format(table.vid, view))
            return view
        raise MissingViewError('postgres database of the warehouse does not have view for {} table.'
                               .format(table.vid))

    def _get_warehouse_table(self, connection, partition):
        """ Returns name of the table who stores partition data.

        Args:
            connection: connection to postgres db who stores warehouse data.
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
        # first check either partition has materialized view.
        logger.debug(
            'Looking for materialized view of the partition.\n    partition: {}'.format(partition.name))
        foreign_table = postgres_med.table_name(partition.vid)
        view_table = '{}_v'.format(foreign_table)
        view_exists = self._relation_exists(connection, view_table)
        if view_exists:
            logger.debug(
                'Materialized view of the partition found.\n    partition: {}, view: {}'
                .format(partition.name, view_table))
            return view_table

        # now check for fdw/virtual table
        logger.debug(
            'Looking for foreign table of the partition.\n    partition: {}'.format(partition.name))
        foreign_exists = self._relation_exists(connection, foreign_table)
        if foreign_exists:
            logger.debug(
                'Foreign table of the partition found.\n    partition: {}, foreign table: {}'
                .format(partition.name, foreign_table))
            return foreign_table
        raise MissingTableError('warehouse postgres database does not have table for {} partition.'
                                .format(partition.vid))

    def _add_partition(self, connection, partition):
        """ Creates FDW for the partition.

        Args:
            connection:
            partition (orm.Partition):

        """
        logger.debug('Creating foreign table for partition.\n    partition: {}'.format(partition.name))
        with connection.cursor() as cursor:
            postgres_med.add_partition(cursor, partition.datafile, partition.vid)

    def _get_connection(self):
        """ Returns connection to the postgres database.

        Returns:
            connection to postgres database who stores warehouse data.

        """
        if not getattr(self, '_connection', None):
            logger.debug(
                'Creating new connection.\n   dsn: {}'
                .format(self._dsn))

            d = parse_url_to_dict(self._dsn)
            self._connection = psycopg2.connect(
                database=d['path'].strip('/'), user=d['username'], password=d['password'],
                port=d['port'], host=d['hostname'])
            # It takes some time to find the way how to get raw connection from sqlalchemy. So,
            # I leave the commented code.
            #
            # self._engine = create_engine(self._dsn)
            # self._connection = self._engine.raw_connection()
            #
        return self._connection

    def _execute(self, connection, query, fetch=True):
        """ Executes given query and returns result.

        Args:
            connection: connection to postgres database who stores warehouse data.
            query (str): sql query
            fetch (boolean, optional): if True, fetch query result and return it. If False, do not fetch.

        Returns:
            iterable with query result or None if fetch is False.

        """
        # execute query
        with connection.cursor() as cursor:
            cursor.execute(query)
            if fetch:
                return cursor.fetchall()

    def _relation_exists(self, connection, relation):
        """ Returns True if relation exists in the postgres db. Otherwise returns False.

        Args:
            connection: connection to postgres database who stores warehouse data.
            relation (str): name of the table, view or materialized view.

        Note:
            relation means table, view or materialized view here.

        Returns:
            boolean: True if relation exists, False otherwise.

        """
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
            ref (str): id, vid, name or versioned name of the partition.
            materialize (boolean): if True, create read-only table. If False create virtual table.

        Returns:
            str: name of the created table.

        """
        self._add_partition(connection, partition)
        virtual_table = sqlite_med.table_name(partition.vid)
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
        """ Finds and returns table view name in the db represented by given connection.

        Args:
            connection: connection to db where to look for partition table.
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
        table_exists = self._relation_exists(connection, virtual_table)
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
    def _get_create_query(partition, tablename):
        """ Creates and returns `create table ...` query for given mprows.

        Args:
            partition (orm.Partition):
            tablename (str): name of the table in the return create query.

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
        for column in sorted(partition.datafile.reader.columns, key=lambda x: x['pos']):
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


def _get_table_name(statement):
    """ Finds first identifier in the statement and returns it.

    Args:
        statement (sqlparse.sql.Statement): parsed by sqlparse sql statement.

    Returns:
        unicode or None if table not found
    """
    for i, token in enumerate(statement.tokens):
        if token.value.lower() == 'from':
            # check rest for table name
            for elem in statement.tokens[i:]:
                if isinstance(elem, sqlparse.sql.Identifier):
                    logger.debug(
                        'Partition table name found in the statement.\n    table_name: {}, statement: {}'
                        .format(elem.get_real_name(), statement.to_unicode()))
                    return elem.get_real_name()
    logger.debug(
        'Partition table not found in the statement.\n    statement: {}'
        .format(statement.to_unicode()))
