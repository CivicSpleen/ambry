import psycopg2

from ambry_sources.med import postgresql as postgres_med

from ambry.util import get_logger, parse_url_to_dict

from ambry.bundle.asql_parser import parse_index

from ..exceptions import MissingTableError, MissingViewError
from .base import DatabaseBackend

logger = get_logger(__name__)

# debug logging
#
# import logging
# logger = get_logger(__name__, level=logging.DEBUG, propagate=False)


class PostgreSQLBackend(DatabaseBackend):
    """ Warehouse backend for PostgreSQL database. """

    def sql_processors(self):
        return [_preprocess_postgres_index]

    def install(self, connection, partition, table_name=None, index_columns=None, materialize=False,
                logger=None):
        """ Creates FDW or materialize view for given partition.

        Args:
            connection: connection to postgresql
            partition (orm.Partition):
            materialize (boolean): if True, create read-only table. If False create virtual table.

        Returns:
            str: name of the created table.

        """

        partition.localize()

        self._add_partition(connection, partition)
        fdw_table = partition.vid
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

        final_table = view_table if materialize else fdw_table

        with connection.cursor() as cursor:
            view_q = "CREATE VIEW IF NOT EXISTS {} AS SELECT * FROM {} ".format(partition.vid, final_table)
            cursor.execute(view_q)
            cursor.execute('COMMIT;')

        return partition.vid


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
        query_tmpl = 'CREATE INDEX ON {table_name} ({column});'
        table_name = '{}_v'.format(partition.vid)
        for column in columns:
            query = query_tmpl.format(table_name=table_name, column=column)
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

    def _get_mpr_view(self, connection, table):
        """ Finds and returns table view name in the postgres db represented by given connection.

        Args:
            connection: connection to postgres db where to look for partition table.
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
        raise MissingViewError('postgres database does not have view for {} table.'
                               .format(table.vid))

    def _get_mpr_table(self, connection, partition):
        """ Returns name of the postgres table who stores mpr data.

        Args:
            connection: connection to postgres db who stores mpr data.
            partition (orm.Partition):

        Returns:
            str:

        Raises:
            MissingTableError: if partition table not found in the db.

        """
        # TODO: This is the first candidate for optimization. Add field to partition
        # with table name and update it while table creation.
        # Optimized version.
        #
        # return partition.mpr_table or raise exception

        # Not optimized version.
        #
        # first check either partition has materialized view.
        logger.debug(
            'Looking for materialized view of the partition.\n    partition: {}'.format(partition.name))
        foreign_table = partition.vid
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
        raise MissingTableError('postgres database does not have table for {} partition.'
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
            connection to postgres database who stores mpr data.

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
            connection: connection to postgres database who stores mpr data.
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
            else:
                cursor.execute('COMMIT;')

    @classmethod
    def _relation_exists(cls, connection, relation):
        """ Returns True if relation exists in the postgres db. Otherwise returns False.

        Args:
            connection: connection to postgres database who stores mpr data.
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


    def clean(self):
        raise NotImplementedError()


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
