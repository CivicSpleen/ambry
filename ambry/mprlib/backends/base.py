import sqlparse

from ambry.bundle.asql_parser import substitute_vids

from ambry.util import get_logger

from ..exceptions import BadSQLError

logger = get_logger(__name__)

# debug logging
#
# import logging
# logger = get_logger(__name__, level=logging.DEBUG, propagate=False)


class DatabaseBackend(object):
    """ Base class for mpr install/query implementations. """

    def __init__(self, library, dsn):
        self._library = library
        self._dsn = dsn

    def install(self, connection, partition, table_name=None, index_columns=None, materialize=False,
                logger=None):
        """ Installs partition's mpr to the database to allow to execute sql queries over mpr.

        Args:
            connection:
            partition (orm.Partition):
            materialize (boolean): if True, create generic table. If False create MED over mpr.

        Returns:
            str: name of the created table.

        """

        raise NotImplementedError

    def install_table(self, connection, table, logger = None):
        """ Installs all partitons of the table and create view with union of all partitons.

        Args:
            connection: connection to database who stores mpr data.
            table (orm.Table):
        """
        # first install all partitions of the table

        queries = []
        query_tmpl = 'SELECT * FROM {}'
        for partition in table.partitions:
            partition.localize()
            installed_name = self.install(connection, partition)
            queries.append(query_tmpl.format(installed_name))

        # now create view with union of all partitions.
        query = 'CREATE VIEW {} AS {} '.format( table.vid, '\nUNION ALL\n'.join(queries))
        logger.debug('Creating view for table.\n    table: {}\n    query: {}'.format(table.vid, query))
        self._execute(connection, query, fetch=False)

    def install_statement(self, connection, sql, logger = None):
        """Convert an SQL statement to use vids and install all of the partitions and tables"""

        sub_statement, tables, partitions = substitute_vids(self._library, sql)

        materialize = False
        return_null = False
        if sub_statement.lower().startswith('materialize'):
            materialize = True
            return_null = True # All of MATERIALIZE is handled here; nothing else ot execute.

            if  not partitions:
                raise BadSQLError("Failed to find partition in MATERIALIZE statement: '{}' ".format(sub_statement))

        for vid in partitions:
            partition = self._library.partition(vid)
            self.install(connection, partition, logger = logger, materialize=materialize)

        for vid in tables:
            table = self._library.table(vid)
            self.install_table(connection, table, logger = logger)

        if return_null:
            return '';
        else:
            return sub_statement

    def query(self, connection, query, fetch=True):
        """ Creates virtual tables for all partitions found in the query and executes query.

        Args:
            query (str): sql query
            fetch (bool): fetch result from database if True, do not fetch overwise.

        """

        self.install_module(connection)

        statements = sqlparse.parse(sqlparse.format(query, strip_comments=True))

        # install all partitions and replace table names in the query.
        #
        logger.debug('Finding and installing all partitions from query. \n    query: {}'.format(query))
        new_query = []

        if len(statements) > 1:
            raise BadSQLError("Can only query a single statement")

        if len(statements) == 0:
            raise BadSQLError("DIdn't get any statements in '{}'".format(query))

        statement = statements[0]

        logger.debug( 'Searching statement for partition ref.\n    statement: {}'.format(statement.to_unicode()))

        statement = self.install_statement(connection, statement.to_unicode())

        logger.debug(
            'Executing updated query after partition install.'
            '\n    query before update: {}\n    query to execute (updated query): {}'
            .format(statement, new_query))

        return self._execute(connection, statement, fetch=fetch)

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

    def _get_mpr_table(self, connection, partition):
        """ Finds and returns mpr table in the db represented by given connection.

        Args:
            connection: connection to db where to look for partition table.
            partition (orm.Partition):

        Raises:
            MissingTableError: if database does not have partition table.

        Returns:
            str: database table storing partition data.

        """
        raise NotImplementedError

    def _get_mpr_view(self, connection, table):
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



def _get_table_names(statement):
    """ Returns table names found in the query.

    NOTE. This routine would use the sqlparse parse tree, but vnames don't parse very well.

    Args:
        statement (sqlparse.sql.Statement): parsed by sqlparse sql statement.

    Returns:
        list of str
    """

    parts = statement.to_unicode().split()

    tables = set()

    for i, token in enumerate(parts):
        if token.lower() == 'from' or token.lower().endswith('join'):
            tables.add(parts[i + 1].rstrip(';'))

    return list(tables)
