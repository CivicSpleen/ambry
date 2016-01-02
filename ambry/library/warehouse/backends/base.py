import sqlparse

from ambry.identity import ObjectNumber, NotObjectNumberError, TableNumber
from ambry.util import get_logger

from ..exceptions import MissingTableError

logger = get_logger(__name__)

# debug logging
#
# import logging
# logger = get_logger(__name__, level=logging.DEBUG, propagate=False)


class DatabaseBackend(object):
    """ Base class for warehouse database engines. """

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

    def query(self, connection, query, fetch=True):
        """ Creates virtual tables for all partitions found in the query and executes query.

        Args:
            query (str): sql query
            fetch (bool): fetch result from database if True, do not fetch overwise.

        """
        statements = sqlparse.parse(sqlparse.format(query, strip_comments=True))

        # install all partitions and replace table names in the query.
        #
        logger.debug('Findind and installing all partitions from query. \n    query: {}'.format(query))
        new_query = []
        for statement in statements:
            logger.debug(
                'Searching statement for partition ref.\n    statement: {}'.format(statement.to_unicode()))
            table_names = _get_table_names1(statement)
            new_statement = statement.to_unicode()

            if table_names:
                for ref in table_names:
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
                            'Searching partition table in the warehouse database.\n    partition: {}'
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
                        new_statement = new_statement.replace(ref, warehouse_table)

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
                        new_statement = new_statement.replace(table.vid, self.get_view_name(table))
            new_query.append(new_statement)

        new_query = '\n'.join(new_query)
        logger.debug(
            'Executing updated query after partition install.'
            '\n    query before update: {}\n    query to execute (updated query): {}'
            .format(query, new_query))
        return self._execute(connection, new_query, fetch=fetch)

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


def _get_table_names(statement):
    """ Returns table names found in the query.

    Args:
        statement (sqlparse.sql.Statement): parsed by sqlparse sql statement.

    Returns:
        list of str
    """
    tables = []
    collect_table = False
    for token in statement.tokens:
        if token.value.lower() == 'from' or token.value.lower().endswith(' join'):
            collect_table = True
            continue
        if isinstance(token, sqlparse.sql.Identifier) and collect_table:
            tables.append(token.get_real_name())
            collect_table = False
    logger.debug(
        'List of table names found in the statement.\n    statement: {}\n    tables: {}\n'
        .format(statement.to_unicode(), tables))
    return tables


def _get_table_names1(statement):
    # Simplified version - is more appropriate for ambry queryes
    parts = statement.to_unicode().split()
    tables = set()
    for i, token in enumerate(parts):
        if token.lower() == 'from' or token.lower().endswith('join'):
            tables.add(parts[i + 1].rstrip(';'))
    return list(tables)
