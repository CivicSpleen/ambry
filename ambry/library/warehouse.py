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

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ambry.identity import ObjectNumber, NotObjectNumberError, TableNumber
from ambry.orm import Table
from ambry.util import get_logger

logger = get_logger(__name__)

# debug logging
#
import logging
logger = get_logger(__name__, level=logging.ERROR, propagate=False)

class Warehouse(object):
    """ Provides SQL access to datasets in the library, allowing users to issue SQL queries, either as SQL
or via SQLAlchemy, to return datasets.
    """

    def __init__(self, library, dsn=None):
        from ambry.library import Library
        assert isinstance(library, Library)

        self._library = library

        if not dsn:
            # Use library database.
            dsn = library.database.dsn

        # Initialize appropriate backend.
        if dsn.startswith('sqlite:'):
            from ambry.mprlib.backends.sqlite import SQLiteBackend
            logger.debug('Initializing sqlite warehouse.')
            self._backend = SQLiteBackend(library, dsn)

        elif dsn.startswith('postgres'):
            try:
                from ambry.mprlib.backends.postgresql import PostgreSQLBackend
                logger.debug('Initializing postgres warehouse.')
                self._backend = PostgreSQLBackend(library, dsn)
            except ImportError as e:
                from ambry.mprlib.backends.sqlite import SQLiteBackend
                from ambry.util import set_url_part, select_from_url
                dsn = "sqlite:///{}/{}".format(self._library.filesystem.build('warehouses'),
                                               select_from_url(dsn,'path').strip('/')+".db")
                logging.error("Failed to import required modules ({})for Postgres warehouse. Using Sqlite dsn={}"
                              .format(e, dsn))
                self._backend = SQLiteBackend(library, dsn)

        else:
            raise Exception('Do not know how to handle {} dsn.'.format(dsn))

        self._warehouse_dsn = dsn

    @property
    def dsn(self):
        return self._warehouse_dsn

    @property
    def connection(self):
        return self._backend._get_connection()

    def clean(self):
        """Remove all of the tables and data from the warehouse"""
        connection = self._backend._get_connection()
        self._backend.clean(connection)

    def list(self):
        """List the tables in the database"""
        connection = self._backend._get_connection()
        return list(self._backend.list(connection))

    @property
    def engine(self):
        """Return A Sqlalchemy engine"""
        return create_engine(self._warehouse_dsn)

    def query(self, query=''):
        """ Creates virtual tables for all partitions found in the query and executes query.

        Args:
            query (str): sql query

        """

        if not query:
            engine = create_engine(self._warehouse_dsn)
            session = sessionmaker(bind=engine)
            return session().query(Table)

        logger.debug(
            'Executing warehouse query using {} backend.\n    query: {}'
            .format(self._backend._dsn, query))

        connection = self._backend._get_connection()

        return self._backend.query(connection, query, fetch=False)

    def install(self, ref, table_name=None, index_columns=None):
        """ Finds partition by reference and installs it to warehouse db.

        Args:
            ref (str): id, vid (versioned id), name or vname (versioned name) of the partition.

        """
        try:
            obj_number = ObjectNumber.parse(ref)
            if isinstance(obj_number, TableNumber):
                table = self._library.table(ref)
                connection = self._backend._get_connection()
                return self._backend.install_table(connection, table)
            else:
                # assume partition
                raise NotObjectNumberError

        except NotObjectNumberError:
            # assume partition.
            partition = self._library.partition(ref)
            connection = self._backend._get_connection()

            return self._backend.install(connection, partition, table_name=table_name, index_columns=index_columns)

    def materialize(self, ref, table_name=None, index_columns=None):
        """ Creates materialized table for given partition reference.

        Args:
            ref (str): id, vid, name or vname of the partition.

        Returns:
            str: name of the partition table in the database.

        """
        from ambry.library import Library
        assert isinstance(self._library, Library )

        logger.debug('Materializing warehouse partition.\n    partition: {}'.format(ref))
        partition = self._library.partition(ref)

        connection = self._backend._get_connection()
        return self._backend.install(connection, partition, table_name = table_name,
                                     index_columns=index_columns, materialize=True)

    def index(self, ref, columns):
        """ Create an index on the columns.

        Args:
            ref (str): id, vid, name or versioned name of the partition.
            columns (list of str): names of the columns needed indexes.

        """
        logger.debug('Creating index for partition.\n    ref: {}, columns: {}'.format(ref, columns))
        connection = self._backend._get_connection()
        partition = self._library.partition(ref)
        self._backend.index(connection, partition, columns)

    def parse_sql(self, asql):
        """ Executes all sql statements from asql.

        Args:
            library (library.Library):
            asql (str): unified sql query - see https://github.com/CivicKnowledge/ambry/issues/140 for details.
        """
        import sqlparse

        statements = sqlparse.parse(sqlparse.format(asql, strip_comments=True))
        parsed_statements = []
        for statement in statements:

            statement_str = statement.to_unicode().strip()

            for preprocessor in self._backend.sql_processors():
                statement_str = preprocessor(statement_str, self._library, self._backend, self.connection)

            parsed_statements.append(statement_str)

        return parsed_statements

    def execute_sql(self, asql, logger = None):
        """ Executes all sql statements from asql.

        Args:
            library (library.Library):
            asql (str): unified sql query - see https://github.com/CivicKnowledge/ambry/issues/140 for details.
        """
        import sqlparse
        from ambry.mprlib.exceptions import BadSQLError
        from ambry.bundle.asql_parser import find_indexable_materializable

        if not logger:
            logger = self._library.logger

        statements = sqlparse.parse(sqlparse.format(asql, strip_comments=True))

        for parsed_statement in statements:

            statement, tables, install, materialize, indexes = \
                find_indexable_materializable(parsed_statement, self._library)

            logger.info("Process statement: {}".format(statement[:40]))

            for vid in install:
                logger.info('    Install {}'.format(vid))
                self.install(vid)

            for vid in materialize:
                logger.info('    Materialize {}'.format(vid))
                self.materialize(vid)

            for vid, columns in indexes:
                logger.info('    Index {}'.format(vid))
                try:
                    self.index(vid, columns)
                except Exception as e:
                    logger.error('    Failed to index {}; {}'.format(vid, e))

            if statement.lower().startswith('create'):
                logger.info('    Create {}'.format(statement))
                connection = self._backend._get_connection()
                cursor = self._backend.query(connection, statement, fetch=False)
                cursor.close()

            if statement.lower().startswith('select'):
                logger.info('Run query {}'.format(statement))
                connection = self._backend._get_connection()

                return self._backend.query(connection, statement, fetch=False)


    def close(self):
        """ Closes warehouse database. """
        self._backend.close()
