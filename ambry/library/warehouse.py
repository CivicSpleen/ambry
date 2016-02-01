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
# import logging
# logger = get_logger(__name__, level=logging.DEBUG, propagate=False)


class Warehouse(object):
    """ Provides SQL access to datasets in the library, allowing users to issue SQL queries, either as SQL
or via SQLAlchemy, to return datasets.
    """

    def __init__(self, library, dsn=None):

        self._library = library

        if not dsn:
            # Use library database.
            dsn = library.database.dsn

        # Initialize appropriate backend.
        if dsn.startswith('sqlite:'):
            from ambry.mprlib.backends.sqlite import SQLiteBackend
            logger.debug('Initializing sqlite warehouse.')
            self._backend = SQLiteBackend(library, dsn)

        elif dsn.startswith('postgresql'):
            from ambry.mprlib.backends.postgresql import PostgreSQLBackend
            logger.debug('Initializing postgres warehouse.')
            self._backend = PostgreSQLBackend(library, dsn)

        else:
            raise Exception(
                'Do not know how to handle {} dsn.'
                .format(dsn))

        self._warehouse_dsn = dsn

    @property
    def dsn(self):
        return self._warehouse_dsn

    def clean(self):
        """Remove all of the tables and data from the warehouse"""
        connection = self._backend._get_connection()
        self._backend.clean(connection)

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
        return self._backend.query(connection, query)

    def install(self, ref):
        """ Finds partition by reference and installs it to warehouse db.

        Args:
            ref (str): id, vid (versioned id), name or vname (versioned name) of the partition.

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
            ref (str): id, vid, name or vname of the partition.

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
