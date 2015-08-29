# -*- coding: utf-8 -*-
import logging

from sqlalchemy import Table, MetaData
from sqlalchemy.sql.expression import text

from ambry.util import get_logger

logger = get_logger(__name__, level=logging.DEBUG, propagate=False)


def add_partition(connection, partition):
    FOREIGN_SERVER_NAME = 'partition_server'
    _create_if_not_exists(connection, FOREIGN_SERVER_NAME)

    columns = ['{} {}'.format(c.name, c.type.compile()) for c in partition.table.columns]
    query = """
        CREATE FOREIGN TABLE {table_name} (
            {columns}
        ) server {server_name} options (
            filename '{file_name}'
        );
    """.format(table_name=_table_name(partition), columns=',\n'.join(columns),
               server_name=FOREIGN_SERVER_NAME, file_name=partition.datafile.syspath)
    logging.debug('Create foreign table for {} partition. Query:\n{}.'.format(partition.vid, query))
    connection.execute(query)


def _as_orm(connection, partition):
    """ Returns sqlalchemy model for partition rows.

    Example:
        PartitionRow = _as_orm(connection, partition)
        print session.query(PartitionRow).all()

    Returns:
        FIXME:
    """

    # FIXME: That solution is not documented by multicorn. Try documented solution again.

    table_name = _table_name(partition)
    metadata = MetaData(bind=connection.engine)
    PartitionRow = Table(table_name, metadata, *partition.table.columns)
    return PartitionRow


def _server_exists(connection, server_name):
    """ Returns True is foreign server with given name exists. Otherwise returns False. """
    query = text("""
        SELECT 1 FROM pg_foreign_server WHERE srvname=:server_name;
    """)
    return connection.execute(query, server_name=server_name).fetchall() == [(1,)]


def _create_if_not_exists(connection, server_name):
    """ Creates foreign server if it does not exist. """
    if not _server_exists(connection, server_name):
        logging.info('Create {} foreign server because it does not exist.'.format(server_name))
        query = """
            CREATE SERVER {} FOREIGN DATA WRAPPER multicorn
            options (
                wrapper 'ambryfdw.PartitionMsgpackForeignDataWrapper'
            );
        """.format(server_name)
        connection.execute(query)
    else:
        logging.debug('{} foreign server already exists. Do nothing.'.format(server_name))


def _table_name(partition):
    """ Returns foreign table name for the given partition. """
    # p_{vid}_ft stands for partition_vid_foreign_table
    # FIXME: it seems prefix + partition.table.name is better choice for foreign table name.
    return 'p_{vid}_ft'.format(vid=partition.vid)
