# -*- coding: utf-8 -*-
from datetime import datetime, date

import msgpack
import gzip

from sqlalchemy import Table as SATable, MetaData

from apsw import MisuseError

from ambry.etl.partition import PartitionMsgpackDataFileReader

# Documents used to implement module and function:
# Module: http://apidoc.apsw.googlecode.com/hg/vtable.html
# Functions: http://www.drdobbs.com/database/query-anything-with-sqlite/202802959?pgno=3


def get_module_class(partition):
    """ Returns module class for the partition. """

    class Source:
        def Create(self, db, modulename, dbname, tablename, *args):
            columns_types = []
            column_names = []
            for i, c in enumerate(partition.table.columns):
                if i == 0:
                    # First column is already reserved for rowid. This is current release constraint
                    # and will be removed when I discover real partitions data more deeply.
                    continue
                columns_types.append('{} {}'.format(c.name, c.type.compile()))
                column_names.append(c.name)
            columns_types_str = ',\n'.join(columns_types)
            schema = 'CREATE TABLE {}({})'.format(tablename, columns_types_str)
            return schema, Table(column_names, partition.datafile.syspath)
        Connect = Create
    return Source


class Table:
    """ Represents a table """
    def __init__(self, columns, filename):
        self.columns = columns
        self.filename = filename

    def BestIndex(self, *args):
        return None

    def Open(self):
        return Cursor(self)

    def Disconnect(self):
        pass

    Destroy = Disconnect


class Cursor:
    """ Represents a cursor """
    def __init__(self, table):
        self.table = table
        self._current_row = None
        self._next_row = None
        self._f = open(table.filename, 'rb')
        self._msg_file = gzip.GzipFile(fileobj=self._f)
        self._unpacker = msgpack.Unpacker(
            self._msg_file, object_hook=PartitionMsgpackDataFileReader.decode_obj)
        self._header = next(self._unpacker)
        self._current_row = next(self._unpacker)

    def Filter(self, *args):
        pass

    def Eof(self):
        return self._current_row is None

    def Rowid(self):
        return self._current_row[0]

    def Column(self, col):
        value = self._current_row[1 + col]
        if isinstance(value, (date, datetime)):
            # Convert to ISO format.
            return value.isoformat()
        return value

    def Next(self):
        try:
            self._current_row = next(self._unpacker)
            assert isinstance(self._current_row, (tuple, list)), self._current_row
        except StopIteration:
            self._current_row = None

    def Close(self):
        self._f.close()
        self._unpacker = None


def add_partition(connection, partition):
    """ Creates virtual table for partition.

    Args:
        connection (apsw.Connection):
        partition (ambry.orm.Partiton):

    """

    module_name = 'mod_partition'
    try:
        connection.createmodule(module_name, get_module_class(partition)())
    except MisuseError:
        # TODO: The best solution I've found to check for existance. Try again later,
        # because MisuseError might mean something else.
        pass

    # create virtual table.
    cursor = connection.cursor()
    cursor.execute('CREATE VIRTUAL TABLE {} using {};'.format(_table_name(partition), module_name))


def _as_orm(connection, partition):
    """ Returns sqlalchemy model for partition rows.

    Example:
        PartitionRow = _as_orm(connection, partition)
        print session.query(PartitionRow).all()

    Returns:
        Table:
    """
    # FIXME:
    raise NotImplementedError(
        'psqlite connection used by sqlalchemy does not see module created by apsw')
    table_name = _table_name(partition)
    metadata = MetaData(bind=connection.engine)
    PartitionRow = SATable(table_name, metadata, *partition.table.columns)
    return PartitionRow


def _table_name(partition):
    """ Returns virtual table name for the given partition. """
    return 'p_{vid}_vt'.format(vid=partition.vid)
