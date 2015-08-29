# -*- coding: utf-8 -*-
import msgpack

from sqlalchemy import Table as SATable, MetaData

from ambry.etl.partition import PartitionMsgpackDataFileReader

# TODO: Do not load all records to memory: http://www.drdobbs.com/database/query-anything-with-sqlite/202802959?pgno=3


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
        self.data = self._read_data(self.filename)

    def _read_data(self, filename):
        data = []
        with open(filename) as stream:
            unpacker = msgpack.Unpacker(stream, object_hook=PartitionMsgpackDataFileReader.decode_obj)
            header = None

            for row in unpacker:
                assert isinstance(row, (tuple, list)), row

                if not header:
                    header = row
                    continue
                data.append(row)
        return data

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

    def Filter(self, *args):
        self.pos = 0

    def Eof(self):
        return self.pos >= len(self.table.data)

    def Rowid(self):
        return self.table.data[self.pos][0]

    def Column(self, col):
        return self.table.data[self.pos][1 + col]

    def Next(self):
        self.pos += 1

    def Close(self):
        pass


def add_partition(connection, partition):
    """ Creates virtual table for partition.

    Args:
        connection (apsw.Connection):
        partition (ambry.orm.Partiton):

    """

    # register module. FIXME: Do not create module for each partition. Create 1 module for all partitions.
    module_name = 'mod_{}'.format(partition.vid)
    connection.createmodule(module_name, get_module_class(partition)())

    # create virtual table.
    cursor = connection.cursor()
    cursor.execute('CREATE VIRTUAL TABLE {} using {}'.format(_table_name(partition), module_name))


def _as_orm(connection, partition):
    """ Returns sqlalchemy model for partition rows.

    Example:
        PartitionRow = _as_orm(connection, partition)
        print session.query(PartitionRow).all()

    Returns:
        FIXME:
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
    # p_{vid}_ft stands for partition_vid_foreign_table
    # FIXME: it seems prefix + partition.table.name is better choice for virtual table name.
    return 'p_{vid}_vt'.format(vid=partition.vid)
