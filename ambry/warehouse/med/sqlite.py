# -*- coding: utf-8 -*-
import msgpack

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
        connection (): FIXME:
        partition (): FIXME:
    """

    # register module.
    module_name = 'mod_{}'.format(partition.vid)
    connection.createmodule(module_name, get_module_class(partition)())

    # create virtual table.
    cursor = connection.cursor()
    cursor.execute('CREATE VIRTUAL TABLE {} using {}'.format(partition.vid, module_name))
