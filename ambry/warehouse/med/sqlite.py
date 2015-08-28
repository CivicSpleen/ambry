# -*- coding: utf-8 -*-


def get_module_class(partition):
    """ Returns module class for the partition. """

    class Source:
        def Create(self, db, modulename, dbname, tablename, *args):
            columns_str = ','.join(["'%s'" % (x,) for x in partition.columns[1:]])
            schema = 'CREATE TABLE {}({})'.format(tablename, columns_str)
            return schema, Table(partition.columns, partition.data)
        Connect = Create
    return Source


class Table:
    """ Represents a table """
    def __init__(self, columns, data):
        self.columns = columns
        self.data = data

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
