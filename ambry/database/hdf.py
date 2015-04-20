
"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""

from ..partitions import Partitions
from . import DatabaseInterface
from .inserter import InserterInterface
import tables
from ..orm import Column


class ValueInserter(InserterInterface):

    """Inserts arrays of values into  database table."""

    def __init__(
            self,
            path,
            bundle,
            partition,
            table=None,
            header=None,
            delimiter='|',
            escapechar='\\',
            encoding='utf-8',
            write_header=False,
            buffer_size=2 *
            1024 *
            1024):

        pass

    def insert(self, values):

        if self._writer is None:
            self._init_writer(values)

        try:
            self._inserter(values)

        except (KeyboardInterrupt, SystemExit):
            self.close()
            self.delete()
            raise
        except Exception as e:
            self.close()
            self.delete()
            raise

        return True

    def close(self):
        if self._f and not self._f.closed:
            self._f.flush()
            self._f.close()

    def delete(self):
        import os
        if os.path.exists(self.path):
            os.remove(self.path)

    def __enter__(self):

        self.partition.set_state(Partitions.STATE.BUILDING)
        return self

    def __exit__(self, type_, value, traceback):

        if type_ is not None:
            self.bundle.error("Got Exception: " + str(value))
            self.partition.set_state(Partitions.STATE.ERROR)
            return False

        self.partition.set_state(Partitions.STATE.BUILT)

        self.close()


class HdfDb(DatabaseInterface):

    EXTENSION = '.h5'

    types = {
        # Sqlalchemy, Python, Sql,
        Column.DATATYPE_TEXT: tables.StringCol,
        Column.DATATYPE_VARCHAR: tables.StringCol,
        Column.DATATYPE_CHAR: tables.StringCol,
        Column.DATATYPE_INTEGER: tables.Int32Col,
        Column.DATATYPE_INTEGER64: tables.Int64Col,
        Column.DATATYPE_REAL: tables.Float32Col,
        Column.DATATYPE_FLOAT: tables.Float32Col,
        Column.DATATYPE_NUMERIC: tables.Float32Col,
        Column.DATATYPE_DATE: tables.Time32Col,
        Column.DATATYPE_TIME: tables.Time64Col,
        Column.DATATYPE_TIMESTAMP: tables.Time64Col,
        Column.DATATYPE_DATETIME: tables.Time64Col,

        Column.DATATYPE_POINT: tables.StringCol,
        Column.DATATYPE_LINESTRING: tables.StringCol,
        Column.DATATYPE_POLYGON: tables.StringCol,
        Column.DATATYPE_MULTIPOLYGON: tables.StringCol,
        Column.DATATYPE_GEOMETRY: tables.StringCol,
        Column.DATATYPE_BLOB: tables.StringCol,
    }

    def __init__(self, bundle, partition, base_path, **kwargs):
        """"""

        self.bundle = bundle
        self.partition = partition

    @classmethod
    def declare_table(cls, table):

        desc = {}

        for c in table.columns:

            if c.type_is_text():
                width = c.width if c.width else 100
                t = cls.types[c.datatype](width, pos=c.sequence_id)
            else:
                t = cls.types[c.datatype](pos=c.sequence_id)

            desc[c.vid] = t

        return desc

    @property
    def path(self):
        return self.partition.path + self.EXTENSION

    def exists(self):
        import os
        return os.path.exists(self.path)

    def is_empty(self):
        return False

    def create(self):
        pass  # Created in the inserter

    def delete(self):
        import os
        if os.path.exists(self.path):
            os.remove(self.path)

    def inserter(self, header=None, skip_header=False, **kwargs):

        if not skip_header and header is None and self.partition.table is not None:
            header = [c.name for c in self.partition.table.columns]

        return ValueInserter(
            self.path,
            self.bundle,
            self.partition,
            header=header,
            **kwargs)

    def close(self):
        pass
