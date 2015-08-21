"""Column map to transform column names from the source files to the schema.
Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'


from sqlalchemy import Column as SAColumn, Integer, UniqueConstraint
from sqlalchemy import  Text, Binary, String, ForeignKey
import datetime
from sqlalchemy.orm import relationship

from ..util import Constant
from ..orm.column import Column
from ambry.etl.intuit import unknown


from . import Base, MutationDict, JSONEncodedObj



class SourceColumn(Base):

    __tablename__ = 'sourcecolumns'

    DATATYPE = Constant()
    DATATYPE.INT = int.__name__
    DATATYPE.FLOAT = float.__name__
    DATATYPE.STRING = str.__name__
    DATATYPE.DATE = datetime.date.__name__
    DATATYPE.TIME = datetime.time.__name__
    DATATYPE.DATETIME = datetime.datetime.__name__
    DATATYPE.UNKNOWN = unknown.__name__

    type_map = {
        DATATYPE.INT: int,
        DATATYPE.FLOAT: float,
        DATATYPE.STRING: str,
        DATATYPE.DATE: datetime.date,
        DATATYPE.TIME: datetime.time,
        DATATYPE.DATETIME: datetime.datetime,
        DATATYPE.DATETIME: unknown
    }

    column_type_map = { # FIXME The COlumn types should be harmonized with these types
        DATATYPE.INT: Column.DATATYPE_INTEGER,
        DATATYPE.FLOAT: Column.DATATYPE_FLOAT,
        DATATYPE.STRING: Column.DATATYPE_STR,
        DATATYPE.DATE: Column.DATATYPE_DATE,
        DATATYPE.TIME: Column.DATATYPE_TIME,
        DATATYPE.DATETIME: Column.DATATYPE_DATETIME,
        DATATYPE.UNKNOWN: Column.DATATYPE_STR
    }

    vid = SAColumn('sc_vid', String(21), primary_key=True)

    d_vid = SAColumn('sc_d_vid', String(13), ForeignKey('datasets.d_vid'), nullable=False)
    st_vid = SAColumn('sc_st_vid', String(17), ForeignKey('sourcetables.st_vid'), nullable=False)

    position = SAColumn('sc_position', Integer) # Integer position of column

    source_header = SAColumn('sc_source_header', Text) # Column header, After coalescing but before mangling.
    dest_header = SAColumn('sc_dest_header', Text)  # orig_header, mangled

    datatype = SAColumn('sc_datatype', Text) # Basic data type, usually intuited
    valuetype = SAColumn('sc_valuetype', Text) # Describes the meaning of the value: state, county, address, etc.

    start = SAColumn('sc_start', Integer) # For fixed width, the column starting position
    width = SAColumn('sc_width', Integer) # for Fixed width, the field width
    size = SAColumn('sc_size', Integer)  # Max size of the column values, after conversion to strings.

    summary = SAColumn('sc_summary', Text) # Short text description
    description = SAColumn('sc_description', Text) # Long text description

    value_labels = SAColumn('sc_value_labels', MutationDict.as_mutable(JSONEncodedObj))

    _next_column_number = None  # Set in next_config_number()

    __table_args__ = (
        UniqueConstraint('sc_st_vid','sc_source_header', name='_uc_sourcecolumns'),
    )

    @property
    def python_datatype(self):
        return self.type_map[self.datatype]

    @property
    def column_datatype(self):
        """Return the data type using the values defined for the schema"""
        return self.column_type_map[self.datatype]

    @staticmethod
    def mangle_name(name):
        """Mangles a column name to a standard form, removing illegal characters.

        :param name:
        :return:

        """
        import re

        try:
            return re.sub('_+', '_', re.sub('[^\w_]', '_', name).lower()).rstrip('_')
        except TypeError:
            raise TypeError(
                'Trying to mangle name with invalid type of: ' + str(type(name)))

    @property
    def row(self):
        from collections import OrderedDict

        # Use an Ordered Dict to make it friendly to creating CSV files.

        d = OrderedDict([('table',self.table.name)] + [(p.key,getattr(self, p.key)) for p in self.__mapper__.attrs
                         if p.key not in ['vid', 'st_vid', 'table','dataset', 'ds_id','d_vid', 'source', 'value_labels']])

        return d

    def update(self, **kwargs):

        if 'table' in kwargs:
            del kwargs['table'] # In source_schema.csv, this is the name of the table, not the object

        for k, v in list(kwargs.items()):
            if hasattr(self, k):

                if k == 'dest_header':
                    # Don't reset the dest header on updates.
                    if self.dest_header and self.dest_header != self.source_header:
                        continue

                setattr(self, k, v)


class SourceTable(Base):
    __tablename__ = 'sourcetables'

    vid = SAColumn('st_vid', String(22), primary_key=True)
    sequence_id = SAColumn('st_sequence_id', Integer, nullable=False)
    d_vid = SAColumn('st_d_vid', String(16), ForeignKey('datasets.d_vid'), nullable=False)
    name = SAColumn('st_name', String(50), nullable=False)

    columns = relationship(SourceColumn, backref='table', order_by="asc(SourceColumn.position)",
                           cascade="all, delete-orphan", lazy='joined')

    __table_args__ = (
        UniqueConstraint('st_d_vid','st_name', name='_uc_sourcetables'),
    )

    def column(self, source_header):

        for c in self.columns:
            if c.source_header == source_header:
                assert c.st_vid == self.vid
                return c
        else:
            return None

    def add_column(self, position, source_header, datatype, **kwargs):
        """
        Add a column to the source table.
        :param position: Integer position of the column
        :param source_header: Name fothe column, as it exists in the source file
        :param datatype: Python datatype ( str, int, float, None ) for the column
        :param kwargs:  Other source record args.
        :return:
        """
        from sqlalchemy.orm import object_session
        from ..identity import GeneralNumber2

        c = self.column(source_header)

        if c:
            c.update(
                position=position,
                datatype=datatype.__name__ if isinstance(datatype, type) else datatype,
                **kwargs )

        else:

            # Hacking an id number, since I don't want to create a new Identity ObjectNUmber type

            c = SourceColumn(
            vid = str(GeneralNumber2('C', self.d_vid, self.sequence_id, int(position))),
            position=position,
            st_vid=self.vid,
            d_vid=self.d_vid,
            datatype=datatype.__name__ if isinstance(datatype, type) else datatype,
            source_header=source_header,
            **kwargs)

            self.columns.append(c)

        return c

    @property
    def column_map(self):
        return { c.source_header: c.dest_header for c in self.columns }

    @property
    def column_index_map(self):
        return {c.source_header: c.position for c in self.columns}

    @property
    def headers(self):
        return [c.source_header for c in self.columns]

    @property
    def widths(self):
        widths =  [ c.width for c in self.columns ]
        if not  all( bool(e) for e in widths ):
            from ambry.dbexceptions import ConfigurationError
            raise ConfigurationError("The widths array for source table {} has zero or null entries ".format(self.name))

        widths = [int(w) for w in widths]

        return widths

    def __str__(self):
        from tabulate import tabulate

        headers = "Pos Source_Header Dest_Header Datatype ".split()
        rows = [(c.position, c.source_header, c.dest_header, c.datatype) for c in self.columns]

        return ('Source Table: {}\n'.format(self.name)) + tabulate(rows, headers)
