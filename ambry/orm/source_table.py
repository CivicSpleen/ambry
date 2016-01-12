"""Column map to transform column names from the source files to the schema.
Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'

import datetime

from sqlalchemy import Column as SAColumn, Integer, UniqueConstraint, Boolean
from sqlalchemy import Text, String, ForeignKey
from sqlalchemy.orm import relationship

import six

from ambry_sources.intuit import unknown

from ..util import Constant
from ..orm.column import Column

from . import Base, MutationDict, JSONEncodedObj


class SourceColumn(Base):

    __tablename__ = 'sourcecolumns'

    _parent_col = 'sc_st_vid'

    DATATYPE = Constant()
    DATATYPE.INT = int.__name__
    DATATYPE.FLOAT = float.__name__
    DATATYPE.STRING = six.binary_type.__name__
    DATATYPE.UNICODE = six.text_type.__name__
    DATATYPE.DATE = datetime.date.__name__
    DATATYPE.TIME = datetime.time.__name__
    DATATYPE.DATETIME = datetime.datetime.__name__
    DATATYPE.UNKNOWN = unknown.__name__

    type_map = {
        DATATYPE.INT: int,
        DATATYPE.FLOAT: float,
        DATATYPE.STRING: six.binary_type,
        DATATYPE.UNICODE: six.text_type,
        DATATYPE.DATE: datetime.date,
        DATATYPE.TIME: datetime.time,
        DATATYPE.DATETIME: datetime.datetime,
        DATATYPE.DATETIME: unknown
    }

    column_type_map = {  # FIXME The Column types should be harmonized with these types
        DATATYPE.INT: Column.DATATYPE_INTEGER,
        DATATYPE.FLOAT: Column.DATATYPE_FLOAT,
        DATATYPE.STRING: Column.DATATYPE_STR,
        DATATYPE.UNICODE: Column.DATATYPE_STR,
        DATATYPE.DATE: Column.DATATYPE_DATE,
        DATATYPE.TIME: Column.DATATYPE_TIME,
        DATATYPE.DATETIME: Column.DATATYPE_DATETIME,
        DATATYPE.UNKNOWN: Column.DATATYPE_STR
    }

    vid = SAColumn('sc_vid', String(21), primary_key=True)

    d_vid = SAColumn('sc_d_vid', String(13), ForeignKey('datasets.d_vid'), nullable=False)
    st_vid = SAColumn('sc_st_vid', String(17), ForeignKey('sourcetables.st_vid'), nullable=False)

    position = SAColumn('sc_position', Integer,
                        doc='Integer position of column')

    source_header = SAColumn('sc_source_header', Text,
                             doc='Column header, After coalescing but before mangling.')
    dest_header = SAColumn('sc_dest_header', Text,
                           doc='Original header, mangled')

    datatype = SAColumn('sc_datatype', Text,
                        doc='Basic data type, usually intuited')
    valuetype = SAColumn('sc_valuetype', Text,
                         doc='Describes the meaning of the value: state, county, address, etc.')
    has_codes = SAColumn('sc_has_codes', Boolean, default=False,
                         doc='If True column also has codes of different type')

    start = SAColumn('sc_start', Integer,
                     doc='For fixed width, the column starting position')
    width = SAColumn('sc_width', Integer,
                     doc='For Fixed width, the field width')
    size = SAColumn('sc_size', Integer,
                    doc='Max size of the column values, after conversion to strings.')

    summary = SAColumn('sc_summary', Text,
                       doc='Short text description')
    description = SAColumn('sc_description', Text,
                           doc='Long text description')

    value_labels = SAColumn('sc_value_labels', MutationDict.as_mutable(JSONEncodedObj))

    _next_column_number = None  # Set in next_config_number()

    __table_args__ = (
        UniqueConstraint('sc_st_vid', 'sc_source_header', name='_uc_sourcecolumns'),
    )

    @property
    def name(self):
        return self.source_header

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

        d = OrderedDict([('table', self.table.name)] + [(p.key, getattr(self, p.key)) for p in self.__mapper__.attrs
                         if p.key not in ['vid', 'st_vid', 'table', 'dataset', 'ds_id', 'd_vid',
                                          'source', 'value_labels']])

        return d

    @property
    def dict(self):
        """A dict that holds key/values for all of the properties in the
        object.

        :return:

        """
        from collections import OrderedDict
        SKIP_KEYS = ()
        return OrderedDict(
            (p.key, getattr(self, p.key)) for p in self.__mapper__.attrs if p.key not in SKIP_KEYS)

    def update(self, **kwargs):

        if 'table' in kwargs:
            del kwargs['table']  # In source_schema.csv, this is the name of the table, not the object

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

    columns = relationship(SourceColumn, backref='table', order_by='asc(SourceColumn.position)',
                           cascade='all, delete-orphan', lazy='joined')

    __table_args__ = (
        UniqueConstraint('st_d_vid', 'st_name', name='_uc_sourcetables'),
    )

    def column(self, source_header_or_pos):
        """
        Return a column by name or position

        :param source_header_or_pos: If a string, a source header name. If an integer, column position
        :return:
        """
        for c in self.columns:
            if c.source_header == source_header_or_pos:
                assert c.st_vid == self.vid
                return c
            elif c.position == source_header_or_pos:
                assert c.st_vid == self.vid
                return c

        else:
            return None

    def add_column(self, position, source_header, datatype, **kwargs):
        """
        Add a column to the source table.
        :param position: Integer position of the column
        :param source_header: Name of the column, as it exists in the source file
        :param datatype: Python datatype ( str, int, float, None ) for the column
        :param kwargs:  Other source record args.
        :return:
        """
        from ..identity import GeneralNumber2

        c = self.column(source_header)
        c_by_pos = self.column(position)

        datatype = 'str' if datatype == 'unicode' else datatype

        assert not c or not c_by_pos or c.vid == c_by_pos.vid

        # Convert almost anything to True / False
        if 'has_codes' in kwargs:
            FALSE_VALUES = ['False', 'false', 'F', 'f', '', None, 0, '0']
            kwargs['has_codes'] = False if kwargs['has_codes'] in FALSE_VALUES else True

        if c:

            # Changing the position can result in conflicts
            assert not c_by_pos or c_by_pos.vid == c.vid

            c.update(
                position=position,
                datatype=datatype.__name__ if isinstance(datatype, type) else datatype,
                **kwargs)

        elif c_by_pos:

            # FIXME This feels wrong; there probably should not be any changes to the both
            # of the table, since then it won't represent the previouls source. Maybe all of the sources
            # should get their own tables initially, then affterward the duplicates can be removed.

            assert not c or c_by_pos.vid == c.vid

            c_by_pos.update(
                source_header=source_header,
                datatype=datatype.__name__ if isinstance(datatype, type) else datatype,
                **kwargs)

        else:

            assert not c and not c_by_pos

            # Hacking an id number, since I don't want to create a new Identity ObjectNUmber type
            c = SourceColumn(
                vid=str(GeneralNumber2('C', self.d_vid, self.sequence_id, int(position))),
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
        return {c.source_header: c.dest_header for c in self.columns}

    @property
    def column_index_map(self):
        return {c.source_header: c.position for c in self.columns}

    @property
    def headers(self):
        return [c.source_header for c in self.columns]

    @property
    def widths(self):
        widths = [c.width for c in self.columns]
        if not all(bool(e) for e in widths):
            from ambry.dbexceptions import ConfigurationError
            raise ConfigurationError(
                'The widths array for source table {} has zero or null entries '.format(self.name))

        widths = [int(w) for w in widths]

        return widths

    def update_id(self, sequence_id=None):
        """Alter the sequence id, and all of the names and ids derived from it. This
        often needs to be don after an IntegrityError in a multiprocessing run"""
        from ..identity import GeneralNumber1

        if sequence_id:
            self.sequence_id = sequence_id

        self.vid = str(GeneralNumber1('T', self.d_vid, self.sequence_id))

    def __str__(self):
        from tabulate import tabulate

        headers = 'Pos Source_Header Dest_Header Datatype '.split()
        rows = [(c.position, c.source_header, c.dest_header, c.datatype) for c in self.columns]

        return ('Source Table: {}\n'.format(self.name)) + tabulate(rows, headers)
