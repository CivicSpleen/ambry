"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'


import datetime

import dateutil

import sqlalchemy

from sqlalchemy import event
from sqlalchemy import Column as SAColumn, Integer, Boolean, UniqueConstraint
from sqlalchemy import Text, String, ForeignKey
from sqlalchemy.orm import relationship

from sqlalchemy.sql import text

from ..util import memoize

from . import Base, MutationDict, MutationList,  JSONEncodedObj, BigIntegerType, GeometryType

from ambry.orm.code import Code
from ambry.identity import ColumnNumber, ObjectNumber


class Column(Base):
    __tablename__ = 'columns'

    vid = SAColumn('c_vid', String(18), primary_key=True)
    id = SAColumn('c_id', String(15))
    sequence_id = SAColumn('c_sequence_id', Integer)
    is_primary_key = SAColumn('c_is_primary_key', Boolean, default=False)
    t_vid = SAColumn('c_t_vid', String(15), ForeignKey('tables.t_vid'), nullable=False, index=True)
    d_vid = SAColumn('c_d_vid', String(13), ForeignKey('datasets.d_vid'), nullable=False, index=True)
    t_id = SAColumn('c_t_id', String(12))
    name = SAColumn('c_name', Text)
    fqname = SAColumn('c_fqname', Text)  # Name with the vid prefix
    altname = SAColumn('c_altname', Text)
    datatype = SAColumn('c_datatype', Text)
    valuetype = SAColumn('c_valuetype', Text)
    start = SAColumn('c_start', Integer)
    size = SAColumn('c_size', Integer)
    width = SAColumn('c_width', Integer)
    sql = SAColumn('c_sql', Text)
    summary = SAColumn('c_summary', Text)
    description = SAColumn('c_description', Text)
    keywords = SAColumn('c_keywords', Text)
    caster = SAColumn('c_caster', Text)
    units = SAColumn('c_units', Text)
    universe = SAColumn('c_universe', Text)
    lom = SAColumn('c_lom', String(1))

    # A column vid, or possibly an equation, describing how this column was
    # created from other columns.
    derivedfrom = SAColumn('c_derivedfrom', Text)

    # ids of columns used for computing ratios, rates and densities
    numerator = SAColumn('c_numerator', String(20))
    denominator = SAColumn('c_denominator', String(20))

    indexes = SAColumn('t_indexes', MutationList.as_mutable(JSONEncodedObj))
    uindexes = SAColumn('t_uindexes', MutationList.as_mutable(JSONEncodedObj))

    default = SAColumn('c_default', Text)
    illegal_value = SAColumn('c_illegal_value', Text)

    data = SAColumn('c_data', MutationDict.as_mutable(JSONEncodedObj))

    codes = relationship(Code, backref='column', order_by="asc(Code.key)",
                         cascade="save-update, delete, delete-orphan")

    __table_args__ = (
        UniqueConstraint('c_sequence_id', 'c_t_vid', name='_uc_c_sequence_id'),
        UniqueConstraint('c_name', 'c_t_vid', name='_uc_c_name'),
    )

    # FIXME. These types should be harmonized with   SourceColumn.DATATYPE
    DATATYPE_STR = 'str'
    DATATYPE_INTEGER = 'int'
    DATATYPE_INTEGER64 = 'long'
    DATATYPE_FLOAT = 'float'
    DATATYPE_DATE = 'date'
    DATATYPE_TIME = 'time'
    DATATYPE_TIMESTAMP = 'timestamp'
    DATATYPE_DATETIME = 'datetime'
    DATATYPE_BLOB = 'blob'

    DATATYPE_POINT = 'point'  # Spatalite, sqlite extensions for geo
    DATATYPE_LINESTRING = 'linestring'  # Spatalite, sqlite extensions for geo
    DATATYPE_POLYGON = 'polygon'  # Spatalite, sqlite extensions for geo
    DATATYPE_MULTIPOLYGON = 'multipolygon'  # Spatalite, sqlite extensions for geo
    DATATYPE_GEOMETRY = 'geometry'  # Spatalite, sqlite extensions for geo

    types = {
        # Sqlalchemy, Python, Sql,

        DATATYPE_STR: (sqlalchemy.types.String, str, 'VARCHAR'),
        DATATYPE_INTEGER: (sqlalchemy.types.Integer, int, 'INTEGER'),
        DATATYPE_INTEGER64: (BigIntegerType, int, 'INTEGER64'),
        DATATYPE_FLOAT: (sqlalchemy.types.Float, float, 'REAL'),
        DATATYPE_DATE: (sqlalchemy.types.Date, datetime.date, 'DATE'),
        DATATYPE_TIME: (sqlalchemy.types.Time, datetime.time, 'TIME'),
        DATATYPE_TIMESTAMP: (sqlalchemy.types.DateTime, datetime.datetime, 'TIMESTAMP'),
        DATATYPE_DATETIME: (sqlalchemy.types.DateTime, datetime.datetime, 'DATETIME'),
        DATATYPE_POINT: (GeometryType, str, 'POINT'),
        DATATYPE_LINESTRING: (GeometryType, str, 'LINESTRING'),
        DATATYPE_POLYGON: (GeometryType, str, 'POLYGON'),
        DATATYPE_MULTIPOLYGON: (GeometryType, str, 'MULTIPOLYGON'),
        DATATYPE_GEOMETRY: (GeometryType, str, 'GEOMETRY'),
        DATATYPE_BLOB: (sqlalchemy.types.LargeBinary, buffer, 'BLOB')
    }

    def __init__(self,  **kwargs):

        super(Column, self).__init__(**kwargs)

        assert self.sequence_id is not None

        if not self.name:
            self.name = 'column' + str(self.sequence_id)
            # raise ValueError('Column must have a name. Got: {}'.format(kwargs))

        # Don't allow these values to be the empty string
        self.derivedfrom = self.derivedfrom or None

    def type_is_int(self):
        return self.python_type  == int

    def type_is_real(self):
        return self.python_type  == float

    def type_is_number(self):
        return self.type_is_real or self.type_is_int

    def type_is_text(self):
        return self.python_type == str

    def type_is_geo(self):
        return self.datatype in (
            Column.DATATYPE_POINT, Column.DATATYPE_LINESTRING,
            Column.DATATYPE_POLYGON, Column.DATATYPE_MULTIPOLYGON, Column.DATATYPE_GEOMETRY)

    def type_is_gvid(self):
        return 'gvid' in self.name

    def type_is_time(self):
        return self.datatype in (Column.DATATYPE_TIME, Column.DATATYPE_TIMESTAMP)

    def type_is_date(self):
        return self.datatype in (Column.DATATYPE_TIMESTAMP, Column.DATATYPE_DATETIME, Column.DATATYPE_DATE)

    @property
    def sqlalchemy_type(self):
        return self.types[self.datatype][0]

    @property
    def valuetype_class(self):
        """Return the valuetype class, if one is defined, or a built-in type if it isn't"""
        from ambry.valuetype import import_valuetype

        try:
            return import_valuetype(self.datatype)
        except AttributeError as e:
            return self.types[self.datatype][1]

    @property
    def python_type(self):
        """Return the python type for the row, possibly getting it from a valuetype reference """

        from ambry.valuetype import python_type as vl_pt

        try:
            return self.types[self.datatype][1]
        except KeyError:
            return vl_pt(self.datatype)

    def python_cast(self, v):
        """Cast a value to the type of the column.

        Primarily used to check that a value is valid; it will throw an
        exception otherwise

        """

        if self.type_is_time():
            dt = dateutil.parser.parse(v)

            if self.datatype == Column.DATATYPE_TIME:
                dt = dt.time()
            if not isinstance(dt, self.python_type):
                raise TypeError('{} was parsed to {}, expected {}'.format(v, type(dt),self.python_type))

            return dt
        else:
            # This isn't calling the python_type method -- it's getting a python type, then instantialting it,
            # such as "int(v)"
            return self.python_type(v)

    @property
    def schema_type(self):

        if not self.datatype:
            from exc import ConfigurationError
            raise ConfigurationError("Column '{}' has no datatype".format(self.name))

        # let it fail with KeyError if datatype is unknown.
        return self.types[self.datatype][2]

    @classmethod
    def convert_numpy_type(cls, dtype):
        """Convert a numpy dtype into a Column datatype. Only handles common
        types.

        Implemented as a function to decouple from numpy

        """

        m = {
            'int64': cls.DATATYPE_INTEGER64,
            'float64': cls.DATATYPE_FLOAT,
            'object': cls.DATATYPE_TEXT  # Hack. Pandas makes strings into object.
        }

        t = m.get(dtype.name, None)

        if not t:
            raise TypeError(
                "Failed to convert numpy type: '{}' ".format(
                    dtype.name))

        return t

    @classmethod
    def convert_python_type(cls, py_type_in, name=None):

        type_map = {
            unicode: str
        }

        for col_type, (sla_type, py_type, sql_type) in cls.types.items():

            if py_type == type_map.get(py_type_in, py_type_in):
                if col_type == 'blob' and name and name.endswith('geometry'):
                    return cls.DATATYPE_GEOMETRY

                elif sla_type != GeometryType: # Total HACK. FIXME
                    return col_type

        return None

    @property
    def foreign_key(self):
        return self.fk_vid

    @property
    def dict(self):
        """A dict that holds key/values for all of the properties in the
        object.

        :return:

        """
        d = {p.key: getattr(self, p.key) for p in self.__mapper__.attrs
             if p.key not in ('table', 'stats', '_codes', 'data')}

        if not d:
            raise Exception(self.__dict__)

        d['schema_type'] = self.schema_type

        if self.data:
            # Copy data fields into top level dict, but don't overwrite existind values.
            for k, v in self.data.items():
                if k not in d and k not in ('table', 'stats', '_codes', 'data'):
                    d[k] = v

        return d

    @property
    def nonull_dict(self):
        """Like dict, but does not hold any null values.

        :return:

        """
        return {k: v for k, v in self.dict.items() if v and k != '_codes'}

    @property
    def insertable_dict(self):
        """Like dict, but properties have the table prefix, so it can be
        inserted into a row."""

        d = {p.key: getattr(self, p.key) for p in self.__mapper__.attrs if p.key not in ('table', 'stats', '_codes')}

        x = {('c_' + k).strip('_'): v for k, v in d.items()}

        return x

    @staticmethod
    def mangle_name(name):
        """Mangles a column name to a standard form, remoing illegal
        characters.

        :param name:
        :return:

        """
        import re
        try:
            return re.sub('_+','_',re.sub('[^\w_]','_',name).lower()).rstrip('_')
        except TypeError:
            raise TypeError(
                'Trying to mangle name with invalid type of: ' + str(type(name)))

    @property
    @memoize
    def reverse_code_map(self):
        """Return a map from a code ( usually a string ) to the  shorter numeric value"""

        return {c.value: (c.ikey if c.ikey else c.key) for c in self.codes}

    @property
    @memoize
    def forward_code_map(self):
        """Return  a map from the short code to the full value """

        return {c.key: c.value for c in self.codes}

    def add_code(self, key, value, description=None, data=None, source=None):
        """

        :param key: The code value that appears in the datasets, either a string or an int
        :param value: The string value the key is mapped to
        :param description:  A more detailed description of the code
        :param data: A data dict to add to the ORM record
        :return: the code record
        """

        # Ignore codes we already have, but will not catch codes added earlier for this same
        # object, since the code are cached

        for cd in self.codes:
            if cd.key == str(key):
                return cd

        def cast_to_int(s):
            try:
                return int(s)
            except ValueError:
                return None

        cd = Code(c_vid=self.vid, t_vid=self.t_vid,
                  key=str(key),
                  ikey=cast_to_int(key),
                  value=value,
                  source = source,
                  description=description,
                  data=data)

        self.codes.append(cd)

        return cd

    @property
    def row(self):
        from collections import OrderedDict

        # Use an Ordered Dict to make it friendly to creating CSV files.

        d = OrderedDict([('table', self.table.name)] +
                        [(p.key, getattr(self, p.key)) for p in self.__mapper__.attrs
                         if p.key not in ['codes', 'dataset', 'stats', 'table', 'd_vid', 'vid', 't_vid',
                                          'sequence_id', 'id', 'is_primary_key']])

        d['column'] = d['name']
        del d['name']

        if self.name == 'id':
            t = self.table
            d['description'] = t.description
            data = t.data
        else:
            data = self.data

        for k, v in data.items():
            d[k] = v

        return d

    def __repr__(self):
        return '<column: {}, {}>'.format(self.name, self.vid)

    @staticmethod
    def before_insert(mapper, conn, target):
        """event.listen method for Sqlalchemy to set the seqience_id for this
        object and create an ObjectNumber value for the id_"""

        # from identity import ObjectNumber
        # assert not target.fk_vid or not ObjectNumber.parse(target.fk_vid).revision

        if target.sequence_id is None:
            from ambry.orm.exc import DatabaseError
            raise DatabaseError("Must have sequence_id before insertion")

        Column.before_update(mapper, conn, target)

    @staticmethod
    def before_update(mapper, conn, target):
        """Set the column id number based on the table number and the sequence
        id for the column."""

        target.name = Column.mangle_name(target.name)

        ton = ObjectNumber.parse(target.t_vid)
        con = ColumnNumber(ton, target.sequence_id)
        target.id = str(ton.rev(None))
        target.vid = str(con)
        target.id = str(con.rev(None))
        target.d_vid = str(ObjectNumber.parse(target.t_vid).as_dataset)




event.listen(Column, 'before_insert', Column.before_insert)
event.listen(Column, 'before_update', Column.before_update)
