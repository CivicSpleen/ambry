"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'

import sys
import datetime

import dateutil

import six

import sqlalchemy

from sqlalchemy import event
from sqlalchemy import Column as SAColumn, Integer, Float, Boolean, UniqueConstraint
from sqlalchemy import Text, String, ForeignKey
from sqlalchemy.orm import relationship

from ..util import memoize

from . import Base, MutationDict, MutationList,  JSONEncodedObj, BigIntegerType, GeometryType

from ambry.orm.code import Code
from ambry.identity import ColumnNumber, ObjectNumber


if sys.version_info > (3,):
    buffer = memoryview


class Column(Base):
    __tablename__ = 'columns'

    _parent_col = 'c_t_vid'

    vid = SAColumn('c_vid', String(18), primary_key=True)
    id = SAColumn('c_id', String(15)) # Probably not necessary

    sequence_id = SAColumn('c_sequence_id', Integer)
    is_primary_key = SAColumn('c_is_primary_key', Boolean, default=False)

    t_vid = SAColumn('c_t_vid', String(15), ForeignKey('tables.t_vid'), nullable=False, index=True)
    d_vid = SAColumn('c_d_vid', String(13), ForeignKey('datasets.d_vid'), nullable=False, index=True)
    t_id = SAColumn('c_t_id', String(12))

    #source_name = SAColumn('c_source_name', Text, index=True)
    name = SAColumn('c_name', Text, index=True)
    altname = SAColumn('c_altname', Text)
    datatype = SAColumn('c_datatype', Text)
    valuetype = SAColumn('c_valuetype', Text)
    start = SAColumn('c_start', Integer, doc='For fixed width files, the starting position of the column')
    size = SAColumn('c_size', Integer, doc='For fixed width files, the ending position of the column')
    width = SAColumn('c_width', Integer, doc='For fixed width files, the width of the column')
    default = SAColumn('c_default', Text)
    illegal_value = SAColumn('c_illegal_value', Text)  # A special value meaning N/A or nan, etc.

    summary = SAColumn('c_summary', Text)
    description = SAColumn('c_description', Text)
    keywords = SAColumn('c_keywords', Text)

    lom = SAColumn('c_lom', String(1),
                   doc='Level of Measurement: n,o,i,r for Nominal, Ordinal, Interval, Ratio')
    role = SAColumn('c_role', String(1),
                   doc='Role: key, dimension, measure, error, name')
    scale = SAColumn('c_scale', Float, doc='Number of measure units per natural units. Ie, if 1 == 1000 people, scale = 1000')
    units = SAColumn('c_units', Text)
    universe = SAColumn('c_universe', Text)

    parent = SAColumn('c_parent', Text)
    derivedfrom = SAColumn('c_derivedfrom', Text)
    numerator = SAColumn('c_numerator', String(20))
    denominator = SAColumn('c_denominator', String(20))

    # New column value casters and generators
    _transform = SAColumn('c_transform', Text)

    data = SAColumn('c_data', MutationDict.as_mutable(JSONEncodedObj))

    # This column should really be called 'value labels'
    codes = relationship(Code, backref='column', order_by='asc(Code.key)',
                         cascade='save-update, delete, delete-orphan')

    __table_args__ = (
        UniqueConstraint('c_sequence_id', 'c_t_vid', name='_uc_c_sequence_id'),
        UniqueConstraint('c_name', 'c_t_vid', name='_uc_c_name'),
    )

    # FIXME. These types should be harmonized with   SourceColumn.DATATYPE
    DATATYPE_STR = six.binary_type.__name__
    DATATYPE_UNICODE = six.text_type.__name__
    DATATYPE_INTEGER = 'int'
    DATATYPE_INTEGER64 = 'long' if six.PY2 else 'int'
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

        # Here, 'str' means ascii, 'unicode' means not ascii.
        # FIXME: Change names to DATATYPE_ASCII, DATATYPE_NOT_ASCII because it confuses while
        # python2/python3 porting.
        DATATYPE_STR: (sqlalchemy.types.String, six.binary_type, 'VARCHAR'),
        DATATYPE_UNICODE: (sqlalchemy.types.String, six.text_type, 'VARCHAR'),
        DATATYPE_INTEGER: (sqlalchemy.types.Integer, int, 'INTEGER'),
        DATATYPE_INTEGER64: (BigIntegerType, int, 'INTEGER64'),
        DATATYPE_FLOAT: (sqlalchemy.types.Float, float, 'REAL'),
        DATATYPE_DATE: (sqlalchemy.types.Date, datetime.date, 'DATE'),
        DATATYPE_TIME: (sqlalchemy.types.Time, datetime.time, 'TIME'),
        DATATYPE_TIMESTAMP: (sqlalchemy.types.DateTime, datetime.datetime, 'TIMESTAMP'),
        DATATYPE_DATETIME: (sqlalchemy.types.DateTime, datetime.datetime, 'DATETIME'),
        DATATYPE_POINT: (GeometryType, six.binary_type, 'POINT'),
        DATATYPE_LINESTRING: (GeometryType, six.binary_type, 'LINESTRING'),
        DATATYPE_POLYGON: (GeometryType, six.binary_type, 'POLYGON'),
        DATATYPE_MULTIPOLYGON: (GeometryType, six.binary_type, 'MULTIPOLYGON'),
        DATATYPE_GEOMETRY: (GeometryType, six.binary_type, 'GEOMETRY'),
        DATATYPE_BLOB: (sqlalchemy.types.LargeBinary, buffer, 'BLOB')
    }

    def __init__(self,  **kwargs):

        super(Column, self).__init__(**kwargs)

        assert self.sequence_id is not None

        if not self.name:
            self.name = 'column' + str(self.sequence_id)
            # raise ValueError('Column must have a name. Got: {}'.format(kwargs))

        # Don't allow these values to be the empty string
        self.transform = self.transform or None

    @classmethod
    def python_types(cls):
        return [e[1] for e in six.itervalues(cls.types)]

    def type_is_int(self):
        return self.python_type == int

    def type_is_real(self):
        return self.python_type == float

    def type_is_number(self):
        return self.type_is_real or self.type_is_int

    def type_is_text(self):
        return self.datatype == Column.DATATYPE_STR or self.datatype == Column.DATATYPE_UNICODE

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

    def type_is_builtin(self):
        """Return False if the datatype is not one of the builtin type"""
        return self.datatype in self.types

    @property
    def sqlalchemy_type(self):
        return self.types[self.datatype][0]

    @property
    def valuetype_class(self):
        """Return the valuetype class, if one is defined, or a built-in type if it isn't"""

        from ambry.valuetype import resolve_value_type

        if self.valuetype:
            return resolve_value_type(self.valuetype)

        else:
            return resolve_value_type(self.datatype)

    @property
    def valuetype_description(self):
        """Return the valuetype class, if one is defined, or a built-in type if it isn't"""

        from ambry.valuetype import resolve_value_type

        return self.valuetype_class.desc

    @property
    def python_type(self):
        """Return the python type for the row, possibly getting it from a valuetype reference """

        from ambry.valuetype import resolve_value_type

        if self.valuetype:
            return resolve_value_type(self.valuetype)._pythontype

        elif self.datatype:
            try:
                return self.types[self.datatype][1]
            except KeyError:
                return resolve_value_type(self.datatype)._pythontype

        else:
            from ambry.exc import ConfigurationError
            raise ConfigurationError("Can't get python_type: neither datatype of valuetype is defined")

    @property
    def role(self):
        '''Return the code for the role,  measure, dimension or error'''
        from ambry.valuetype.core import role_descriptions, ROLE

        if not self.valuetype_class:
            return ''

        role = self.valuetype_class.role

        if role == ROLE.UNKNOWN:
            vt_code = self.valuetype_class.vt_code

            if len(vt_code) == 1 or vt_code[1] == '/':
                return vt_code[0]
            else:
                return ''

        return role

    @property
    def role_description(self):
        from ambry.valuetype.core import role_descriptions
        return role_descriptions.get(self.role,'')


    @property
    def has_nulls(self):
        """Return True if the datatype allows for null values ( it is specified with a '?' at the end ) """
        return self.valuetype.endswith('?')

    @property
    def children(self):
        """"Return the table's other column that have this column as a parent, excluding labels"""
        for c in self.table.columns:
            if c.parent == self.name and '/label' not in c.valuetype:
                yield c

    @property
    def label(self):
        """"Return first child that of the column that is marked as a label"""
        for c in self.table.columns:
            if c.parent == self.name and '/label' in c.valuetype:
                return c

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
                raise TypeError('{} was parsed to {}, expected {}'.format(v, type(dt), self.python_type))

            return dt
        else:
            # This isn't calling the python_type method -- it's getting a python type, then instantialting it,
            # such as "int(v)"
            return self.python_type(v)

    @property
    def schema_type(self):

        if not self.datatype:
            from .exc import ConfigurationError
            raise ConfigurationError("Column '{}' has no datatype".format(self.name))

        # let it fail with KeyError if datatype is unknown.
        pt = self.python_type.__name__
        return self.types[pt][2]

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
            six.text_type: six.binary_type
        }

        for col_type, (sla_type, py_type, sql_type) in six.iteritems(cls.types):

            if py_type == type_map.get(py_type_in, py_type_in):
                if col_type == 'blob' and name and name.endswith('geometry'):
                    return cls.DATATYPE_GEOMETRY

                elif sla_type != GeometryType:  # Total HACK. FIXME
                    return col_type

        return None

    @property
    def foreign_key(self):
        return self.fk_vid

    @property
    def dest_header(self):
        """Allows destination tables to be used as source tables when creating schema from a 'partition' source"""
        if self.altname:
            return self.altname
        else:
            return self.name

    @property
    def has_codes(self):
        """Allows destination tables to be used as source tables when creating schema from a 'partition' source"""
        return False

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
            for k, v in six.iteritems(self.data):
                if k not in d and k not in ('table', 'stats', '_codes', 'data'):
                    d[k] = v

        return d

    @property
    def nonull_dict(self):
        """Like dict, but does not hold any null values.

        :return:

        """
        return {k: v for k, v in six.iteritems(self.dict) if v and k != '_codes'}

    @staticmethod
    def mangle_name(name):
        """Mangles a column name to a standard form, remoing illegal
        characters.

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

        from six import text_type

        for cd in self.codes:
            if cd.key == text_type(key):
                return cd

        def cast_to_int(s):
            try:
                return int(s)
            except (TypeError, ValueError):
                return None

        cd = Code(c_vid=self.vid, t_vid=self.t_vid,
                  key=text_type(key),
                  ikey=cast_to_int(key),
                  value=value,
                  source=source,
                  description=description,
                  data=data)

        self.codes.append(cd)

        return cd

    @property
    def transform(self):
        return self._transform

    @transform.setter
    def transform(self, v):
        self._transform = self.clean_transform(v)

    @staticmethod
    def make_xform_seg(init_=None, datatype=None, transforms=None, exception=None, column=None):
        return {
            'init': init_,
            'transforms': transforms if transforms else [],
            'exception': exception,
            'datatype': datatype,
            'column': column
        }

    @staticmethod
    def _expand_transform(transform):
        from ambry.dbexceptions import ConfigurationError

        if not bool(transform):
            return []

        transform = transform.rstrip('|')

        segments = []

        for i, seg_str in enumerate(transform.split('||')):
            pipes = seg_str.split('|')

            d = Column.make_xform_seg()

            for pipe in pipes:

                if not pipe.strip():
                    continue

                if pipe[0] == '^':  # First, the initializer
                    if d['init']:
                        raise ConfigurationError('Can only have one initializer in a pipeline segment')
                    if i != 0:
                        raise ConfigurationError('Can only have an initializer in the first pipeline segment')
                    d['init'] = pipe[1:]
                elif pipe[0] == '!':  # Exception Handler
                    if d['exception']:
                        raise ConfigurationError('Can only have one exception handler in a pipeline segment')
                    d['exception'] = pipe[1:]
                else:  # Assume before the datatype
                    d['transforms'].append(pipe)

            segments.append(d)

        return segments

    @property
    def expanded_transform(self):
        """Expands the transform string into segments """

        segments = self._expand_transform(self.transform)

        if segments:

            segments[0]['datatype'] = self.valuetype_class

            for s in segments:
                s['column'] = self

        else:

            segments = [self.make_xform_seg(datatype=self.valuetype_class, column=self)]

        # If we want to add the find datatype cast to a transform.
        #segments.append(self.make_xform_seg(transforms=["cast_"+self.datatype], column=self))

        return segments

    @staticmethod
    def clean_transform(transform):

        segments = Column._expand_transform(transform)

        def pipeify_seg(seg):

            o = []

            seg['init'] and o.append('^' + seg['init'])
            o += seg['transforms']
            seg['exception'] and o.append('!' + seg['exception'])

            return '|'.join(o)

        return '||'.join(pipeify_seg(seg) for seg in segments)



    @property
    def row(self):
        from collections import OrderedDict

        # Use an Ordered Dict to make it friendly to creating CSV files.

        name_map = {
            'name': 'column'
        }

        d = OrderedDict([('table', self.table.name)] +
                        [(name_map.get(p.key, p.key), getattr(self, p.key)) for p in self.__mapper__.attrs
                         if p.key not in ['codes', 'dataset', 'stats', 'table', 'd_vid', 'vid', 't_vid',
                                          'id', 'is_primary_key', 'data']])

        d['transform'] = d['_transform']
        del d['_transform']

        if self.name == 'id':
            t = self.table
            d['description'] = t.description
            data = t.data
        else:
            data = self.data

        for k, v in six.iteritems(data):
            d['d_'+k] = v

        assert 'data' not in d

        return d

    def __repr__(self):
        return '<column: {}, {}>'.format(self.name, self.vid)

    @staticmethod
    def update_number(target):

        ton = ObjectNumber.parse(target.t_vid)
        con = ColumnNumber(ton, target.sequence_id)
        target.id = str(ton.rev(None))
        target.vid = str(con)
        target.id = str(con.rev(None))
        target.d_vid = str(ObjectNumber.parse(target.t_vid).as_dataset)

    @staticmethod
    def before_insert(mapper, conn, target):
        """event.listen method for Sqlalchemy to set the seqience_id for this
        object and create an ObjectNumber value for the id_"""

        # from identity import ObjectNumber
        # assert not target.fk_vid or not ObjectNumber.parse(target.fk_vid).revision

        if target.sequence_id is None:
            from ambry.orm.exc import DatabaseError
            raise DatabaseError('Must have sequence_id before insertion')

        # Check that the id column is always sequence id 1
        assert (target.name == 'id') == (target.sequence_id == 1), (target.name, target.sequence_id)

        Column.before_update(mapper, conn, target)

    @staticmethod
    def before_update(mapper, conn, target):
        """Set the column id number based on the table number and the sequence
        id for the column."""

        assert target.datatype or target.valuetype

        target.name = Column.mangle_name(target.name)

        Column.update_number(target)

event.listen(Column, 'before_insert', Column.before_insert)
event.listen(Column, 'before_update', Column.before_update)
