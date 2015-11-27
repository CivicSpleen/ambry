"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

__docformat__ = 'restructuredtext en'

import json

from six import string_types, iteritems

import sqlalchemy
from sqlalchemy import BigInteger
from sqlalchemy import Text
from sqlalchemy.types import TypeDecorator, TEXT, UserDefinedType
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.mutable import Mutable
from sqlalchemy.dialects import postgresql, mysql, sqlite
from sqlalchemy import func

Base = declarative_base()

from sqlalchemy.dialects import registry
registry.register('spatialite', 'ambry.orm.dialects.spatialite', 'SpatialiteDialect')
registry.register('postgis', 'ambry.orm.dialects.postgis', 'PostgisDialect')

# http://stackoverflow.com/a/23175518/1144479
# SQLAlchemy does not map BigInt to Int by default on the sqlite dialect.
# It should, but it doesnt.

BigIntegerType = BigInteger()
BigIntegerType = BigIntegerType.with_variant(postgresql.BIGINT(), 'postgresql')
BigIntegerType = BigIntegerType.with_variant(mysql.BIGINT(), 'mysql')
BigIntegerType = BigIntegerType.with_variant(sqlite.INTEGER(), 'sqlite')


class Geometry(UserDefinedType):

    """Geometry type, to ensure that WKT text is properly inserted into the
    database with the GeomFromText() function.

    NOTE! This is paired with code in
    database.relational.RelationalDatabase.table() to convert NUMERIC
    fields that have the name 'geometry' to GEOMETRY types. Sqlalchemy
    sees spatialte GEOMETRY types as NUMERIC

    """

    DEFAULT_SRS = 4326

    def get_col_spec(self):
        return "GEOMETRY"

    def bind_expression(self, bindvalue):
        return func.ST_GeomFromText(bindvalue, self.DEFAULT_SRS, type_=self)

    def column_expression(self, col):
        return func.ST_AsText(col, type_=self)


class SpatialiteGeometry(Geometry):

    def get_col_spec(self):
        return "BLOB"

GeometryType = Geometry()
GeometryType = GeometryType.with_variant(SpatialiteGeometry(), 'spatialite')
GeometryType = GeometryType.with_variant(Text(), 'sqlite')  # Just write the WKT through
GeometryType = GeometryType.with_variant(Text(), 'postgresql')


def table_convert_geometry(metadata, table_name):
    """Get table metadata from the database."""
    from sqlalchemy import Table
    from ..orm import Geometry

    table = Table(table_name, metadata, autoload=True)

    for c in table.columns:

        # HACK! Sqlalchemy sees spatialte GEOMETRY types
        # as NUMERIC

        if c.name == 'geometry':
            c.type = Geometry # What about variants?

    return table


class JSONEncoder(json.JSONEncoder):

    """A JSON encoder that turns unknown objets into a string representation of
    the type."""

    def default(self, o):

        try:
            return o.dict
        except AttributeError:
            return str(type(o))


class JSONEncodedObj(TypeDecorator):

    "Represents an immutable structure as a json-encoded string."

    impl = TEXT

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value, cls=JSONEncoder)
        else:
            value = '{}'
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)

        else:
            value = {}
        return value


class MutationObj(Mutable):

    @classmethod
    def coerce(cls, key, value):
        if isinstance(value, dict) and not isinstance(value, MutationDict):
            return MutationDict.coerce(key, value)

        if isinstance(value, list) and not isinstance(value, MutationList):
            return MutationList.coerce(key, value)

        return value

    @classmethod
    def _listen_on_attribute(cls, attribute, coerce, parent_cls):
        key = attribute.key
        if parent_cls is not attribute.class_:
            return

        # rely on "propagate" here
        parent_cls = attribute.class_

        def load(state, *args):
            val = state.dict.get(key, None)
            if coerce:
                val = cls.coerce(key, val)
                state.dict[key] = val
            if isinstance(val, cls):
                val._parents[state.obj()] = key

        def set(target, value, oldvalue, initiator):
            if not isinstance(value, cls):
                value = cls.coerce(key, value)
            if isinstance(value, cls):
                value._parents[target.obj()] = key
            if isinstance(oldvalue, cls):
                oldvalue._parents.pop(target.obj(), None)
            return value

        def pickle(state, state_dict):
            val = state.dict.get(key, None)
            if isinstance(val, cls):
                if 'ext.mutable.values' not in state_dict:
                    state_dict['ext.mutable.values'] = []
                state_dict['ext.mutable.values'].append(val)

        def unpickle(state, state_dict):
            if 'ext.mutable.values' in state_dict:
                for val in state_dict['ext.mutable.values']:
                    val._parents[state.obj()] = key

        sqlalchemy.event.listen(parent_cls,'load',load,raw=True,propagate=True)
        sqlalchemy.event.listen(parent_cls,'refresh',load,raw=True,propagate=True)
        sqlalchemy.event.listen(attribute,'set',set,raw=True,retval=True,propagate=True)
        sqlalchemy.event.listen(parent_cls,'pickle',pickle,raw=True,propagate=True)
        sqlalchemy.event.listen(parent_cls,'unpickle',unpickle,raw=True,propagate=True)

class MutationDict(Mutable, dict):

    @classmethod
    def coerce(cls, key, value):  # @ReservedAssignment
        """Convert plain dictionaries to MutationDict."""

        if not isinstance(value, MutationDict):
            if isinstance(value, dict):
                return MutationDict(value)

            # this call will raise ValueError
            return Mutable.coerce(key, value)
        else:
            return value

    def __setitem__(self, key, value):
        """Detect dictionary set events and emit change events."""
        dict.__setitem__(self, key, value)

        self.changed()

    def __delitem__(self, key):
        """Detect dictionary del events and emit change events."""

        dict.__delitem__(self, key)
        self.changed()


class MutationList(MutationObj, list):

    @classmethod
    def coerce(cls, key, value):
        """Convert plain list to MutationList."""

        if isinstance(value, string_types):
            value = value.strip()
            if value[0] == '[':  # It's json encoded, probably
                try:
                    value = json.loads(value)
                except ValueError:
                    raise ValueError("Failed to parse JSON: '{}' ".format(value))
            else:
                value = value.split(',')

        if not value:
            value = []

        self = MutationList((MutationObj.coerce(key, v) for v in value))
        self._key = key
        return self

    def __setitem__(self, idx, value):
        list.__setitem__(self, idx, MutationObj.coerce(self._key, value))
        self.changed()

    def __setslice__(self, start, stop, values):
        list.__setslice__(self,start,stop,(MutationObj.coerce(    self._key,    v) for v in values))
        self.changed()

    def __delitem__(self, idx):
        list.__delitem__(self, idx)
        self.changed()

    def __delslice__(self, start, stop):
        list.__delslice__(self, start, stop)
        self.changed()

    def append(self, value):
        list.append(self, MutationObj.coerce(self._key, value))
        self.changed()

    def insert(self, idx, value):
        list.insert(self, idx, MutationObj.coerce(self._key, value))
        self.changed()

    def extend(self, values):
        list.extend(self, (MutationObj.coerce(self._key, v) for v in values))
        self.changed()

    def pop(self, *args, **kw):
        value = list.pop(self, *args, **kw)
        self.changed()
        return value

    def remove(self, value):
        list.remove(self, value)
        self.changed()


def JSONAlchemy(sqltype):
    """A type to encode/decode JSON on the fly.

    sqltype is the string type for the underlying DB column.

    You can use it like:
    Column(JSONAlchemy(Text(600)))

    """
    class _JSONEncodedObj(JSONEncodedObj):
        impl = sqltype

    return MutationObj.as_mutable(_JSONEncodedObj)


class SavableMixin(object):

    def save(self):
        self.session.commit()

class DataPropertyMixin(object):

    """A Mixin for appending a value into a list in the data field."""

    def _append_string_to_list(self, sub_prop, value):
        """"""
        if not sub_prop in self.data:
            self.data[sub_prop] = []

        if value and not value in self.data[sub_prop]:
            self.data[sub_prop] = self.data[sub_prop] + [value]

class LoadPropertiesMixin(object):

    def load_properties(self, args, kwargs):
        for p in self.__mapper__.attrs:
            if p.key in kwargs:
                setattr(self, p.key, kwargs[p.key])
                del kwargs[p.key]

        if self.data:
            self.data.update(kwargs)


# Sould have things derived from this, once there are test cases for it.

class DictableMixin(object):

    def set_attributes(self, **kwargs):
        for k, v in iteritems(kwargs):
            setattr(self, k, v)

    @property
    def record_dict(self):
        return {p.key: getattr(self, p.key) for p in self.__mapper__.attrs}

    @property
    def dict(self):

        d = self.record_dict

        # Move the values in the data attribute into the top level.
        if 'data' in d and d['data']:
            for k in self.data:
                assert k not in d # Data items can't overlap attributes
                d[k] = self.data[k]

        return d


def _clean_flag(in_flag):

    if in_flag is None or in_flag == '0':
        return False

    return bool(in_flag)

# DEPRECATED
# The two remaining uses of this should be replaced with dataset.next_sequence_id
def next_sequence_id(session, sequence_ids, parent_vid, table_class, force_query = False):
    """
    Return the next sequence id for a object, identified by the vid of the parent object, and the database prefix
    for the child object. On the first call, will load the max sequence number
    from the database, but subsequence calls will run in process, so this isn't suitable for
    multi-process operation -- all of the tables in a dataset should be created by one process

    The child table must have a sequence_id value.

    :param session: Database session or connection ( must have an execute() method )
    :param sequence_ids: A dict for caching sequence ids
    :param parent_vid: The VID of the parent object, which sets the namespace for the sequence
    :param table_class: Table class of the child object, the one getting a number
    :return:
    """

    from sqlalchemy import text

    seq_col = table_class.sequence_id.property.columns[0].name

    try:
        parent_col = table_class._parent_col
    except AttributeError:
        parent_col = table_class.d_vid.property.columns[0].name

    assert bool(parent_vid)

    key = (parent_vid, table_class.__name__)

    number = sequence_ids.get(key, None)

    if (not number and session) or force_query:

        sql = text("SELECT max({seq_col})+1 FROM {table} WHERE {parent_col} = '{vid}'"
                   .format(table=table_class.__tablename__, parent_col=parent_col,
                           seq_col=seq_col, vid=parent_vid))

        max_id, = session.execute(sql).fetchone()

        if not max_id:
            max_id = 1

        sequence_ids[key] = int(max_id)

    elif not session:
        # There was no session set. This should only happen when the parent object is new, and therefore,
        # there are no child number, so the appropriate starting number is 1. If the object is not new,
        # there will be conflicts.
        sequence_ids[key] = 1

    else:
        # There were no previous numbers, so start with 1
        sequence_ids[key] += 1

    return sequence_ids[key]


from ambry.orm.code import Code
from ambry.orm.column import Column
from ambry.orm.file import File
from ambry.orm.partition import Partition
from ambry.orm.table import Table
from ambry.orm.config import Config
from ambry.orm.dataset import Dataset
from ambry.orm.columnstat import ColumnStat
from ambry.orm.source_table import SourceColumn, SourceTable
from ambry.orm.source import DataSource, TransientDataSource
from ambry.orm.database import Database
from ambry.orm.account import Account
from ambry.orm.process import Process
