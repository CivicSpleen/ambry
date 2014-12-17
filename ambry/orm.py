"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'
import datetime
import sqlalchemy
from sqlalchemy import orm
from sqlalchemy import event
from sqlalchemy import Column as SAColumn, Integer, BigInteger, Boolean, UniqueConstraint, ForeignKeyConstraint
from sqlalchemy import Float as Real,  Text, String, ForeignKey, Binary, Table as SATable
from sqlalchemy.orm import relationship
from sqlalchemy.types import TypeDecorator, TEXT, PickleType
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.mutable import Mutable
from sqlalchemy.exc import OperationalError
from util import Constant, memoize
from identity import LocationRef

from sqlalchemy.sql import text
from ambry.identity import  DatasetNumber, ColumnNumber
from ambry.identity import TableNumber, PartitionNumber, ObjectNumber

import json


# http://stackoverflow.com/a/23175518/1144479
# SQLAlchemy does not map BigInt to Int by default on the sqlite dialect.
# It should, but it doesnt.

from sqlalchemy import BigInteger
from sqlalchemy.dialects import postgresql, mysql, sqlite

BigIntegerType = BigInteger()
BigIntegerType = BigIntegerType.with_variant(postgresql.BIGINT(), 'postgresql')
BigIntegerType = BigIntegerType.with_variant(mysql.BIGINT(), 'mysql')
BigIntegerType = BigIntegerType.with_variant(sqlite.INTEGER(), 'sqlite')


Base = declarative_base()


class JSONEncoder(json.JSONEncoder):
    """A JSON encoder that turns unknown objets into a string representation of the type """
    def default(self, o):
        from ambry.identity import Identity

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
            try:
                value = json.loads(value)
            except:
                # We've changed from using pickle to json, so this handles legacy cases
                import pickle
                value = pickle.loads(value)

        else:
            value = {}
        return value

class MutationDict(Mutable, dict):
    @classmethod
    def coerce(cls, key, value): #@ReservedAssignment
        "Convert plain dictionaries to MutationDict."

        if not isinstance(value, MutationDict):
            if isinstance(value, dict):
                return MutationDict(value)

            # this call will raise ValueError
            return Mutable.coerce(key, value)
        else:
            return value

    def __setitem__(self, key, value):
        "Detect dictionary set events and emit change events."
        dict.__setitem__(self, key, value)

        self.changed()

    def __delitem__(self, key):
        "Detect dictionary del events and emit change events."

        dict.__delitem__(self, key)
        self.changed()

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
 
        sqlalchemy.event.listen(parent_cls, 'load', load, raw=True, propagate=True)
        sqlalchemy.event.listen(parent_cls, 'refresh', load, raw=True, propagate=True)
        sqlalchemy.event.listen(attribute, 'set', set, raw=True, retval=True, propagate=True)
        sqlalchemy.event.listen(parent_cls, 'pickle', pickle, raw=True, propagate=True)
        sqlalchemy.event.listen(parent_cls, 'unpickle', unpickle, raw=True, propagate=True)
        
class MutationList(MutationObj, list):
    @classmethod
    def coerce(cls, key, value):
        """Convert plain list to MutationList"""
        self = MutationList((MutationObj.coerce(key, v) for v in value))
        self._key = key
        return self
 
    def __setitem__(self, idx, value):
        list.__setitem__(self, idx, MutationObj.coerce(self._key, value))
        self.changed()
 
    def __setslice__(self, start, stop, values):
        list.__setslice__(self, start, stop, (MutationObj.coerce(self._key, v) for v in values))
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
    """A type to encode/decode JSON on the fly
 
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


class LinkableMixin(object):
    """A mixin for creating acessors to link between objects with references in the .dataproperty
    Should probably be a descriptor, but I don't feel like fighting with it. """

    # _get_link_array(self, name, clz, id_column):
    # _append_link(self, name, object_id):
    # _remove_link(self, name, object_id):

    def _get_link_array(self, name, clz, id_column):
        """
        name: the name of the link, a key in the data property
        clz: Sqlalchemy ORM class for the foreign object
        id_column, the Sqlalchemy property, from the clz classs, that hpolds the stored id value for the object.
        """
        from sqlalchemy.orm import object_session

        id_values = self.data.get(name, [])

        if not id_values:
            return []

        return object_session(self).query(clz).filter(id_column.in_(id_values)).all()


    def _append_link(self, name, object_id):
        """
        name: the name of the link, a key in the data property
        o: the object being linked. If none, no back link is made
        object_id: the object identitifer that is stored in the data property
        """
        if not name in self.data:
            self.data[name] = []

        if not object_id in self.data[name]:
            self.data[name] = self.data[name] + [object_id]

    def _remove_link(self, name, object_id):
        """For linking manifests to stores"""
        if not name in self.data:
            return

        if self.data[name] and object_id in self.data[name]:
            self.data[name] = self.data[name].remove(object_id)

class DataPropertyMixin(object):
    """A Mixin for appending a value into a list in the data field"""



    def _append_string_to_list(self, sub_prop, value):
        """ """
        if not sub_prop in self.data:
            self.data[sub_prop] = []

        if value and not value in self.data[sub_prop]:
            self.data[sub_prop] = self.data[sub_prop] + [value]

# Sould have things derived from this, once there are test cases for it.
# Actually, this is a mixin.
class DictableMixin(object):

    def set_attributes(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def dict(self):
        from sqlalchemy.orm.attributes import InstrumentedAttribute
        import inspect

        return dict(inspect.getmembers(self.__class__, lambda x: isinstance(x, InstrumentedAttribute)))


    def __repr__(self):
        return "<{}: {}>".format(type(self), self.dict)





class Dataset(Base, LinkableMixin):
    __tablename__ = 'datasets'

    LOCATION = Constant()
    LOCATION.LIBRARY = LocationRef.LOCATION.LIBRARY
    LOCATION.PARTITION = LocationRef.LOCATION.PARTITION
    LOCATION.SREPO = LocationRef.LOCATION.SREPO
    LOCATION.SOURCE = LocationRef.LOCATION.SOURCE
    LOCATION.REMOTE =  LocationRef.LOCATION.REMOTE
    LOCATION.UPSTREAM = LocationRef.LOCATION.UPSTREAM

    vid = SAColumn('d_vid',String(20), primary_key=True)
    id_ = SAColumn('d_id',String(20), )
    name = SAColumn('d_name',String(200),  nullable=False, index=True)
    vname = SAColumn('d_vname',String(200), unique=True,  nullable=False, index=True)
    fqname = SAColumn('d_fqname',String(200), unique=True,  nullable=False)
    cache_key = SAColumn('d_cache_key',String(200), unique=True,  nullable=False, index=True)
    source = SAColumn('d_source',String(200), nullable=False)
    dataset = SAColumn('d_dataset',String(200), nullable=False)
    subset = SAColumn('d_subset',String(200))
    variation = SAColumn('d_variation',String(200))
    btime = SAColumn('d_btime', String(200))
    bspace = SAColumn('d_bspace', String(200))
    creator = SAColumn('d_creator',String(200), nullable=False)
    revision = SAColumn('d_revision',Integer, nullable=False)
    version = SAColumn('d_version',String(20), nullable=False)

    data = SAColumn('d_data', MutationDict.as_mutable(JSONEncodedObj))

    path = None  # Set by the LIbrary and other queries. 

    tables = relationship("Table", backref='dataset', cascade="all, delete-orphan", 
                          passive_updates=False)

    partitions = relationship("Partition", backref='dataset', cascade="all, delete-orphan",
                               passive_updates=False)



    #__table_args__ = (
    #    UniqueConstraint('d_vid', 'd_location', name='u_vid_location'),
    #    UniqueConstraint('d_fqname', 'd_location', name='u_fqname_location'),
    #    UniqueConstraint('d_cache_key', 'd_location', name='u_cache_location'),
    #)


    def __init__(self,**kwargs):
        self.id_ = kwargs.get("oid",kwargs.get("id",kwargs.get("id_", None)) )
        self.vid = kwargs.get("vid", None)
        # Deprecated?
        self.location = kwargs.get("location", self.LOCATION.LIBRARY)
        self.name = kwargs.get("name",None) 
        self.vname = kwargs.get("vname",None) 
        self.fqname = kwargs.get("fqname",None)
        self.cache_key = kwargs.get("cache_key",None)
        self.source = kwargs.get("source",None) 
        self.dataset = kwargs.get("dataset",None) 
        self.subset = kwargs.get("subset",None) 
        self.variation = kwargs.get("variation",None)
        self.btime = kwargs.get("btime", None)
        self.bspace = kwargs.get("bspace", None)
        self.creator = kwargs.get("creator",None) 
        self.revision = kwargs.get("revision",None) 
        self.version = kwargs.get("version",None) 

        if not self.id_:
            dn = DatasetNumber(None, self.revision )
            self.vid = str(dn)
            self.id_ = str(dn.rev(None))
        elif not self.vid:
            try:
                self.vid = str(ObjectNumber.parse(self.id_).rev(self.revision))
            except ValueError as e:
                print repr(self)
                raise ValueError('Could not parse id value; '+e.message)

        if self.cache_key is None:
            self.cache_key = self.identity.cache_key

        assert self.vid[0] == 'd'

    def __repr__(self):
        return """<datasets: id={} vid={} name={} source={} ds={} ss={} var={} creator={} rev={}>""".format(
                    self.id_, self.vid, self.name, self.source,
                    self.dataset, self.subset, self.variation, 
                    self.creator, self.revision)
        
        
    @property
    def identity(self):
        from identity import Identity
        return Identity.from_dict(self.dict )
       
    @property 
    def dict(self):
        d =  {
                'id':self.id_, 
                'vid':self.vid,
                'name':self.name,
                'vname':self.vname,
                'fqname':self.fqname,
                'cache_key':self.cache_key,
                'source':self.source,
                'dataset':self.dataset, 
                'subset':self.subset, 
                'variation':self.variation,
                'btime': self.btime,
                'bspace': self.bspace,
                'creator':self.creator, 
                'revision':self.revision, 
                'version':self.version, 
                }

        if self.data:
            for k in self.data:
                assert k not in d
                d[k] = self.data[k]

        return d


    # For linking partitions to manifests
    @property
    def linked_manifests(self): return self._get_link_array('manifests', File, File.ref)
    def link_manifest(self, f): return self._append_link('manifests', f.ref)
    def delink_manifest(self, f): return self._remove_link('manifests', f.ref)

    @property
    def linked_stores(self): return self._get_link_array('stores', File, File.ref)
    def link_store(self, f): return self._append_link('stores', f.ref)
    def delink_store(self, f): return self._remove_link('stores', f.ref)

def _clean_flag( in_flag):
    
    if in_flag is None or in_flag == '0':
        return False

    return bool(in_flag)

class Column(Base):
    __tablename__ = 'columns'

    vid = SAColumn('c_vid',String(20), primary_key=True)
    id_ = SAColumn('c_id',String(20))
    sequence_id = SAColumn('c_sequence_id',Integer)
    t_vid = SAColumn('c_t_vid',String(20),ForeignKey('tables.t_vid'), index=True)
    t_id = SAColumn('c_t_id',String(20))
    name = SAColumn('c_name',Text)
    altname = SAColumn('c_altname',Text)
    datatype = SAColumn('c_datatype',Text)
    size = SAColumn('c_size',Integer)
    start = SAColumn('c_start', Integer)
    width = SAColumn('c_width',Integer)
    sql = SAColumn('c_sql',Text)
    precision = SAColumn('c_precision',Integer)
    flags = SAColumn('c_flags',Text)
    description = SAColumn('c_description',Text)
    keywords = SAColumn('c_keywords',Text)
    measure = SAColumn('c_measure',Text)
    units = SAColumn('c_units',Text)
    universe = SAColumn('c_universe',Text)
    scale = SAColumn('c_scale',Real)

    # Reference to a column that provides an example of how this column should be used.
    proto_vid = SAColumn('c_proto_vid', String(20), index=True)

    # Reference to a column that this column links to.
    fk_vid = SAColumn('c_fk_vid', String(20),  index=True)

    data = SAColumn('c_data',MutationDict.as_mutable(JSONEncodedObj))

    is_primary_key = SAColumn('c_is_primary_key',Boolean, default = False)

    unique_constraints = SAColumn('c_unique_constraints',Text)
    indexes = SAColumn('c_indexes',Text)
    uindexes = SAColumn('c_uindexes',Text)
    default = SAColumn('c_default',Text)
    illegal_value = SAColumn('c_illegal_value',Text)

    __table_args__ = (UniqueConstraint('c_sequence_id', 'c_t_vid', name='_uc_columns_1'),
                     )

    DATATYPE_TEXT = 'text'
    DATATYPE_INTEGER ='integer' 
    DATATYPE_INTEGER64 ='integer64' 
    DATATYPE_REAL = 'real'
    DATATYPE_FLOAT = 'float'
    DATATYPE_NUMERIC = 'numeric'
    DATATYPE_DATE = 'date'
    DATATYPE_TIME = 'time'
    DATATYPE_TIMESTAMP = 'timestamp'
    DATATYPE_DATETIME = 'datetime'
    DATATYPE_POINT = 'point' # Spatalite, sqlite extensions for geo
    DATATYPE_LINESTRING = 'linestring' # Spatalite, sqlite extensions for geo
    DATATYPE_POLYGON = 'polygon' # Spatalite, sqlite extensions for geo
    DATATYPE_MULTIPOLYGON = 'multipolygon' # Spatalite, sqlite extensions for geo
    DATATYPE_CHAR = 'char'
    DATATYPE_VARCHAR = 'varchar'
    DATATYPE_BLOB = 'blob'


    types  = {
        # Sqlalchemy, Python, Sql,
        DATATYPE_TEXT:(sqlalchemy.types.Text,str,'TEXT'),
        DATATYPE_VARCHAR:(sqlalchemy.types.String,str,'VARCHAR'),
        DATATYPE_CHAR:(sqlalchemy.types.String,str,'VARCHAR'),
        DATATYPE_INTEGER:(sqlalchemy.types.Integer,int,'INTEGER'),
        DATATYPE_INTEGER64:(BigIntegerType,long,'INTEGER64'),
        DATATYPE_REAL:(sqlalchemy.types.Float,float,'REAL'),
        DATATYPE_FLOAT:(sqlalchemy.types.Float,float,'REAL'),
        DATATYPE_NUMERIC:(sqlalchemy.types.Float,float,'REAL'),
        DATATYPE_DATE:(sqlalchemy.types.Date,datetime.date,'DATE'),
        DATATYPE_TIME:(sqlalchemy.types.Time,datetime.time,'TIME'),
        DATATYPE_TIMESTAMP:(sqlalchemy.types.DateTime,datetime.datetime,'TIMESTAMP'),
        DATATYPE_DATETIME:(sqlalchemy.types.DateTime,datetime.datetime,'DATETIME'),
        DATATYPE_POINT:(sqlalchemy.types.LargeBinary,buffer,'POINT'),
        DATATYPE_LINESTRING:(sqlalchemy.types.LargeBinary,buffer,'LINESTRING'),
        DATATYPE_POLYGON:(sqlalchemy.types.LargeBinary,buffer,'POLYGON'),
        DATATYPE_MULTIPOLYGON:(sqlalchemy.types.LargeBinary,buffer,'MULTIPOLYGON'),
        DATATYPE_BLOB:(sqlalchemy.types.LargeBinary,buffer,'BLOB')
        }


    def type_is_text(self):
        return self.datatype in (Column.DATATYPE_TEXT, Column.DATATYPE_CHAR, Column.DATATYPE_VARCHAR)

    def type_is_time(self):
        return self.datatype in (Column.DATATYPE_TIME, Column.DATATYPE_TIMESTAMP, Column.DATATYPE_DATETIME, Column.DATATYPE_DATE)

    @property
    def sqlalchemy_type(self):
        return self.types[self.datatype][0]
    
    @property
    def python_type(self):
        return self.types[self.datatype][1]
 
    def python_cast(self,v):
        """Cast a value to the type of the column. Primarily used to check that a value is valid; it will
        throw an exception otherwise"""

        if self.type_is_time():
            import dateutil.parser
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
            from dbexceptions import ConfigurationError
            raise ConfigurationError("Column '{}' has no datatype".format(self.name))

        try:
            return self.types[self.datatype][2]
        except KeyError:
            print '!!!', self.datatype, self.types
            raise

    @classmethod
    def convert_numpy_type(cls,dtype):
        '''Convert a numpy dtype into a Column datatype. Only handles common types.

        Implemented as a function to decouple from numpy'''

        import numpy as np

        m = {
            'int64': cls.DATATYPE_INTEGER64,
            'float64': cls.DATATYPE_FLOAT,
            'object': cls.DATATYPE_TEXT # Hack. Pandas makes strings into object.

        }

        t =  m.get(dtype.name, None)

        if not t:
            raise TypeError("Failed to convert numpy type: '{}' ".format(dtype.name))

        return t

    @classmethod
    def convert_python_type(cls, py_type_in):

        type_map = {
            unicode : str
        }

        for col_type, (sla_type, py_type, sql_type) in cls.types.items():
            if py_type == type_map.get(py_type_in, py_type_in):
                return col_type

        return None


    @property
    def foreign_key(self):
        return self.fk_vid



    def __init__(self,table, **kwargs):

        self.sequence_id = kwargs.get("sequence_id",len(table.columns)+1) 
        self.name = kwargs.get("name",None) 
        self.altname = kwargs.get("altname",None) 
        self.is_primary_key = _clean_flag(kwargs.get("is_primary_key",False))
        self.datatype = kwargs.get("datatype",None) 
        self.size = kwargs.get("size",None) 
        self.precision = kwargs.get("precision",None)
        self.start = kwargs.get("start", None)
        self.width = kwargs.get("width",None)    
        self.sql = kwargs.get("sql",None)      
        self.flags = kwargs.get("flags",None) 
        self.description = kwargs.get("description",None) 
        self.keywords = kwargs.get("keywords",None) 
        self.measure = kwargs.get("measure",None) 
        self.units = kwargs.get("units",None) 
        self.universe = kwargs.get("universe",None) 
        self.scale = kwargs.get("scale",None)
        self.fk_vid = kwargs.get("fk_vid", kwargs.get("foreign_key", None))
        self.proto_vid = kwargs.get("proto_vid",kwargs.get("proto",None))
        self.data = kwargs.get("data",None) 

        # the table_name attribute is not stored. It is only for
        # building the schema, linking the columns to tables. 
        self.table_name = kwargs.get("table_name",None) 

        if not self.name:
            raise ValueError('Column must have a name. Got: {}'.format(kwargs))

        self.t_id = table.id_
        self.t_vid = table.vid
        ton = ObjectNumber.parse(table.vid)
        con = ColumnNumber(ton, self.sequence_id)
        self.vid = str(con)
        self.id = str(con.rev(None))


    @property
    def dict(self):
        x = {k: v for k, v in self.__dict__.items()
             if k in ['id_', 'vid', 't_vid','t_id',
                      'sequence_id', 'name', 'altname', 'is_primary_key', 'datatype', 'size',
                      'precision', 'start', 'width', 'sql', 'flags', 'description', 'keywords', 'measure',
                      'units', 'universe', 'scale', 'proto_vid', 'fk_vid', 'data']}
        if not x:
            raise Exception(self.__dict__)

        x['schema_type'] = self.schema_type

        return x

    @property
    def nonull_dict(self):
        return {k: v for k, v in self.dict.items() if v}


    @property
    def insertable_dict(self):
        x =  {('c_' + k).strip('_'): v for k, v in self.dict.items()}

        return x

    @staticmethod
    def mangle_name(name):
        """
        Mangles a column name to a standard form, remoing illegal characters.

        :param name:
        :return:
        """
        import re
        try:
            return re.sub('[^\w_]','_',name).lower()
        except TypeError:
            raise TypeError('Trying to mangle name with invalid type of: '+str(type(name)))

    @property
    @memoize
    def codes(self):
        return self._codes # Caches the query, I hope ...

    def add_code(self, key, value, description=None, data = None):
        """

        :param key: The code value that appears in the datasets, either a string or an int
        :param value: The string value the key is mapped to
        :param description:  A more detailed description of the code
        :param data: A data dict to add to the ORM record
        :return: the code record
        """
        from  sqlalchemy.orm.session import Session

        # Ignore codes we already have, but will not catch codes added earlier t this same
        # object, since the code are cached
        for cd in self.codes:
            if cd.key == str(key):
                return cd

        cd = Code(c_vid = self.vid, t_vid = self.t_vid,
                  key = str(key),
                  ikey = key if isinstance(key, int) else None,
                  value = value,
                  description = description, data = data)

        Session.object_session(self).add(cd)

        return cd

    @staticmethod
    def before_insert(mapper, conn, target):
        '''event.listen method for Sqlalchemy to set the seqience_id for this
        object and create an ObjectNumber value for the id_'''
        
        if target.sequence_id is None:
            conn.execute("BEGIN IMMEDIATE") # In case this happens in multi-process mode
            sql = text('''SELECT max(c_sequence_id)+1 FROM columns WHERE c_t_id = :tid''')
    
            max_id, = conn.execute(sql, tid=target.t_id).fetchone()
      
            if not max_id:
                max_id = 1

            target.sequence_id = max_id
        
        Column.before_update(mapper, conn, target)

    @staticmethod
    def before_update(mapper, conn, target):
        '''Set the column id number based on the table number and the 
        sequence id for the column'''
       
        if target.id_  is None:
            table_on = ObjectNumber.parse(target.t_id)
            target.id_ = str(ColumnNumber(table_on, target.sequence_id))


    def __repr__(self):
        return "<column: {}, {}>".format(self.name, self.vid)
 
event.listen(Column, 'before_insert', Column.before_insert)
event.listen(Column, 'before_update', Column.before_update)
 
class Table(Base, LinkableMixin, DataPropertyMixin):
    __tablename__ ='tables'

    vid = SAColumn('t_vid',String(20), primary_key=True)
    id_ = SAColumn('t_id',String(20), primary_key=False)
    d_id = SAColumn('t_d_id',String(20))
    d_vid = SAColumn('t_d_vid', String(20), ForeignKey('datasets.d_vid'), index=True)
    sequence_id = SAColumn('t_sequence_id',Integer, nullable = False)
    name = SAColumn('t_name',String(200), nullable = False)
    altname = SAColumn('t_altname',Text)
    description = SAColumn('t_description',Text)
    universe = SAColumn('t_universe',String(200))
    keywords = SAColumn('t_keywords',Text)
    type = SAColumn('t_type', String(20))
    # Reference to a column that provides an example of whow this column should be used.
    proto_vid = SAColumn('t_proto_vid', String(20), index=True)

    installed = SAColumn('t_installed', String(100))
    data = SAColumn('t_data',MutationDict.as_mutable(JSONEncodedObj))

    __table_args__ = (
        #ForeignKeyConstraint([d_vid, d_location], ['datasets.d_vid', 'datasets.d_location']),
        UniqueConstraint('t_sequence_id', 't_d_vid', name='_uc_tables_1'),
        UniqueConstraint('t_name', 't_d_vid', name='_uc_tables_2'),
                     )
    
    columns = relationship(Column, backref='table',
                           order_by="asc(Column.sequence_id)",
                           cascade="all, delete-orphan", lazy='joined')

    def __init__(self,dataset, **kwargs):

        assert 'proto' not in kwargs


        self.sequence_id = kwargs.get("sequence_id",None)  
        self.name = kwargs.get("name",None) 
        self.vname = kwargs.get("vname",None) 
        self.altname = kwargs.get("altname",None) 
        self.description = kwargs.get("description",None)
        self.universe = kwargs.get("universe", None)
        self.keywords = kwargs.get("keywords",None)
        self.type = kwargs.get("type", 'table')
        self.proto_vid = kwargs.get("proto_vid")
        self.data = kwargs.get("data",None) 
        
        self.d_id = dataset.id_
        self.d_vid = dataset.vid
        don = ObjectNumber.parse(dataset.vid)
        ton = TableNumber(don, self.sequence_id)
      
        self.vid = str(ton)
        self.id_ = str(ton.rev(None))

        if self.name:
            self.name = self.mangle_name(self.name, kwargs.get('preserve_case', False))

        self.init_on_load()

    @property
    def dict(self):
        d =  {k:v for k,v in self.__dict__.items() if k in
                ['id_','vid', 'd_id', 'd_vid', 'sequence_id', 'name', 'altname', 'vname', 'description',
                 'universe', 'keywords', 'installed', 'proto_vid', 'type']}

        if self.data:
            for k in self.data:
                assert k not in d, "Value '{}' is a table field and should not be in data ".format(k)
                d[k] = self.data[k]

        d['is_geo'] = False

        for c in self.columns:
            if c in ('geometry', 'wkt', 'wkb', 'lat'):
                d['is_geo'] = True



        return d

    @property
    def nonull_dict(self):
        return {k: v for k, v in self.dict.items() if v}

    @property
    def nonull_col_dict(self):

        tdc = {}
        for c in self.columns:
            tdc[c.id_] = c.nonull_dict
            tdc[c.id_]['codes'] = { cd.key:cd.dict for cd in c.codes}


        td = self.nonull_dict
        td['columns'] = tdc

        return td

    @property
    def insertable_dict(self):
        x =  {('t_' + k).strip('_'): v for k, v in self.dict.items()}

        if not 't_vid' in x or not x['t_vid']:
            raise ValueError("Must have vid set: {} ".format(x))

        return x

    # For linking tables to manifests
    @property
    def linked_files(self): return self._get_link_array('files', File, File.ref)
    def link_file(self, f): return self._append_link('files', f.ref)
    def delink_file(self, f): return self._remove_link('files', f.ref)

    @property
    def info(self):
        
       
        x =  """
------ Table: {name} ------  
id   : {id_}
vid  : {vid}   
name : {name} 
Columns:      
""".format(**self.dict)
        
        for c in self.columns:
            # ['id','vid','sequence_id', 't_vid', 'name', 'description', 'keywords', 'datatype', 'size', 'is_primary_kay', 'data']}

            x += "   {sequence_id:3d} {name:12s} {schema_type:8s} {description}\n".format(**c.dict)
         
        return x


    def _repr_html_(self):
        '''IPython display'''

        t1 = """
        <table>
        <tr><th>Name</th><td>{name}</td></tr>
        <tr><th>Id</th><td>{id_}</td></tr>
        <tr><th>Vid</th><td>{vid}</td></tr>
        </table>
        """.format(**self.dict)

        return t1+self.html_table()

    def html_table(self):
        ''''''

        rows = []
        rows.append(
            "<tr><th>#</th><th>Name</th><th>Datatype</th><th>description</th></tr>")
        for c in self.columns:
            rows.append(
                "<tr><td>{sequence_id:d}</td><td>{name:s}</td><td>{schema_type:s}</td><td>{description}</td></tr>".format(
                    **c.dict))

        return "<table>\n" + "\n".join(rows) + "\n</table>"


    def vid_select(self):
        """ Return a SQL fragment to translate the column names to vids. This allows the identity of the column
        to propagate through views. """

        cols = []

        return ",".join( ["{} AS {}".format(c.name, c.vid) for c in self.columns] )



    @orm.reconstructor
    def init_on_load(self):
        self._or_validator = None
        self._and_validator = None
        self._null_row = None
        self._row_hasher = None
        
    @staticmethod
    def before_insert(mapper, conn, target):
        '''event.listen method for Sqlalchemy to set the seqience_id for this  
        object and create an ObjectNumber value for the id_'''
        if target.sequence_id is None:
            sql = text('''SELECT max(t_sequence_id)+1 FROM tables WHERE t_d_id = :did''')
    
            max_id, = conn.execute(sql, did=target.d_id).fetchone()
      
            if not max_id:
                max_id = 1
                
            target.sequence_id = max_id
        
        Table.before_update(mapper, conn, target)
        
    @staticmethod
    def before_update(mapper, conn, target):
        '''Set the Table ID based on the dataset number and the sequence number
        for the table '''
        if isinstance(target,Column):
            raise TypeError('Got a column instead of a table')
        
        if target.id_ is None:
            dataset_id = ObjectNumber.parse(target.d_id)
            target.id_ = str(TableNumber(dataset_id, target.sequence_id))

        assert 'proto_vid' not in target.data # Check that pro vaules are removed, from warehouse install_table()


    @staticmethod
    def mangle_name(name, preserve_case = False):
        import re
        try:
            r =  re.sub('[^\w_]','_',name.strip())

            if not preserve_case:
                r = r.lower()

            return r
        except TypeError:
            raise TypeError('Not a valid type for name '+str(type(name)))

    @property
    def oid(self):   
        return TableNumber(self.d_id, self.sequence_id)

    def add_column(self, name, **kwargs):
        '''Add a column to the table, or update an existing one '''

        import sqlalchemy.orm.session
        from sqlalchemy.orm.exc import NoResultFound

        s = sqlalchemy.orm.session.Session.object_session(self)


        name = Column.mangle_name(name)

        try:
            row = self.column(name)
        except NoResultFound:
            row = None

        if row:
            extant = True

        else:
            row = Column(self, name=name, **kwargs)
            extant = False

        for key, value in kwargs.items():

            excludes = ['d_id','t_id','name', 'schema_type']

            if key == 'proto' and isinstance(value, basestring):  # Proto is the name of the object.
                key = 'proto_vid'

            if extant:
                excludes.append('sequence_id')

            if key[0] != '_' and key not in excludes :
                try:
                    setattr(row, key, value)
                except AttributeError:
                    raise AttributeError("Column record has no attribute {}".format(key))

            if isinstance(value, basestring) and len(value) == 0:
                if key == 'is_primary_key':
                    value = False
                    setattr(row, key, value)



        # If the id column has a description and the table does not, add it to the table.
        if row.name == 'id' and row.is_primary_key and not self.description:
            self.description = row.description
            s.merge(self)

        if extant:
            s.merge(row)
        else:
            s.add(row)
     
        if kwargs.get('commit', True):
            s.commit()


        return row
   
    def column(self, name_or_id, default=None):
        from sqlalchemy.sql import or_
        import sqlalchemy.orm.session
        s = sqlalchemy.orm.session.Session.object_session(self)
        
        q = (s.query(Column)
               .filter(or_(Column.id_==name_or_id,Column.name==name_or_id))
               .filter(Column.t_id == self.id_)
            )
      
        if not default is None:
            try:
                return  q.one()
            except:
                return default
        else:
            return  q.one()
    
    @property
    def primary_key(self):
        for c in self.columns:
            if c.is_primary_key:
                return c
        return None
    
    def get_fixed_regex(self):
            '''Using the size values for the columns for the table, construct a
            regular expression to  parsing a fixed width file.'''
            import re

            pos = 0
            regex = ''
            header = []
            
            for col in  self.columns:
                
                size = col.width if col.width else col.size
                
                if not size:
                    continue
                
                pos += size
            
                regex += "(.{{{}}})".format(size)
                header.append(col.name)

            return header, re.compile(regex) , regex

    def get_fixed_unpack(self):
            '''Using the size values for the columns for the table, construct a
            regular expression to  parsing a fixed width file.'''
            from functools import partial
            import struct
            unpack_str = ''
            header = []
            length = 0
            
            for col in  self.columns:
                
                size = col.width if col.width else col.size
                
                if not size:
                    continue
                
                length += size
            
                unpack_str += "{}s".format(size)
                
                header.append(col.name)
           
            return partial(struct.unpack, unpack_str), header, unpack_str, length

    def get_fixed_colspec(self):
        """Return the column specification suitable for use in  the Panads read_fwf function

        This will ignore any columns that don't have one or both of the start and width values
        """

        # Warning! Assuming th start values are sorted. Really should check.

        return (
            [ c.name for c in self.columns if c.start and c.width],
            [ ( c.start, c.start + c.width ) for c in self.columns if c.start and c.width]
        )



    @property
    def null_row(self):
        if self._null_row is None:
            self._null_row = []
            for col in self.columns:
                if col.is_primary_key:
                    v = None
                elif col.default:
                    v = col.default
                else:
                    v = None
                    
                self._null_row.append(v)
            
        return self._null_row

    @property
    def null_dict(self):
        if self._null_row is None:
            self._null_row = {}
            for col in self.columns:
                if col.is_primary_key:
                    v = None
                elif col.default:
                    v = col.default
                else:
                    v = None
                    
                self._null_row[col.name] = v
            
        return self._null_row

    @property
    def header(self):
        '''Return an array of column names in the same order as the column definitions, to be used zip with
        a row when reading a CSV file

        >> row = dict(zip(table.header, row))

        '''

        return [ c.name for c in self.columns ]



    def _get_validator(self, and_join=True):
        '''Return a lambda function that, when given a row to this table, 
        returns true or false to indicate the validitity of the row
        
        :param and_join: If true, join multiple column validators with AND, other
        wise, OR
        :type and_join: Bool
        
        :rtype: a `LibraryDb` object
    
            
        '''

        f = prior = lambda row : True
        first = True
        for i,col in  enumerate(self.columns):

            if col.data.get('mandatory', False):
                default_value = col.default
                index = i
                
                if and_join:
                    f = lambda row, default_value=default_value, index=index, prior=prior: prior(row) and str(row[index]) != str(default_value)
                elif first:
                    # OR joins would either need the initial F to be 'false', or just don't use it
                    f = lambda row, default_value=default_value, index=index:  str(row[index]) != str(default_value)  
                else:
                    f = lambda row, default_value=default_value, index=index, prior=prior: prior(row) or str(row[index]) != str(default_value)
                            
                prior = f
                first = False
            
        return f
    
    def validate_or(self, values):

        if self._or_validator is None:
            self._or_validator = self._get_validator(and_join=False)
        
        return self._or_validator(values)
     
    def validate_and(self, values):

        if self._and_validator is None:
            self._and_validator = self._get_validator(and_join=True)
        
        return self._and_validator(values)
    
    def _get_hasher(self):
        '''Return a  function to generate a hash for the row'''
        import hashlib
 
        # Try making the hash set from the columns marked 'hash'
        indexes = [ i for i,c in enumerate(self.columns) if  
                   c.data.get('hash',False) and  not c.is_primary_key  ]
 
        # Otherwise, just use everything by the primary key. 
        if len(indexes) == 0:
            indexes = [ i for i,c in enumerate(self.columns) if not c.is_primary_key ]

        def hasher(values):
            m = hashlib.md5()
            for index in indexes: 
                x = values[index]
                try:
                    m.update(x.encode('utf-8')+'|') # '|' is so 1,23,4 and 12,3,4 aren't the same  
                except:
                    m.update(str(x)+'|') 
            return int(m.hexdigest()[:14], 16)
        
        return hasher
    
    def row_hash(self, values):
        '''Calculate a hash from a database row''' 
        
        if self._row_hasher is None:
            self._row_hasher = self._get_hasher()
            
        return self._row_hasher(values)
         
    @property
    def caster(self):
        '''Returns a function that takes a row that can be indexed by positions which returns a new
        row with all of the values cast to schema types. '''
        from ambry.transform import CasterTransformBuilder


        bdr = CasterTransformBuilder()

        for c in self.columns:
            bdr.append(c.name, c.python_type)

        return bdr


    def add_installed_name(self, name):
        self._append_string_to_list( 'installed_names', name)


    @property
    def linked_manifests(self): return self._get_link_array('manifests', File, File.ref)
    def link_manifest(self, f): return self._append_link('manifests', f.ref)
    def delink_manifest(self, f): return self._remove_link('manifests', f.ref)

event.listen(Table, 'before_insert', Table.before_insert)
event.listen(Table, 'before_update', Table.before_update)

class Config(Base):

    __tablename__ = 'config'

    d_vid = SAColumn('co_d_vid',String(16), primary_key=True)
    group = SAColumn('co_group',String(200), primary_key=True)
    key = SAColumn('co_key',String(200), primary_key=True)
    #value = SAColumn('co_value', PickleType(protocol=0))
    
    value = SAColumn('co_value', JSONAlchemy(Text()))

    source = SAColumn('co_source',String(200))

    def __init__(self,**kwargs):
        self.d_vid = kwargs.get("d_vid",None) 
        self.group = kwargs.get("group",None) 
        self.key = kwargs.get("key",None) 
        self.value = kwargs.get("value",None)
        self.source = kwargs.get("source",None) 

    def __repr__(self):
        return "<config: {},{},{} = {}>".format(self.d_vid, self.group, self.key, self.value)


class Partition(Base, LinkableMixin):
    __tablename__ = 'partitions'

    vid = SAColumn('p_vid',String(20), primary_key=True, nullable=False)
    id_ = SAColumn('p_id',String(20), nullable=False)
    name = SAColumn('p_name',String(200), nullable=False, index=True)
    vname = SAColumn('p_vname',String(200), unique=True, nullable=False, index=True)
    fqname = SAColumn('p_fqname',String(200), unique=True, nullable=False, index=True)
    ref = SAColumn('p_ref', String(200), index=True)
    cache_key = SAColumn('p_cache_key',String(200), unique=True, nullable=False, index=True)
    sequence_id = SAColumn('p_sequence_id',Integer)
    t_vid = SAColumn('p_t_vid',String(20),ForeignKey('tables.t_vid'), index=True)
    t_id = SAColumn('p_t_id',String(20))
    d_vid = SAColumn('p_d_vid',String(20),ForeignKey('datasets.d_vid'), index=True)
    d_id = SAColumn('p_d_id',String(20))
    time = SAColumn('p_time',String(20))
    space = SAColumn('p_space',String(50))
    grain = SAColumn('p_grain',String(50))
    variant = SAColumn('p_variant',String(50))
    format = SAColumn('p_format',String(50))
    segment = SAColumn('p_segment',Integer)
    min_key = SAColumn('p_min_key',BigIntegerType)
    max_key = SAColumn('p_max_key',BigIntegerType)
    count = SAColumn('p_count',Integer)
    state = SAColumn('p_state',String(50))
    data = SAColumn('p_data',MutationDict.as_mutable(JSONEncodedObj))

    installed = SAColumn('p_installed',String(100))

    __table_args__ = (
        #ForeignKeyConstraint( [d_vid, d_location], ['datasets.d_vid','datasets.d_location']),
        UniqueConstraint('p_sequence_id', 'p_t_vid', name='_uc_partitions_1'),
    )

    table = relationship('Table', backref='partitions', lazy='subquery')

    # Already have a 'partitions' replationship on Dataset
    #dataset = relationship('Dataset', backref='partitions')


    def __init__(self, dataset, **kwargs):

        self.vid = kwargs.get("vid", kwargs.get("id_", None))
        self.id_ = kwargs.get("id",kwargs.get("id_",None))
        self.name = kwargs.get("name",kwargs.get("name",None))
        self.vname = kwargs.get("vname",None)
        self.ref = kwargs.get("ref", None)
        self.fqname = kwargs.get("fqname",None)
        self.cache_key = kwargs.get("cache_key",None)
        self.sequence_id = kwargs.get("sequence_id",None)
        self.d_id = kwargs.get("d_id",None)
        self.space = kwargs.get("space",None)
        self.time = kwargs.get("time",None)
        self.t_id = kwargs.get("t_id",None)
        self.grain = kwargs.get('grain',None)
        self.format = kwargs.get('format',None)
        self.segment = kwargs.get('segment',None)
        self.data = kwargs.get('data',None)

        if dataset:
            self.d_id = dataset.id_
            self.d_vid = dataset.vid

        # See before_insert for setting self.vid and self.id_

        if self.t_id:
            don = ObjectNumber.parse(self.d_vid)
            ton = ObjectNumber.parse(self.t_id)
            self.t_vid = str(ton.rev( don.revision))

        assert self.cache_key is not None

        if True: # Debugging
            from partition import extension_for_format_name

            ext = extension_for_format_name(self.format)

            assert self.cache_key.endswith(ext)

    @property
    def identity(self):
        '''Return this partition information as a PartitionId'''
        from sqlalchemy.orm import object_session
        from identity import PartitionIdentity

        if self.dataset is None:
            # The relationship will be null until the object is committed
            s = object_session(self)

            ds = s.query(Dataset).filter(Dataset.id_ == self.d_id).one()
        else:
            ds = self.dataset

        d = dict(ds.dict.items() + self.dict.items())

        return PartitionIdentity.from_dict(d)

    @property
    def dict(self):

        d =  {
                 'id':self.id_,
                 'vid':self.vid,
                 'name':self.name,
                 'vname':self.vname,
                 'ref': self.ref,
                 'fqname':self.fqname,
                 'cache_key':self.cache_key,
                 'd_id': self.d_id,
                 'd_vid': self. d_vid,
                 't_id': self.t_id,
                 't_vid': self. t_vid,
                 'space':self.space,
                 'time':self.time,
                 'table': self.table.name if self.t_vid is not None else None,
                 'grain':self.grain,
                 'segment':self.segment,
                 'format': self.format if self.format else 'db',
                 'count': self.count,
                 'min_key': self.min_key,
                 'max_key': self.max_key
                }

        for k in self.data:
            assert k not in d
            d[k] = self.data[k]


        return d

    @property
    def nonull_dict(self):
        return {k: v for k, v in self.dict.items() if v}

    @property
    def insertable_dict(self):
        return {('p_' + k).strip('_'): v for k, v in self.dict.items()}


    def __repr__(self):
        return "<{} partition: {}>".format(self.format, self.vname)

    def set_ids(self, sequence_id):
        from identity import Identity

        if not self.vid or not self.id_:

            self.sequence_id = sequence_id

            don = ObjectNumber.parse(self.d_vid)
            pon = PartitionNumber(don, self.sequence_id)

            self.vid = str(pon)
            self.id_ = str(pon.rev(None))

        self.fqname = Identity._compose_fqname(self.vname,self.vid)

    # For linking partitions to manifests
    @property
    def linked_manifests(self): return self._get_link_array('manifests', File, File.ref)
    def link_manifest(self, f): return self._append_link('manifests', f.ref)
    def delink_manifest(self, f): return self._remove_link('manifests', f.ref)

    @property
    def linked_stores(self): return self._get_link_array('stores', File, File.ref)
    def link_store(self, f): return self._append_link('stores', f.ref)
    def delink_store(self, f): return self._remove_link('stores', f.ref)

    @staticmethod
    def before_insert(mapper, conn, target):
        '''event.listen method for Sqlalchemy to set the sequence for this
        object and create an ObjectNumber value for the id_'''
        from identity import Identity

        if target.sequence_id is None:
            # These records can be added in an multi-process environment, we
            # we need exclusive locking here, where we don't for other sequence ids.
            conn.execute("BEGIN IMMEDIATE")
            sql = text('''SELECT max(p_sequence_id)+1 FROM Partitions WHERE p_d_id = :did''')

            max_id, = conn.execute(sql, did=target.d_id).fetchone()

            if not max_id:
                max_id = 1

            target.sequence_id = max_id

        target.set_ids(target.sequence_id)

        Partition.before_update(mapper, conn, target)


    @staticmethod
    def before_update(mapper, conn, target):
        '''Set the column id number based on the table number and the
        sequence id for the column'''
        if not target.id_:
            dataset = ObjectNumber.parse(target.d_id)
            target.id_ = str(PartitionNumber(dataset, target.sequence_id))

event.listen(Partition, 'before_insert', Partition.before_insert)
event.listen(Partition, 'before_update', Partition.before_update)

class File(Base, SavableMixin, LinkableMixin):
    __tablename__ = 'files'

    oid = SAColumn('f_id',Integer, primary_key=True, nullable=False)
    path = SAColumn('f_path',Text, nullable=False)
    ref = SAColumn('f_ref', Text, index=True)
    type_ = SAColumn('f_type', Text)
    source_url = SAColumn('f_source_url',Text)
    process = SAColumn('f_process',Text)
    state = SAColumn('f_state',Text)
    hash = SAColumn('f_hash',Text)
    modified = SAColumn('f_modified',Integer)
    size = SAColumn('f_size',BigIntegerType)
    group = SAColumn('f_group',Text)
    priority = SAColumn('f_priority', Integer)

    data = SAColumn('f_data',MutationDict.as_mutable(JSONEncodedObj))

    content = SAColumn('f_content', Binary)

    __table_args__ = (
        UniqueConstraint('f_path', 'f_type', 'f_group', name='u_type_path'),
        UniqueConstraint('f_ref', 'f_type', 'f_group', name='u_ref_path'),
    )


    def __init__(self,**kwargs):
        self.oid = kwargs.get("oid",None)
        self.path = kwargs.get("path",None)
        self.source_url = kwargs.get("source_url",None)
        self.process = kwargs.get("process",None)
        self.state = kwargs.get("state",None)
        self.modified = kwargs.get("modified",None)
        self.size = kwargs.get("size",None)
        self.group = kwargs.get("group",None)
        self.ref = kwargs.get("ref",None)
        self.hash = kwargs.get("hash", None)
        self.type_ = kwargs.get("type",kwargs.get("type_",None))

        self.data = kwargs.get('data',None)
        self.priority = kwargs.get('priority', 0)
        self.content = kwargs.get('content', None)

    def __repr__(self):
        return "<file: {}; {}>".format(self.path, self.state)

    def update(self, f):
        """Copy another files properties into this one. """

        for p in self.__mapper__.attrs:

            if p.key == 'oid':
                continue
            try:
                setattr(self, p.key, getattr(f, p.key))

            except AttributeError:
                # The dict() method copies data property values into the main dict,
                # and these don't have associated class properties.
                continue



    @property
    def dict(self):

        d =   dict((col, getattr(self, col)) for col
                     in ['oid','path', 'ref',  'type_',  'source_url', 'process', 'state',
                         'hash', 'modified', 'size', 'group',  'priority'])

        if self.data:
            for k in self.data:
                assert k not in d
                d[k] = self.data[k]

        return d

    @property
    def record_dict(self):
        '''Like dict, but does not move data items into the top level'''
        return { p.key: getattr(self, p.key) for p in self.__mapper__.attrs}

    @property
    def insertable_dict(self):
        """Like record_dict, but prefixes all of the keys with 'f_', so it can be used in inserts """
        # .strip('_') is for type_
        return {'f_'+p.key.strip('_'): getattr(self, p.key) for p in self.__mapper__.attrs }

    ## These partition and file acessors were originally implemented as many-to-many tables
    ## and probably should still be, but this is easier to understand than dealing with Sqlalchemy cascaded
    ## deletes on M-to-M secondary tables.

    @property
    def linked_partitions(self): return self._get_link_array('partitions', Partition, Partition.vid)
    def link_partition(self, p): return self._append_link('partitions', p.vid)
    def delink_partition(self, p): return self._remove_link('partitions', p.vid)

    @property
    def linked_tables(self): return self._get_link_array('tables', Table, Table.vid)
    def link_table(self, t): return self._append_link('tables', t.vid)
    def delink_table(self, t): return self._remove_link('tables', t.vid)

    @property
    def linked_manifests(self): return self._get_link_array('manifests', File, File.ref)
    def link_manifest(self, f):
        assert self.group != 'manifest'
        return self._append_link('manifests', f.ref)
    def delink_manifest(self, f): return self._remove_link('manifests', f.ref)

    @property
    def linked_stores(self): return self._get_link_array('stores', File, File.ref)
    def link_store(self, f): return self._append_link('stores', f.ref)
    def delink_store(self, f): return self._remove_link('stores', f.ref)

class Code(Base, SavableMixin, LinkableMixin):
    """Code entries for variables"""
    __tablename__ = 'codes'

    oid = SAColumn('cd_id',Integer, primary_key=True, nullable=False)

    t_vid = SAColumn('cd_t_vid', String(20), ForeignKey('tables.t_vid'), index=True)
    table = relationship('Table', backref='codes', lazy='subquery')

    c_vid = SAColumn('cd_c_vid', String(20), ForeignKey('columns.c_vid'), index=True)
    column = relationship('Column', backref='_codes', lazy='subquery')

    key = SAColumn('cd_skey', String(20), nullable=False, index=True) # String version of the key, the value in the dataset
    ikey = SAColumn('cd_ikey', Integer, index=True) # Set only if the key is actually an integer

    value = SAColumn('cd_value',Text, nullable=False) # The value the key maps to
    description = SAColumn('f_description', Text, index=True)

    data = SAColumn('co_data',MutationDict.as_mutable(JSONEncodedObj))

    __table_args__ = (
        UniqueConstraint('cd_c_vid', 'cd_skey', name='u_code_col_key'),
    )

    def __init__(self,**kwargs):

        for p in self.__mapper__.attrs:
            if p.key in kwargs:
                setattr(self, p.key, kwargs[p.key])
                del kwargs[p.key]

        if self.data:
            self.data.update(kwargs)

    def __repr__(self):
        return "<code: {}->{} >".format(self.key, self.value)

    def update(self, f):
        """Copy another files properties into this one. """

        for p in self.__mapper__.attrs:

            if p.key == 'oid':
                continue
            try:
                setattr(self, p.key, getattr(f, p.key))

            except AttributeError:
                # The dict() method copies data property values into the main dict,
                # and these don't have associated class properties.
                continue

    @property
    def dict(self):

        d = { p.key: getattr(self, p.key) for p in self.__mapper__.attrs if p.key not in  ('data','column','table') }

        if self.data:
            for k in self.data:
                assert k not in d
                d[k] = self.data[k]

        return d

    @property
    def insertable_dict(self):
        return { ('cd_'+k).strip('_'):v for k,v in self.dict.items()}








