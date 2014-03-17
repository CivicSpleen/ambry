"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
import datetime
import sqlalchemy
from sqlalchemy import orm
from sqlalchemy import event
from sqlalchemy import Column as SAColumn, Integer, BigInteger, Boolean, UniqueConstraint, ForeignKeyConstraint
from sqlalchemy import Float as Real,  Text, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.types import TypeDecorator, TEXT, PickleType
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.mutable import Mutable
from sqlalchemy.exc import OperationalError
from util import Constant
from identity import LocationRef

from sqlalchemy.sql import text
from ambry.identity import  DatasetNumber, ColumnNumber
from ambry.identity import TableNumber, PartitionNumber, ObjectNumber

import json

SCHEMA_VERSION = 11

Base = declarative_base()

class JSONEncodedObj(TypeDecorator):
    "Represents an immutable structure as a json-encoded string."

    impl = TEXT

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value)
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
        

class Dataset(Base):
    __tablename__ = 'datasets'


    LOCATION = Constant()
    LOCATION.LIBRARY = LocationRef.LOCATION.LIBRARY
    LOCATION.PARTITION = LocationRef.LOCATION.PARTITION
    LOCATION.SREPO = LocationRef.LOCATION.SREPO
    LOCATION.SOURCE = LocationRef.LOCATION.SOURCE
    LOCATION.REMOTE =  LocationRef.LOCATION.REMOTE
    LOCATION.UPSTREAM = LocationRef.LOCATION.UPSTREAM


    vid = SAColumn('d_vid',String(20), primary_key=True)
    location = SAColumn('d_location', String(5), primary_key=True, default=LOCATION.LIBRARY)
    id_ = SAColumn('d_id',String(20), )
    name = SAColumn('d_name',String(200),  nullable=False)
    vname = SAColumn('d_vname',String(200),  nullable=False)
    fqname = SAColumn('d_fqname',String(200),  nullable=False)
    cache_key = SAColumn('d_cache_key',String(200),  nullable=False)
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



    __table_args__ = (
        UniqueConstraint('d_vid', 'd_location', name='u_vid_location'),
        UniqueConstraint('d_fqname', 'd_location', name='u_fqname_location'),
        UniqueConstraint('d_cache_key', 'd_location', name='u_cache_location'),
    )


    def __init__(self,**kwargs):
        self.id_ = kwargs.get("oid",kwargs.get("id",kwargs.get("id_", None)) )
        self.vid = kwargs.get("vid", None)
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
        return {
                'id':self.id_, 
                'vid':self.vid,
                'location': self.location,
                'name':self.name,
                'vname':self.fqname, 
                'fqname':self.vname,
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
        
def _clean_flag( in_flag):
    
    if in_flag is None or in_flag == '0':
        return False

    return bool(in_flag)

class Column(Base):
    __tablename__ = 'columns'

    vid = SAColumn('c_vid',String(20), primary_key=True)
    id_ = SAColumn('c_id',String(20))
    sequence_id = SAColumn('c_sequence_id',Integer)
    t_vid = SAColumn('c_t_vid',String(20),ForeignKey('tables.t_vid'))
    t_id = SAColumn('c_t_id',String(20))
    name = SAColumn('c_name',Text)
    altname = SAColumn('c_altname',Text)
    datatype = SAColumn('c_datatype',Text)
    size = SAColumn('c_size',Integer)
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
    data = SAColumn('c_data',MutationDict.as_mutable(JSONEncodedObj))

    is_primary_key = SAColumn('c_is_primary_key',Boolean, default = False)
    
    _is_foreign_key = SAColumn('c_is_foreign_key',Boolean, default = False)
    _foreign_key = SAColumn('c_foreign_key',String(16), nullable=True)
    
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
        DATATYPE_INTEGER64:(sqlalchemy.types.BigInteger,long,'INTEGER64'),
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
        if self.type_is_time():
            import dateutil.parser
            dt = dateutil.parser.parse(v)
           
            if self.datatype == Column.DATATYPE_TIME:
                dt = dt.time()
            if not isinstance(dt, self.python_type):
                raise TypeError('{} was parsed to {}, expected {}'.format(v, type(dt), self.python_type))
               
        else:
            return self.python_type(v)

    @property
    def schema_type(self):
        return self.types[self.datatype][2]

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

    @property
    def foreign_key(self):
        try:
            return self._foreign_key
        except OperationalError:
            try:
                return self._is_foreign_key if bool(self._is_foreign_key) else None
            except TypeError: # Something about requiring an integer
                return False
    
    @foreign_key.setter
    def foreign_key(self, value):
        try:
            self._foreign_key  = value
        except OperationalError:
            self._is_foreign_key  = value     
       
    @property
    def is_foreign_key(self): raise NotImplementedError("Use foreign_key instead")
    
    @is_foreign_key.setter
    def is_foreign_key(self, value): raise NotImplementedError("Use foreign_key instead")
        
    def __init__(self,table, **kwargs):

        self.sequence_id = kwargs.get("sequence_id",len(table.columns)+1) 
        self.name = kwargs.get("name",None) 
        self.altname = kwargs.get("altname",None) 
        self.is_primary_key = _clean_flag(kwargs.get("is_primary_key",False))
        self.datatype = kwargs.get("datatype",None) 
        self.size = kwargs.get("size",None) 
        self.precision = kwargs.get("precision",None) 
        self.width = kwargs.get("width",None)    
        self.sql = kwargs.get("sql",None)      
        self.flags = kwargs.get("flags",None) 
        self.description = kwargs.get("description",None) 
        self.keywords = kwargs.get("keywords",None) 
        self.measure = kwargs.get("measure",None) 
        self.units = kwargs.get("units",None) 
        self.universe = kwargs.get("universe",None) 
        self.scale = kwargs.get("scale",None) 
        self.data = kwargs.get("data",None) 

        # the table_name attribute is not stored. It is only for
        # building the schema, linking the columns to tables. 
        self.table_name = kwargs.get("table_name",None) 

        if not self.name:
            raise ValueError('Column must have a name')

        self.t_id = table.id_
        self.t_vid = table.vid
        ton = ObjectNumber.parse(table.vid)
        con = ColumnNumber(ton, self.sequence_id)
        self.vid = str(con)
        self.id = str(con.rev(None))

    @property
    def dict(self):
        x =  {k:v for k,v in self.__dict__.items() if k in ['id','vid','sequence_id', 't_vid', 'name', 'description', 'keywords', 'datatype', 'size', 'is_primary_key', 'data']}
        x['schema_type'] = self.schema_type
        return x
    
    @staticmethod
    def mangle_name(name):
        import re
        try:
            return re.sub('[^\w_]','_',name).lower()
        except TypeError:
            raise TypeError('Not a valid type for name '+str(type(name)))

    @staticmethod
    def before_insert(mapper, conn, target):
        '''event.listen method for Sqlalchemy to set the seqience_id for this  
        object and create an ObjectNumber value for the id_'''
        
        if target.sequence_id is None:
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
 
class Table(Base):
    __tablename__ ='tables'

    vid = SAColumn('t_vid',String(20), primary_key=True)
    id_ = SAColumn('t_id',String(20), primary_key=False)
    d_id = SAColumn('t_d_id',String(20))
    d_vid = SAColumn('t_d_vid',String(20), nullable = False)
    d_location = SAColumn('t_d_location',String(5), nullable = False, default = Dataset.LOCATION.LIBRARY)
    sequence_id = SAColumn('t_sequence_id',Integer, nullable = False)
    name = SAColumn('t_name',String(200), nullable = False)
    altname = SAColumn('t_altname',Text)
    description = SAColumn('t_description',Text)
    universe = SAColumn('t_universe',String(200))
    keywords = SAColumn('t_keywords',Text)
    data = SAColumn('t_data',MutationDict.as_mutable(JSONEncodedObj))
    installed = SAColumn('t_installed',String(100))
    
    __table_args__ = (
        ForeignKeyConstraint([d_vid, d_location], ['datasets.d_vid', 'datasets.d_location']),
        UniqueConstraint('t_sequence_id', 't_d_vid', name='_uc_tables_1'),
        UniqueConstraint('t_name', 't_d_vid', name='_uc_tables_2'),
                     )
    
    columns = relationship(Column, backref='table', cascade="all, delete-orphan")

    def __init__(self,dataset, **kwargs):

        self.sequence_id = kwargs.get("sequence_id",None)  
        self.name = kwargs.get("name",None) 
        self.vname = kwargs.get("vname",None) 
        self.altname = kwargs.get("altname",None) 
        self.description = kwargs.get("description",None)
        self.universe = kwargs.get("universe", None)
        self.keywords = kwargs.get("keywords",None) 
        self.data = kwargs.get("data",None) 
        
        self.d_id = dataset.id_
        self.d_vid = dataset.vid
        don = ObjectNumber.parse(dataset.vid)
        ton = TableNumber(don, self.sequence_id)
      
        self.vid = str(ton)
        self.id_ = str(ton.rev(None))

        if self.name:
            self.name = self.mangle_name(self.name)

        self.init_on_load()

    @property
    def dict(self):
        return {k:v for k,v in self.__dict__.items() if k in ['id_','vid', 'sequence_id', 'name', 
                                                              'vname', 'description', 'keywords', 'installed', 'data']}
    
    @property
    def help(self):
        
       
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

    @staticmethod
    def mangle_name(name):
        import re
        try:
            return re.sub('[^\w_]','_',name.strip()).lower()
        except TypeError:
            raise TypeError('Not a valid type for name '+str(type(name)))

    @property
    def oid(self):   
        return TableNumber(self.d_id, self.sequence_id)

    def add_column(self, name, **kwargs):
        '''Add a column to the table, or update an existing one '''

        import sqlalchemy.orm.session
        from sqlalchemy.orm.exc import NoResultFound
        from dbexceptions import ConfigurationError
        
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

            if extant:
                excludes.append('sequence_id')

            if key[0] != '_' and key not in excludes :
                setattr(row, key, value)

            if isinstance(value, basestring) and len(value) == 0:
                if key == 'is_primary_key':
                    value = False
                    setattr(row, key, value)

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
            '''Using the size values for the columsn for the table, construct a
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

    @property
    def vid_enc(self):
        '''vid, urlencoded'''
        return self.vid.replace('/','|')

event.listen(Table, 'before_insert', Table.before_insert)
event.listen(Table, 'before_update', Table.before_update)

class Config(Base):
    
    ROOT_CONFIG_NAME = 'a0'
    ROOT_CONFIG_NAME_V = 'a0/001'
    
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
     

class File(Base, SavableMixin):
    __tablename__ = 'files'

    oid = SAColumn('f_id',Integer, primary_key=True, nullable=False)
    path = SAColumn('f_path',Text, nullable=False)
    ref = SAColumn('f_ref', Text)
    type_ = SAColumn('f_type', Text)
    source_url = SAColumn('f_source_url',Text)
    process = SAColumn('f_process',Text)
    state = SAColumn('f_state',Text)
    content_hash = SAColumn('f_hash',Text)
    modified = SAColumn('f_modified',Integer)
    size = SAColumn('f_size',BigInteger)
    group = SAColumn('f_group',Text)


    data = SAColumn('f_data',MutationDict.as_mutable(JSONEncodedObj))

    __table_args__ = (
        UniqueConstraint('f_path', 'f_type', name='u_type_path'),
        UniqueConstraint('f_ref', 'f_type', name='u_ref_path'),
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
        self.type_ = kwargs.get("type",kwargs.get("type_",None))
        self.content_hash = kwargs.get("content_hash",None) 
        self.data = kwargs.get('data',None)
      
    def __repr__(self):
        return "<file: {}; {}>".format(self.path, self.state)

    @property
    def dict(self):

        return  dict((col, getattr(self, col)) for col 
                     in ['path', 'ref',  'type_',  'source_url', 'process', 'state', 'content_hash', 'modified', 'size', 'group', 'data'])
 

class Partition(Base):
    __tablename__ = 'partitions'

    vid = SAColumn('p_vid',String(20), primary_key=True, nullable=False)
    id_ = SAColumn('p_id',String(20), nullable=False)
    name = SAColumn('p_name',String(200), nullable=False)
    vname = SAColumn('p_vname',String(200), unique=True, nullable=False)
    fqname = SAColumn('p_fqname',String(200), unique=True, nullable=False)
    cache_key = SAColumn('p_cache_key',String(200), unique=True, nullable=False)
    sequence_id = SAColumn('p_sequence_id',Integer)
    t_vid = SAColumn('p_t_vid',String(20),ForeignKey('tables.t_vid'))
    t_id = SAColumn('p_t_id',String(20))
    d_vid = SAColumn('p_d_vid',String(20))
    d_location = SAColumn('p_d_location',String(5),default = Dataset.LOCATION.LIBRARY)
    d_id = SAColumn('p_d_id',String(20))
    time = SAColumn('p_time',String(20))
    space = SAColumn('p_space',String(50))
    grain = SAColumn('p_grain',String(50))
    variant = SAColumn('p_variant',String(50))
    format = SAColumn('p_format',String(50))
    segment = SAColumn('p_segment',Integer)
    min_key = SAColumn('p_min_key',BigInteger)
    max_key = SAColumn('p_max_key',BigInteger)
    count = SAColumn('p_count',Integer)
    state = SAColumn('p_state',String(50))
    data = SAColumn('p_data',MutationDict.as_mutable(JSONEncodedObj))
    installed = SAColumn('p_installed',String(100))

    __table_args__ = (
        ForeignKeyConstraint( [d_vid, d_location], ['datasets.d_vid','datasets.d_location']),
        UniqueConstraint('p_sequence_id', 'p_t_vid', name='_uc_partitions_1'))

    table = relationship('Table', backref='partitions', lazy='subquery')
    # Already have a 'partitions' replationship on Dataset
    #dataset = relationship('Dataset', backref='partitions')



    def __init__(self,dataset, **kwargs):
        self.id_ = kwargs.get("id",kwargs.get("id_",None)) 
        self.name = kwargs.get("name",kwargs.get("name",None)) 
        self.vname = kwargs.get("vname",None) 
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

        return {
                 'id':self.id_, 
                 'vid':self.vid,
                 'name':self.name,
                 'vname':self.fqname, 
                 'fqname':self.vname,
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
                 'format': self.format if self.format else 'db'
                }
      
    def __repr__(self):
        return "<{} partition: {}>".format(self.format, self.vname)

    @staticmethod
    def before_insert(mapper, conn, target):
        '''event.listen method for Sqlalchemy to set the sequence for this  
        object and create an ObjectNumber value for the id_'''
        from identity import Identity
        
        if target.sequence_id is None:
            sql = text('''SELECT max(p_sequence_id)+1 FROM Partitions WHERE p_d_id = :did''')
    
            max_id, = conn.execute(sql, did=target.d_id).fetchone()
      
            if not max_id:
                max_id = 1
                
            target.sequence_id = max_id
            
            
        don = ObjectNumber.parse(target.d_vid)
        pon = PartitionNumber(don, target.sequence_id)
        
        target.vid = str(pon)
        target.id_ = str(pon.rev(None))
        target.fqname = Identity._compose_fqname(target.vname,target.vid)

        Partition.before_update(mapper, conn, target)


    @staticmethod
    def before_update(mapper, conn, target):
        '''Set the column id number based on the table number and the 
        sequence id for the column'''
        dataset = ObjectNumber.parse(target.d_id)
        target.id_ = str(PartitionNumber(dataset, target.sequence_id))
        
    @staticmethod
    def after_load(target, context):
        '''Move older way of noting format types to the newer format var'''
        if not target.format:
            if 'db_type' in target.data:
                target.format = target.data['db_type']
                del target.data['db_type']
            else:
                target.format = 'db'

        
event.listen(Partition, 'before_insert', Partition.before_insert)
event.listen(Partition, 'before_update', Partition.before_update)
event.listen(Partition, 'load', Partition.after_load)