
"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from . import DatabaseInterface #@UnresolvedImport
from .inserter import  ValueInserter
import os 
import logging
from ambry.util import get_logger
from ..database.inserter import SegmentedInserter, SegmentInserterFactory
from contextlib import contextmanager
             

class RelationalDatabase(DatabaseInterface):
    '''Represents a Sqlite database'''

    DBCI = {
            'postgis':'postgresql+psycopg2://{user}:{password}@{server}{colon_port}/{name}', # Stored in the ambry module.
            'postgres':'postgresql+psycopg2://{user}:{password}@{server}{colon_port}/{name}', # Stored in the ambry module.
            'sqlite':'sqlite:///{name}',
            'spatialite':'sqlite:///{name}', # Only works if you properly install spatialite. 
            'mysql':'mysql://{user}:{password}@{server}{colon_port}/{name}'
            }
    
    dsn = None

    def __init__(self,  driver=None, server=None, dbname = None, username=None, password=None, port=None,  **kwargs):

        '''Initialize the a database object
        
        Args:
            bundle. a Bundle object
            
            base_path. Path to the database file. If None, uses the name of the
            bundle, in the bundle build director. 
            
            post_create. A function called during the create() method. has
            signature post_create(database)
       
        '''
        self.driver = driver
        self.server = server
        self.dbname = dbname
        self.username = username
        self.password = password

        self.enable_delete = False

        if port:
            self.colon_port = ':'+str(port)
        else:
            self.colon_port = ''
                
        self._engine = None

        self._connection = None


        self._table_meta_cache = {}

        self.dsn_template = self.DBCI[self.driver]
        self.dsn = self.dsn_template.format(user=self.username, password=self.password, 
                    server=self.server, name=self.dbname, colon_port=self.colon_port)


        self.logger = get_logger(__name__)
        self.logger.setLevel(logging.INFO) 
        
        self._unmanaged_session = None

    def log(self,message):
        self.logger.info(message)
    
    def error(self, message):
        self.logger.error(message)

    def create(self):

        self._create()
        
        return True
    
    @property
    def version(self):
        raise NotImplemented()
    
    def exists(self):
        
        try:
            self.connection
        except:
            return False
        

        if self.is_empty():
            return False
        
        
        return True
    
    def is_empty(self):
        
        if not 'config' in self.inspector.get_table_names():
            return True
        else:
            return False

    def _create(self):
        """Create the database from the base SQL"""
        from ambry.orm import  Config

        if not self.exists():

            self.require_path()

            # For Sqlite, this will create an empty database.
            self.get_connection(check_exists=False)

            tables = [ Config ]

            for table in tables:
                table.__table__.create(bind=self.engine)

            return True #signal did create

        return False # signal didn't create

    def _post_create(self):
        # call the post create function
        from ..orm import Config
        from datetime import datetime
        
        if not 'config' in self.inspector.get_table_names():
            Config.__table__.create(bind=self.engine) #@UndefinedVariable
        
        self.set_config_value(Config.ROOT_CONFIG_NAME_V, 'process','dbcreated', datetime.now().isoformat(), session=self.creation_session )
        
    def post_create(self):
        '''Call all implementations of _post_create in this object's class heirarchy'''
        import inspect

        for cls in inspect.getmro(self.__class__):
            for n,f in inspect.getmembers(cls,lambda m: inspect.ismethod(m) and m.__func__ in m.im_class.__dict__.values()):
                if n == '_post_create':
                    f(self)


    def drop(self):
        if not self.enable_delete:
            raise Exception("Deleting not enabled")

        for table in reversed(self.metadata.sorted_tables):  # sorted by foreign key dependency

            if table.name not in ['spatial_ref_sys']:
                table.drop(self.engine, checkfirst=True)

    def delete(self):
        self.drop()

    def drop_table(self, table_name, use_id = False):
        table = self.table(table_name)
        
        table.drop(self.engine)


    @property
    def engine(self):
        '''return the SqlAlchemy engine for this database'''
        from sqlalchemy import create_engine
        import sqlite3

        if not self._engine:
            self.require_path()
            self._engine = create_engine(self.dsn,
                                         connect_args={
                                             'detect_types': sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES},
                                         native_datetime=True,
                                         echo=False)

            self._on_create_engine(self._engine)

        return self._engine

    @property
    def connection(self):
        '''Return an SqlAlchemy connection'''

        return self.get_connection()


    def get_connection(self, check_exists=True):
        '''Return an SqlAlchemy connection. check_exists is ignored'''

        if not self._connection:
            try:
                self._connection = self.engine.connect()
                self._on_create_connection(self._connection)
            except Exception as e:
                self.error("Failed to open: '{}': {} ".format(self.dsn, e))
                raise
            
        return self._connection


    def require_path(self):
        '''Used in engine but only implemented for sqlite'''
        pass

    def _on_create_connection(self, connection):
        pass

    def _on_create_engine(self, engine):
        pass

    @property
    def unmanaged_session(self):
        

        if not self._unmanaged_session:
            from sqlalchemy.orm import sessionmaker
            Session = sessionmaker(bind=self.engine,autocommit=False, autoflush=False)
            self._unmanaged_session =  Session()

        return self._unmanaged_session

    @property
    def session(self):
        return self.unmanaged_session
    
    @property
    def creation_session(self):
        '''Writable Session to be used during databasecreation'''

        from sqlalchemy.orm import sessionmaker
        Session = sessionmaker(bind=self.engine,autocommit=False, autoflush=False)
        return Session()



    @property
    def metadata(self):
        '''Return an SqlAlchemy MetaData object, bound to the engine'''
        
        from sqlalchemy import MetaData   
        meta = MetaData(bind=self.engine)
        meta.reflect(bind=self.engine)
    
        return meta
    
    @property
    def inspector(self):
        from sqlalchemy.engine.reflection import Inspector

        return Inspector.from_engine(self.engine)
 
   
    def open(self):
        # Fetching the connection objects the database
        # This isn't necessary for Sqlite databases. 
        return self.connection
   
    def close(self):

        if self._connection:
            self._connection.close()
            self._connection = None

    def clean_table(self, table):

        if isinstance(table, basestring):
            self.connection.execute('DELETE FROM {} '.format(table))
        else:
            self.connection.execute('DELETE FROM {} '.format(table.name))
            
        self.commit()

    def create_table(self, table_name=None, table_meta=None):
        '''Create a table that is defined in the table table
        
        This method will issue the DDL to create a table that is defined
        in the meta data tables, of which the 'table' table ho;d information
        about tables.
        
        Args:
            table_name. The name of the table to create
        
        '''
        
        if not table_name in self.inspector.get_table_names():
            if not table_meta:
                table_meta, table = self.bundle.schema.get_table_meta(table_name) #@UnusedVariable
                
            table_meta.create(bind=self.engine)
            
            if not table_name in self.inspector.get_table_names():
                raise Exception("Don't have table "+table_name)
             
    def tables(self):
        
        return self.metadata.sorted_tables
                   
    def table(self, table_name): 
        '''Get table metadata from the database''' 
        from sqlalchemy import Table
        
        table = self._table_meta_cache.get(table_name, False)
        
        if table is not False:
            r =  table
        else:
            metadata = self.metadata
            table = Table(table_name, metadata, autoload=True)
            self._table_meta_cache[table_name] = table
            r =  table

        return r

    def X_inserter(self,table_name, **kwargs):
        '''Creates an inserter for a database, but which may not have an associated bundle, 
        as, for instance, a warehouse. '''
        
        from sqlalchemy.schema import Table

        table = Table(table_name, self.metadata, autoload=True, autoload_with=self.engine)

        return ValueInserter(self,None, table , **kwargs)


    class csv_partition_factory(SegmentInserterFactory):
       
        def __init__(self, bundle, db, table):
            self.db = db
            self.table = table
            self.bundle = bundle
        
        def next_inserter(self, seg): 
            ident = self.db.partition.identity
            ident.segment = seg
            
            if self.bundle.has_session:
                p = self.db.bundle.partitions.find_or_new_csv(**ident.dict)
            else:
                p = self.db.bundle.partitions.find_or_new_csv(**ident.dict)
            return p.inserter(self.table)

    def csvinserter(self, table_or_name=None,segment_rows=200000,  **kwargs):
        '''Return an inserter that writes to segmented CSV partitons'''
        
        sif = self.csv_partition_factory(self.bundle, self, table_or_name)

        return SegmentedInserter(segment_size=segment_rows, segment_factory = sif,  **kwargs)



    def set_config_value(self, d_vid, group, key, value, session=None):
        from ambry.orm import Config as SAConfig
        
        if group == 'identity' and d_vid != SAConfig.ROOT_CONFIG_NAME_V:
            raise ValueError("Can't set identity group from this interface. Use the dataset")

      
        key = key.strip('_')
  

        session = self.session if not session else session
  
        session.query(SAConfig).filter(SAConfig.group == group,
                                 SAConfig.key == key,
                                 SAConfig.d_vid == d_vid).delete()
        

        o = SAConfig(group=group, key=key,d_vid=d_vid,value = value)
        session.add(o)
        session.commit()



    def get_config_value(self, d_vid, group, key):
        from ambry.orm import Config as SAConfig


        key = key.strip('_')
  
        return self.session.query(SAConfig).filter(SAConfig.group == group,
                                 SAConfig.key == key,
                                 SAConfig.d_vid == d_vid).first()



        

class RelationalBundleDatabaseMixin(object):
    
    bundle = None
    
    def _init(self, bundle, **kwargs):   

        self.bundle = bundle 

    def _create(self):
        """Create the database from the base SQL"""
        from ambry.orm import  Dataset, Partition, Table, Column, File
        from ..identity import Identity
        from sqlalchemy.orm import sessionmaker


        tables = [ Dataset, Partition, Table, Column, File ]

        for table in tables:
            table.__table__.create(bind=self.engine)

        # Create the Dataset record
        session = self.creation_session
        
        ds = Dataset(**self.bundle.config.identity)

        ident = Identity.from_dict(self.bundle.config.identity)
        
        ds.name = ident.sname
        ds.vname = ident.vname
        ds.fqname = ident.fqname
        ds.cache_key = ident.cache_key

        try:
            ds.creator = self.bundle.config.about.author
        except:
            ds.creator = 'n/a'

        session.add(ds)
        session.commit()

    def rewrite_dataset(self):
        from ..orm import Dataset
        # Now patch up the Dataset object


        ds = Dataset(**self.bundle.identity.dict)
        ds.name = self.bundle.identity.sname
        ds.vname = self.bundle.identity.vname
        ds.fqname = self.bundle.identity.fqname

        try:
            ds.creator = self.bundle.config.about.author
        except:
            ds.creator = 'n/a'

        self.session.merge(ds)

    def _post_create(self):
        from ..orm import Config
        from sqlalchemy.orm import sessionmaker

        self.set_config_value(self.bundle.identity.vid, 'info','type', 'bundle', session=self.creation_session )
        self.set_config_value(Config.ROOT_CONFIG_NAME_V, 'bundle','vname', self.bundle.identity.vname, session=self.creation_session  )
        self.set_config_value(Config.ROOT_CONFIG_NAME_V, 'bundle','vid', self.bundle.identity.vid , session=self.creation_session )

class RelationalPartitionDatabaseMixin(object):
    
    bundle = None
    
    def _init(self, bundle, partition, **kwargs):   

        self.partition = partition
        self.bundle = bundle 

    def _post_create(self):
        from ..orm import Config

        if not 'config' in self.inspector.get_table_names():
            Config.__table__.create(bind=self.engine) #@UndefinedVariable
            
        self.set_config_value(self.bundle.identity.vid, 'info','type', 'partition' )
        self.set_config_value(Config.ROOT_CONFIG_NAME_V, 'bundle','vname', self.bundle.identity.vname )
        self.set_config_value(Config.ROOT_CONFIG_NAME_V, 'bundle','vid', self.bundle.identity.vid )
        self.set_config_value(Config.ROOT_CONFIG_NAME_V, 'partition','vname', self.partition.identity.vname )
        self.set_config_value(Config.ROOT_CONFIG_NAME_V, 'partition','vid', self.partition.identity.vid )
        self.session.commit()






