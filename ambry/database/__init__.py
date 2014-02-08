"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from __future__ import absolute_import
from ..dbexceptions import ConfigurationError
from collections import namedtuple

def new_database(config, bundle=None, class_=None):

    service = config['driver']
    
    if 'class' in config and class_ and config['class'] != class_:
        raise ConfigurationError("Mismatch in class configuration {} != {}".format(config['class'], class_))
    
    class_ = config['class'] if 'class' in config else class_

    k = (service,class_)


    if k == ('sqlite',None):
        from .sqlite import SqliteBundleDatabase
        return SqliteBundleDatabase(bundle=bundle, **config)
    
    elif k == ('mysql',None):   
        raise NotImplemented() 
        
    elif k == ('postgres',None):   
        from .relational import RelationalDatabase
        return RelationalDatabase(**config)
    
    elif k == ('postgis',None):   
        from .postgis import PostgisDatabase
        return PostgisDatabase(**config)
    
    elif k == ('sqlite','bundle'):
        from .sqlite import SqliteBundleDatabase
        return SqliteBundleDatabase(bundle=bundle, **config)
    
    elif k == ('sqlite','warehouse'):
        from .sqlite import SqliteWarehouseDatabase
        dbname = config['dbname']
        del config['dbname']
        return SqliteWarehouseDatabase(dbname, **config)

    elif k == ('spatialite', 'warehouse'):
        from .spatialite import SpatialiteDatabase

        dbname = config['dbname']
        del config['dbname']
        return SpatialiteDatabase(dbname, **config)

    elif k == ('mysql','warehouse'):   
        raise NotImplemented()  
       
    elif k == ('postgres','warehouse'):   
        raise NotImplemented()    
    

class DatabaseInterface(object):

    @property
    def name(self):  
        raise NotImplementedError() 
   
    def exists(self):
        raise NotImplementedError() 
    
    def create(self):
        raise NotImplementedError() 
    
    def add_post_create(self, f):
        raise NotImplementedError() 
    
    def delete(self):
        raise NotImplementedError() 
    
    def open(self):
        raise NotImplementedError() 
    
    def close(self):
        raise NotImplementedError() 
    
    def inserter(self, table_or_name=None,**kwargs):
        raise NotImplementedError() 

    def updater(self, table_or_name=None,**kwargs):
        raise NotImplementedError() 

    def commit(self):
        raise NotImplementedError() 
  
    def tables(self):
        raise NotImplementedError()  
    
    def has_table(self, table_name):
        raise NotImplementedError()   
    
    def create_table(self, table):
        raise NotImplementedError()  
    
    def drop_table(self, table_name):
        raise NotImplementedError()



def _on_connect_geo(dbapi_con, con_record):
    '''ISSUE some Sqlite pragmas when the connection is created'''
    from ..util import RedirectStdStreams

    dbapi_con.execute('PRAGMA page_size = 8192')
    dbapi_con.execute('PRAGMA temp_store = MEMORY')
    dbapi_con.execute('PRAGMA cache_size = 500000')
    dbapi_con.execute('PRAGMA foreign_keys = ON')
    dbapi_con.execute('PRAGMA journal_mode = OFF')
    #dbapi_con.execute('PRAGMA synchronous = OFF')

    try:
        dbapi_con.execute('select spatialite_version()')
        return
    except:
        try:
            dbapi_con.enable_load_extension(True)
        except AttributeError as e:
            raise

    try:
        with RedirectStdStreams():  # Spatialite prints its version header always, this supresses it.
            dbapi_con.execute("select load_extension('/usr/lib/libspatialite.so')")
    except:
        with RedirectStdStreams():  # Spatialite prints its version header always, this supresses it.
            dbapi_con.execute("select load_extension('/usr/lib/libspatialite.so.3')")

