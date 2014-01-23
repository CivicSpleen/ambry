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
        from .sqlite import SqliteBundleDatabase #@UnresolvedImport
        return SqliteBundleDatabase(bundle=bundle, **config)
    
    elif k == ('mysql',None):   
        raise NotImplemented() 
        
    elif k == ('postgres',None):   
        from .relational import RelationalDatabase #@UnresolvedImport
        return RelationalDatabase(**config)
    
    elif k == ('postgis',None):   
        from .postgis import PostgisDatabase #@UnresolvedImport
        return PostgisDatabase(**config)
    
    
    elif k == ('sqlite','bundle'):
        from .sqlite import SqliteBundleDatabase #@UnresolvedImport
        return SqliteBundleDatabase(bundle=bundle, **config)
    
    elif k == ('sqlite','warehouse'):
        from .sqlite import SqliteWarehouseDatabase #@UnresolvedImport    
        dbname = config['dbname']
        del config['dbname']
        return SqliteWarehouseDatabase(dbname, **config)
    
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
