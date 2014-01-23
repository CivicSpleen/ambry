"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from . import DatabaseInterface
import semidbm as dbm  # @UnresolvedImport
import os
import json

class Dbm(DatabaseInterface):
    
    def __init__(self, bundle, base_path, suffix=None):

        self.bundle = bundle

        self.suffix = suffix

        self._path = base_path
     
        if suffix:
            self._path += '-'+suffix
            
        self._path += '.dbm'
            
        self._file = None
 
       
    @property
    def reader(self):
        self.close()
        self._file = dbm.open(self._path, 'r')
        return self
   
    @property
    def writer(self):
        """Return a new writer. Will always create a new file"""
        self.close()
        self._file = dbm.open(self._path, 'n')

        return self
  
    @property
    def appender(self):
        """Return a new writer, preserving the file if it already exists"""
        self.close()
        self._file = dbm.open(self._path, 'c')
        return self
        
    def delete(self):
        
        if os.path.exists(self._path):
            os.remove(self._path)
        
        
    def close(self):
        if self._file:
            self._file.close()
            self._file = None

    
    def __getitem__(self, key):
        return json.loads(self._file[key])
        

    def __setitem__(self, key, val):
        #print key,'<-',val

        self._file[str(key)] =  json.dumps(val)
    
    def keys(self):
        
        return self._file.keys()

      