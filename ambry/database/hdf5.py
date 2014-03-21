"""Base class for Bundle and Partition databases. This module also includes
interfaces for temporary CSV files and HDF files.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import h5py
import os.path
from numpy import * 

class Hdf5File(h5py.File):
    
    def __init__(self, path):

        self._path = path
        self._is_open = False
        super(Hdf5File, self).__init__(self._path)  

    def exists(self):
        import os.path
        
        return os.path.exists(self._path)
        
    def open(self):
        pass
        
    @property
    def path(self):
        return self._path

    def recursive_require_group(self, path):
        
        parts = path.split('/')
        
        ds_name = parts.pop()
        
        parent = self
       
        for part in parts:      
            parent = parent.require_group(part)
        
        return parent, ds_name

    def put_geo(self,path, a, aa):
        '''Store an array along with an Analysis Area'''
        import json

        group, name = self.recursive_require_group(path)
        
        if name in group:
            del group[name]
        
        ds = group.create_dataset(name, data=a, compression=9)
        
        ds.attrs['analysis-area'] = json.dumps(aa.__dict__)
     
        try:
            if a.mask is not ma.nomask:
                ds.attrs['nodata'] = a.fill_value
        except:
            pass
        
        self.flush()
        

    def get_geo(self, path):
        """Return an array an an associated analysis area"""
        import json
        from ambry.geo.analysisarea import AnalysisArea

        group, name = self.recursive_require_group(path)

        
        try:
            ds = group[name]
        except KeyError:
            raise KeyError("Geo group in {} doesn't have dataset named '{}'".format(self.path,name))
        

        aa = AnalysisArea(**(json.loads(ds.attrs['analysis-area'])))
        
        return ds,aa

    def list_geo(self):

        return self.require_group("geo").keys()

    def table(self, table_name, mode='a', expected=None):
        import tables #@UnresolvedImport
        from ambry.orm import Column

        raise NotImplemented()

        try:
            return self.file.root._f_getChild(table_name)
        except tables.NoSuchNodeError:

            tdef = self.bundle.schema.table(table_name)
            descr = {}
            for i, col in enumerate(tdef.columns):
                if col.datatype == Column.DATATYPE_INTEGER64:
                    descr[str(col.name)] = tables.Int64Col(pos=i) #@UndefinedVariable
                    
                elif col.datatype == Column.DATATYPE_INTEGER:
                    descr[str(col.name)] = tables.Int32Col(pos=i) #@UndefinedVariable
                    
                elif col.datatype == Column.DATATYPE_REAL:
                    descr[str(col.name)] = tables.Float32Col(pos=i) #@UndefinedVariable
                    
                elif col.datatype == Column.DATATYPE_TEXT:
                    descr[str(col.name)] = tables.StringCol(pos=i, itemsize= col.width if col.width else 50) #@UndefinedVariable
                else:
                    raise ValueError('Unknown datatype: '+col.datatype)

 
            table = self._file.createTable(self.file.root, table_name, descr, expectedrows=expected)
        
            return table
        
