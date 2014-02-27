"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
   
from inserter import InserterInterface, UpdaterInterface
from .partition import PartitionDb
from ..partition.geo import GeoPartitionName

class FeatureInserter(InserterInterface):
    
    def __init__(self, partition, table, dest_srs=4326, source_srs=None, layer_name = None):
        from ..geo.sfschema import TableShapefile

        self.partition = partition
        self.bundle = partition.bundle
        self.table = table

        self.sf = TableShapefile(self.bundle, partition.database.path, table, dest_srs, source_srs, name=layer_name)
        
    
    def __enter__(self):
        from ..partitions import Partitions
        self.partition.set_state(Partitions.STATE.BUILDING)
        return self
    
    def __exit__(self, type_, value, traceback):
        from ..partitions import Partitions
        
        if type_ is not None:
            self.bundle.error("Got Exception: "+str(value))
            self.partition.set_state(Partitions.STATE.ERROR)
            return False

        self.partition.set_state(Partitions.STATE.BUILT)
        self.close()
                    
        return self
    
    def insert(self, row, source_srs=None):
        from sqlalchemy.engine.result import RowProxy
        
        if isinstance(row, RowProxy):
            row  = dict(row)

        return self.sf.add_feature( row, source_srs)

    def close(self):
        self.sf.close()
    
        self.partition.database.post_create()
    
        self.partition.convert_dates(self.table)
    
    @property
    def extents(self, where=None):
        '''Return the bounding box for the dataset. The partition must specify 
        a table
        
        '''
        raise NotImplemented()
        #import ..geo.util
        #return ..geo.util.extents(self.database,self.table.name, where=where)
   
    
class GeoDb(PartitionDb):

    EXTENSION = GeoPartitionName.PATH_EXTENSION

    MIN_NUMBER_OF_TABLES = 5 # Used in is_empty
    
    def __init__(self, bundle, partition, base_path, **kwargs):
        ''''''    

        kwargs['driver'] = 'spatialite' 

        super(GeoDb, self).__init__(bundle, partition, base_path, **kwargs)  

    @classmethod
    def make_path(cls, container):
        return container.path + cls.EXTENSION

    def _on_connect(self):
        from  sqlite import _on_connect_update_sqlite_schema
        '''Called from engine() to update the database'''
        _on_connect_geo(self.connection, None)

    def inserter(self,  table = None, dest_srs=4326, source_srs=None, layer_name=None):
        
        if table is None and self.partition.identity.table:
            table = self.partition.identity.table
        
        return FeatureInserter(self.partition,  table, dest_srs, source_srs, layer_name = layer_name)

class SpatialiteWarehouseDatabase(GeoDb):
    pass


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


