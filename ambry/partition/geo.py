"""Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import sys
from ..identity import PartitionIdentity, PartitionName
from sqlite import SqlitePartition


def _geo_db_class(): # To break an import dependency
    from ..database.geo import GeoDb
    return GeoDb

class GeoPartitionName(PartitionName):
    PATH_EXTENSION = '.geodb'
    FORMAT = 'geo'

class GeoPartitionIdentity(PartitionIdentity):
    _name_class = GeoPartitionName

class GeoPartition(SqlitePartition):
    '''A Partition that hosts a Spatialite for geographic data'''

    _id_class = GeoPartitionIdentity
    _db_class = _geo_db_class()
    
    def __init__(self, bundle, record, **kwargs):
        super(GeoPartition, self).__init__(bundle, record)

    @property
    def database(self):
        if self._database is None:
            self._database = self._db_class(self.bundle, self, base_path=self.path)
        return self._database

    def _get_srs_wkt(self):
        
        #
        # !! Assumes only one layer!
        
        try:
            q ="select srs_wkt, spatial_ref_sys.srid from geometry_columns, spatial_ref_sys where spatial_ref_sys.srid == geometry_columns.srid;"
            return self.database.query(q).first()
        except:
            q ="select srtext, spatial_ref_sys.srid from geometry_columns, spatial_ref_sys where spatial_ref_sys.srid == geometry_columns.srid;"
            return self.database.query(q).first()

    def get_srs_wkt(self):
        r = self._get_srs_wkt()
        return r[0]

    def get_srid(self):
        r = self._get_srs_wkt()
        return r[1]

    def get_srs(self):
        import ogr 
        
        srs = ogr.osr.SpatialReference()
        srs.ImportFromWkt(self.get_srs_wkt())
        return srs

    @property
    def srs(self):
        return self.get_srs()

    def get_transform(self, dest_srs=4326):
        """Get an ogr transform object to convert from the SRS of this partition 
        to another"""
        import ogr, osr
        
      
        srs2 = ogr.osr.SpatialReference()
        srs2.ImportFromEPSG(dest_srs) 
        transform = osr.CoordinateTransformation(self.get_srs(), srs2)

        return transform

    def create(self, dest_srs=4326, source_srs=None):

        from ambry.geo.sfschema import TableShapefile

        if self.identity.table:

            tsf = TableShapefile(self.bundle, self._db_class.make_path(self), self.identity.table,
                                 dest_srs = dest_srs, source_srs = source_srs )

            tsf.close()

            self.add_tables(self.data.get('tables',None))

    def convert(self, table_name, progress_f=None):
        """Convert a spatialite geopartition to a regular arg
        by extracting the geometry and re-projecting it to WGS84
        
        :param config: a `RunConfig` object
        :rtype: a `LibraryDb` object
        
        :param config: a `RunConfig` object
        :rtype: a `LibraryDb` object
                
        """
        import subprocess
        import csv 
        from ambry.orm import Column
        from ambry.dbexceptions import ConfigurationError

        #
        # Duplicate the geo arg table for the new arg
        # Then make the new arg
        #

        t = self.bundle.schema.add_table(table_name)
        
        ot = self.table
        
        for c in ot.columns:
            self.bundle.schema.add_column(t,c.name,datatype=c.datatype)
                
        
        #
        # Open a connection to spatialite and run the query to 
        # extract CSV. 
        #
        # It would be a lot more efficient to connect to the 
        # Spatialite procss, attach the new database, the copt the 
        # records in SQL. 
        #
        
        try:
            subprocess.check_output('spatialite -version', shell=True)
        except:
            raise ConfigurationError('Did not find spatialite on path. Install spatialite')
        
        # Check the type of geometry:
        p = subprocess.Popen(('spatialite {file} "select GeometryType(geometry) FROM {table} LIMIT 1;"'
                              .format(file=self.database.path,table = self.identity.table)), 
                             stdout = subprocess.PIPE, shell=True)
        
        out, _ = p.communicate()
        out = out.strip()
        
        if out == 'POINT':
            self.bundle.schema.add_column(t,'_db_lon',datatype=Column.DATATYPE_REAL)
            self.bundle.schema.add_column(t,'_db_lat',datatype=Column.DATATYPE_REAL)
            
            command_template = """spatialite -csv -header {file} "select *,   
            X(Transform(geometry, 4326)) AS _db_lon, Y(Transform(geometry, 4326)) AS _db_lat 
            FROM {table}" """  
        else:
            self.bundle.schema.add_column(t,'_wkb',datatype=Column.DATATYPE_TEXT)
            
            command_template = """spatialite -csv -header {file} "select *,   
            AsBinary(Transform(geometry, 4326)) AS _wkb
            FROM {table}" """              

        self.bundle.database.commit()

        pid = self.identity
        pid.table = table_name
        arg = self.bundle.partitions.new_partition(pid)
        arg.create_with_tables()

        #
        # Now extract the data into a new database. 
        #

        command = command_template.format(file=self.database.path,
                                          table = self.identity.table)

        
        self.bundle.log("Running: {}".format(command))
        
        p = subprocess.Popen(command, stdout = subprocess.PIPE, shell=True)
        stdout, stderr = p.communicate()
        
        #
        # Finally we can copy the data. 
        #
 
        rdr = csv.reader(stdout.decode('ascii').splitlines())# local csv module shadows root #@UndefinedVariable
        header = rdr.next()
       
        if not progress_f:
            progress_f = lambda x: x
       
        with arg.database.inserter(table_name) as ins:
            for i, line in enumerate(rdr):
                ins.insert(line)
                progress_f(i)


    def convert_dates(self, table_name):
        '''Remove the 'T' at the end of dates that OGR adds erroneously'''
        from ambry.orm import Column
    
        table = self.bundle.schema.table(table_name)
    
        clauses = []
        
        for column in table.columns:
            if column.datatype == Column.DATATYPE_DATE:
                clauses.append("{col} = REPLACE({col},'T','')".format(col=column.name))
        
        if clauses:
            print 'convert_dates HERE', self.database.dsn
            self.database.connection.execute( "UPDATE {} SET {}".format(table.name, ','.join(clauses)))

    def load_shapefile(self, path, t_srs = '4326',  **kwargs):
        """Load a shape file into a partition as a spatialite database. 
        
        Will also create a schema entry for the table speficified in the 
        table parameter of the  pid, using the fields from the table in the
        shapefile
        """
        import subprocess
        from ambry.dbexceptions import ConfigurationError
        from ambry.geo.util import get_shapefile_geometry_types
        import os

        if t_srs:
            t_srs_opt = '-t_srs EPSG:{}'.format(t_srs)
        else:
            t_srs_opt = ''
        
        if path.startswith('http'):
            shape_url = path
            path = self.bundle.filesystem.download_shapefile(shape_url)
        
        try:
            subprocess.check_output('ogr2ogr --help-general', shell=True)
        except:
            raise ConfigurationError('Did not find ogr2ogr on path. Install gdal/ogr')
        
        self.bundle.log("Checking types in file {}".format(path))
        types, type = get_shapefile_geometry_types(path)
        
        #ogr_create="ogr2ogr -explodecollections -skipfailures -f SQLite {output} -nlt  {type} -nln \"{table}\" {input}  -dsco SPATIALITE=yes"
        
        ogr_create="ogr2ogr  -overwrite -progress -skipfailures -f SQLite {output} -gt 65536 {t_srs} -nlt  {type} -nln \"{table}\" {input}  -dsco SPATIALITE=yes"

        dir_ = os.path.dirname(self.database.path)

        if not os.path.exists(dir_):
            self.bundle.log("Make dir: "+dir_)
            os.makedirs(dir_)

        if os.path.exists(self.database.path):
            os.remove(self.database.path)

        cmd = ogr_create.format(input = path,
                                output = self.database.path,
                                table = self.table.name,
                                type = type,
                                t_srs = t_srs_opt
                                 )
        
        self.bundle.log("Running: "+ cmd)
    
        output = subprocess.check_output(cmd, shell=True)

        #with self.bundle.session:
        #    for row in self.database.connection.execute("pragma table_info('{}')".format(self.table.name)):
        #        parts = row[2].lower().strip(')').split('(')
        #        datatype = parts[0]
        #        size = int(parts[1]) if len(parts) > 1 else None
        #        self.bundle.schema.add_column(self.table,row[1],datatype = datatype, size=size,
        #                                       is_primary_key=True if row[1].lower()=='ogc_fid' else False, commit = False)
                
        self.database.post_create()


    def __repr__(self):
        return "<geo partition: {}>".format(self.name)


    @property
    def info(self):
        """Returns a human readable string of useful information"""

        try:
            srid = self.get_srid()
        except Exception as e:
            self.bundle.error(e)
            srid = 'error'

        return super(GeoPartition, self).info+ '{:10s}: {}\n'.format('SRID',srid)

