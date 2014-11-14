"""Access to common geographic datasets

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ambry.dbexceptions import ConfigurationError


class US:
    """ Access to US states, regions, etc. """
    def __init__(self, library):
        self.library = library

    @property 
    def usgeo(self):
        try:
            usgeo = self.library.dep('usgeo')
        except ConfigurationError:
            raise ConfigurationError("MISSING DEPENDENCY: "+"To use the US geo datasets, the bundle ( or library  ) must specify a"+
               " dependency with a set named 'usgeo', in build.dependencies.usgeo")      
        return usgeo
              
   
    def _places(self):
        try:
           places = self.library.dep('places').partition
        except ConfigurationError:
            raise ConfigurationError("MISSING DEPENDENCY: "+"To use the US county datasets, the bundle ( or library  ) must specify a"+
               " dependency with a set named 'places', in build.dependencies.places. "+
               " See https://github.com/clarinova/ambry/wiki/Error-Messages#geoanalysisareasget_analysis_area")
        return places
              
    
    @property
    def states(self):
        return [ UsState(self.library, row) for row in self.bundle.query('SELECT * FROM states')]
       
    def state(self,abbrev=None,**kwargs):
        """Retrieve a state record by abbreviation, fips code, ansi code or census code
        
        The argument to the function is a keyword that can be:
        
            abbrev    Lookup by the state's abbreviation
            fips      Lookup by the state's fips code
            ansi      Lookup by the state's ansi code
            census    Lookup by the state's census code
        
        
        Note that the ansi codes are represented as integers, but they aren't actually numbers; 
        the codes have a leading zero that is only maintained when the codes are used as strings. This
        interface returnes the codes as integers, with the leading zero removed. 
        
        """

        if kwargs.get('abbrev') or abbrev:
            
            if not abbrev:
                abbrev = kwargs.get('abbrev')
            
            rows = self.usgeo.query("SELECT * FROM states WHERE stusab = ?", abbrev.upper() )
        elif kwargs.get('fips'):
            rows = self.usgeo.query("SELECT * FROM states WHERE state = ?", int(kwargs.get('fips')))
        elif kwargs.get('ansi'):
            rows = self.usgeo.query("SELECT * FROM states WHERE statens = ?", int(kwargs.get('ansi')))
        elif kwargs.get('census'):
            rows = self.usgeo.query("SELECT * FROM states WHERE statece = ?", int(kwargs.get('ansi')))
        else:
            rows = None
            

        if rows:
            return UsState(self.library, rows.first())     
        else:
            return None
        
               
    def county(self, code):
        row = self._places().query("""SELECT AsText(geometry) as wkt, SRID(geometry) as srid, * 
        FROM counties WHERE code = ? LIMIT 1""", code).first()    
        
        return UsCounty(self.library, row)
    
    
    def place(self, code):
        
        row = self._places().query("""SELECT AsText(geometry) as wkt, SRID(geometry) as srid, * 
        FROM places WHERE code = ? LIMIT 1""", code).first()    
        
        if not row:
            return None
        
        return Place(self.library, row)
    
    @property
    def places(self):
        
        for row in self._places().query("""SELECT AsText(geometry) as wkt, SRID(geometry) as srid, *  FROM places"""):
            yield Place(self.library, row)
        
    
class UsState:
    """Represents a US State, with acessors for counties, tracks, blocks and other regions
    
    This object is a wrapper on the state table in the geodim dataset, so the fields in the object
    that are acessible through _-getattr__ depend on that table, but are typically: 
    
    geoid     TEXT    
    region    INTEGER    Region
    division  INTEGER    Division
    state     INTEGER    State census code
    stusab    INTEGER    State Abbreviation
    statece   INTEGER    State (FIPS)
    statens   INTEGER    State (ANSI)
    lsadc     TEXT       Legal/Statistical Area Description Code
    name      TEXT    

    Additional acessors include:
    
    fips    FIPS code, equal to the 'state' field
    ansi    ANSI code, euals to the 'statens' field
    census  CENSUS code, equal to the 'statece' field
    usps    Uppercase state abbreviation, equal to the 'stusab' field

    
    """
    
    def __init__(self,library, row):
        self.library = library
        self.row = row
        
    def __getattr__(self, name):
        return self.row[name]
       
    @property
    def fips(self):
        return self.row['state']
    
    @property
    def ansi(self):
        return self.row['statens']
    
    @property
    def census(self):
        return self.row['statece']
    
    @property
    def usps(self):
        return self.row['stusab']
            
    def __str__(self):
        return "<{}:{}>".format('USState',self.row['name'])


class UsCounty(object):
    
    def __init__(self,library, row):
        self.library = library
        self.row = row
    
    @property
    def places(self):
        
        for row in self._places().query("""SELECT AsText(geometry) as wkt, SRID(geometry) as srid, *  FROM places"""):
            yield Place(self.library, row)
    
class Place(object):
    
    def __init__(self,library, row):
        self.library = library
        self.row = row
       
        if not self.row:
            raise Exception('row cannot be None')
        

    @property
    def spsrs(self):
        return self.row['spsrs']


    @property
    def type(self):
        return self.row['type']

    @property
    def name(self):
        return self.row['name']

    @property
    def code(self):
        return self.row['code']

    def aa(self, scale=None):
        """Return an analysis Area"""
        import json  
        from ..old.analysisarea import AnalysisArea
        
        d = json.loads(self.row['aa'])
        
        if scale:
            d['_scale'] = scale
        
        if not d.get('_scale'):
            d['_scale'] = 20

        return AnalysisArea(**d)

    def mask(self, ar=None, nodata=0, scale=10):
        import ogr
        import gdal
        from osgeo.gdalconst import GDT_Byte
        import numpy as np  
        import numpy.ma as ma


        """Return an numpy array with a hard mask to exclude areas outside of the place"""
        srs_in = ogr.osr.SpatialReference()
        srs_in.ImportFromEPSG(self.row['srid'])
        
        aa =  self.aa(scale)
  
        image = aa.get_memimage(data_type=GDT_Byte)

        ogr_ds = ogr.GetDriverByName('Memory').CreateDataSource('/tmp/foo')
        lyr = ogr_ds.CreateLayer('place', aa.srs)

        geometry = ogr.CreateGeometryFromWkt(self.row['wkt'])
        geometry.AssignSpatialReference(srs_in)
        geometry.TransformTo(aa.srs) 
        
        feature = ogr.Feature(lyr.GetLayerDefn())
        feature.SetGeometryDirectly(geometry)

        lyr.CreateFeature(feature)
        
        gdal.RasterizeLayer( image, [1], lyr, burn_values=[1])   
        feature.Destroy()
        
        mask = np.logical_not(np.flipud(np.array(image.GetRasterBand(1).ReadAsArray(), dtype=bool)))
        
        if ar is not None:
            return ma.masked_array(ar, mask=mask, nodata=nodata, hard=True)  
        else:
            return mask

    