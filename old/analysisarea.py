""" Definition of a geographic area for which a Raster or Aray will be created. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import ogr #@UnresolvedImport
from numpy import *
from osgeo.gdalconst import * #@UnresolvedImport

from ambry.geo import Point
from util import create_poly



#ogr.UseExceptions()

class _RasterLayer(object):
    
    def __init__(self, aa, file_=None, data_type=GDT_Byte, nodata=0):
        import uuid, tempfile, os
        
        self.data_type = data_type
        self.aa = aa

        if file_:
            self.image = self.aa.get_geotiff(file_,data_type=self.data_type, nodata=nodata )
        else:
            self.image = self.aa.get_memimage(data_type=self.data_type)

        # File should not actually get written. 
        self.ogr_ds = ogr.GetDriverByName('Memory').CreateDataSource(os.path.join(tempfile.gettempdir(), str(uuid.uuid4())))

        field_map = {
                    GDT_Byte : ogr.OFTInteger, 
                    GDT_UInt16 : ogr.OFTInteger, 
                    GDT_Int16 : ogr.OFTInteger, 
                    GDT_UInt32 : ogr.OFTInteger, 
                    GDT_Int32 : ogr.OFTInteger, 
                    GDT_Float32 : ogr.OFTReal,
                    GDT_Float64 : ogr.OFTReal, 
                    GDT_CInt16 : ogr.OFTInteger, 
                    GDT_CInt32 : ogr.OFTInteger, 
                    GDT_CFloat32 : ogr.OFTReal, 
                    GDT_CFloat64 : ogr.OFTReal,
                     }

        self.lyr = self.ogr_ds.CreateLayer('rastered', self.aa.srs)
        self.lyr.CreateField(ogr.FieldDefn( "value", field_map[self.data_type] )) # 0

    
    def add_geometry(self, geometry, value):

        geometry.AssignSpatialReference(self.aa.srs)
        
        feature = ogr.Feature(self.lyr.GetLayerDefn())
        feature.SetField(0, value )
        feature.SetGeometryDirectly(geometry)

        self.lyr.CreateFeature(feature)
        feature.Destroy()
            
    def add_wkt(self, wkt, value):
        
        geometry = ogr.CreateGeometryFromWkt(wkt)
    
        self.add_geometry(geometry, value)
    
    def rasterize(self):
        '''Rasterize the layer, to a file if a file was set, and return a numpy array for the
        rasterized data'''
        
        import gdal
        import numpy as np
        
        gdal.RasterizeLayer( self.image, [1], self.lyr, options = ["ATTRIBUTE=value"])   

        self.ogr_ds.SyncToDisk()
        self.ogr_ds.Release()

        field_map = {
                    GDT_Byte : int, 
                    GDT_UInt16 : int, 
                    GDT_Int16 : int, 
                    GDT_UInt32 : int, 
                    GDT_Int32 : int, 
                    GDT_Float32 : float,
                    GDT_Float64 : float, 
                    GDT_CInt16 : float, 
                    GDT_CInt32 : float, 
                    GDT_CFloat32 : float, 
                    GDT_CFloat64 : float,
                     }

        a = np.flipud(np.array(self.image.GetRasterBand(1).ReadAsArray(), dtype=field_map[self.data_type]))

        return a


def get_analysis_area(library, **kwargs):
    """Return an analysis area by name or GEOID
    
    Requires a build dependency for 'extents' such as build.dependencies.extents
    
    Keyword Arguments:
    
        geoid: The geoid of the analysis area
        ependency_name: The name of the dependency for the extents dataset. Defaults to 'places'
     
    :rtype: An `AnalysisArea` object.
    
    """
    from old.datasets.geo import US

    state_code = kwargs.get('state', False)
    county_code = kwargs.get('county', False)
    place_code = kwargs.get('place', False)

    for kw in ('state','county','place'):
        if kwargs.get(kw, False):
            del kwargs[kw]

    if place_code:
        code = place_code
        place = US(library).place(place_code)
    else:
        raise NotImplemented()
    
    if not place:
        raise Exception("Failed to get analysis area record for geoid: {}".format(code))

    return place.aa(**kwargs)


def draw_edges(a):
        for i in range(0,a.shape[0]): # Iterate over Y
            a[i,0] = 1
            a[i,1] = 2
            a[i,2] = 3
        
            a[i,a.shape[1]-2] = 2
            a[i,a.shape[1]-1] = 1
                                    
        for i in range(0,a.shape[1]): # Iterate over x
            a[0,i] = 1
            a[1,i] = 2
            a[2,i] = 3
            
            a[a.shape[0]-2,i] = 2
            a[a.shape[0]-1,i] = 1
                  


class AnalysisArea(object):
    
    SCALE = 10 # Default cell size
    MAJOR_GRID = 100 # All boundary dimensions must be even modulo this. 
    
    MAX_CELLS = 75 * 1000 * 1000
    
    DEFAULT_D_TYPE = GDT_Float32
    
    def __init__(self, name, geoid,
                 eastmin, eastmax, northmin, northmax, 
                 lonmin, lonmax, latmin, latmax, 
                 srid, srswkt, scale=SCALE, **kwargs):
        """ 
        
        Args:
        
            _scale: The size of a side of a cell in array, in meters. 
        
        """
        self.name = name
        self.geoid = geoid
        self.eastmin = eastmin
        self.eastmax = eastmax
        self.northmin = northmin
        self.northmax = northmax
 
        self.mideast = int( (self.eastmax - self.eastmin) / 2) +  self.eastmin
        self.midnorth = int( (self.northmax - self.northmin) / 2) +  self.northmin
 
        self.lonmin = lonmin
        self.lonmax = lonmax
        self.latmin = latmin
        self.latmax = latmax

        
        self.srid = srid
        self.srswkt = srswkt

        if kwargs.get("_scale"): # appears when called from from_json()
            
            self.scale = kwargs.get("_scale") 
        else:
            self.scale = scale # UTM meters per grid area

        #Dimensions ust be even by MAJOR_GRID
     
        if  (self.eastmin % self.MAJOR_GRID + self.eastmax % self.MAJOR_GRID +
             self.northmin % self.MAJOR_GRID + self.northmax % self.MAJOR_GRID ) > 0:
            raise Exception("Bounding box dimensions must be even modulo {}"
                            .format(self.MAJOR_GRID))
                                 

    def _too_big(self):
        if self.size_x * self.size_y > self.MAX_CELLS:
            raise Exception("Too big for scale {}: {} * {} = {}M cells"
                            .format(self._scale,self.size_x, self.size_y, self.size_x * self.size_y / 1000000))


    def new_array(self, dtype=float, mask=None):

        self._too_big()

        return zeros((self.size_y, self.size_x), dtype = dtype)
            

    @property
    def scale(self):
        return self._scale
    
    @scale.setter
    def scale(self, scale):

        self._scale = scale
        
        if  self.MAJOR_GRID % self._scale != 0:
            raise Exception("The _scale {} must divide evenly into the MAJOR_GRID {}"
                            .format(self._scale, self.MAJOR_GRID))                                                
    
        self.size_x = (self.eastmax - self.eastmin) / self._scale
        self.size_y = (self.northmax - self.northmin) / self._scale        

        self._too_big()
        
    def new_masked_array(self, dtype=float, nodata=0, mask=None):
        
        return ma.masked_array(self.new_array(dtype=dtype),nodata=nodata, mask=mask)  
        
    @property 
    def state(self):
        '''Extract the state from the geoid.'''
        import re 
        r = re.match('CG(\d\d).*', self.geoid)
        if r:
            return int(r.group(1))
        else:
            raise NotImplementedError("state can only handle AAs with census geoids. ")

    @property
    def lower_left(self):
        return (self.eastmin, self.northmin)
    
    @property
    def upper_left(self):
        return (self.eastmin, self.northmax)
    
    @property
    def pixel_size(self):
        return self._scale

    def translate_to_array(self, x, y):
        """Translate state plane coordinates to array_coordinates"""
        
        return Point(
                         int((x-self.eastmin)/self._scale),
                         int((y-self.northmin)/self._scale)
                    )
        

    @property
    def srs(self):

        return self._get_srs(self.srid)
      

    def _get_srs(self, srs_spec=None, default=4326):
        
        srs = ogr.osr.SpatialReference()
        
        if srs_spec is None and default is not None:
            return self._get_srs(default, None)
            srs.ImportFromEPSG(default) # Lat/Long in WGS84
        elif isinstance(srs_spec,int):
            srs.ImportFromEPSG(srs_spec)
        elif  isinstance(srs_spec,basestring):
            srs.ImportFromWkt(srs_spec)
        elif isinstance(srs_spec, ogr.osr.SpatialReference ):
            return srs_spec
        else:
            raise ValueError("Bad srs somewhere. Source={}, Default = {}"
                             .format(srs_spec, default))
        
        return srs
        
    def get_coord_transform(self,  source_srs=None):
        """Get the OGR object for converting coordinates

        """
        s_srs = self._get_srs(source_srs)
        d_srs = self.srs
    
        return ogr.osr.CoordinateTransformation(s_srs, d_srs)
                
    def get_translator(self, source_srs=None):
        """Get a function that transforms coordinates from a source srs
        to the coordinates of this array """

        trans = self.get_coord_transform(source_srs)

        def _transformer(x,y):
            xp,yp,z =  trans.TransformPoint(x,y)
            return Point(
                         int((xp-self.eastmin)/self._scale),
                         int((yp-self.northmin)/self._scale)
                    )
        
        return _transformer
  
        
        
    def is_in_ll(self, lon, lat):
        """Return true if the (lat, lon) is inside the area"""
        return (lat < self.latmax and
                lat > self.latmin and
                lon < self.lonmax and
                lon > self.lonmin )     
        
        
    def is_in_ll_query(self, lat_name='lat',lon_name='lon'):
        """Return SQL text for querying for a lat/long point that is in this analysis area"""
        
        return (""" {lat_name} >= {latmin} AND {lat_name} <= {latmax} AND
        {lon_name} >= {lonmin} AND {lon_name} <= {lonmax}"""
        .format( lat_name=lat_name, lon_name=lon_name,**self.__dict__))
        
    def is_in_utm(self, x, y):
        """Return true if the (lat, lon) is inside the area"""
        return (y < self.northmax and
                y > self.northmin and
                x < self.eastmax and
                x > self.eastmin )     
        

    @property
    def place_bb_poly(self):
        """Polygon for the bounding box of the place"""
        geo =  create_poly(((self.lonmin,self.latmin),
                                  (self.lonmin,self.latmax),
                                  (self.lonmax,self.latmax),
                                  (self.lonmax,self.latmin)
                                ),self._get_srs(None)
                                 )
        
        return geo
    
    @property
    def area_bb_poly(self):
        """Polygon for the bounding box of the analysis area"""
        geo =  create_poly(((self.eastmin,self.northmin),
                          (self.eastmin,self.northmax),
                          (self.eastmax,self.northmax),
                          (self.eastmax,self.northmin)
                        ), self.srs)
    
    
        return geo
    
    def write_poly(self, file_, layer='poly', poly=None):
        """Write both bounding boxes into a KML file
        
        Write the bounding box area: 
        >>>> aa.write_poly('/tmp/place',layer='place', poly=aa.place_bb_poly)
        >>>> aa.write_poly('/tmp/area',layer='area', poly=aa.area_bb_poly)
        
        """
        import ogr #@UnresolvedImport

        if not file_.endswith('.kml'):
            file_ = file_+'.shp'
   
        #driver = ogr.GetDriverByName('ESRI Shapefile')
        driver = ogr.GetDriverByName('KML')
        
        if poly is None:
            poly = self.area_bb_poly
        
        datasource = driver.CreateDataSource(file_)
        layer = datasource.CreateLayer(layer,
                                       srs = poly.GetSpatialReference(),
                                       geom_type=ogr.wkbPolygon)

     
        #create feature object with point geometry type from layer object:
        feature = ogr.Feature( layer.GetLayerDefn() )
        feature.SetGeometry(poly)      
        layer.CreateFeature(feature)
   
        poly = self.place_bb_poly
     
        #create feature object with point geometry type from layer object:
        feature = ogr.Feature( layer.GetLayerDefn() )
        feature.SetGeometry(poly)      
        layer.CreateFeature(feature)

        #flush memory
        feature.Destroy()
        datasource.Destroy()

    def get_geotiff(self, file_,  bands=1, over_sample=1, data_type=DEFAULT_D_TYPE, nodata=0):
        return self._get_image(file_, bands, over_sample, data_type, nodata, 'GTiff')

    def get_memimage(self,  bands=1, over_sample=1, data_type=DEFAULT_D_TYPE, nodata=0):
        return self._get_image('/tmp/foo.mem', bands, over_sample, data_type, nodata, 'MEM')

    def _get_image(self, file_,  bands=1, over_sample=1, data_type=DEFAULT_D_TYPE, nodata=0, driver='GTiff'):
        from osgeo import gdal
    
        if driver in ('GTiff'):
            options = [ 'COMPRESS=LZW' ]
        else:
            options =  []
    
        driver = gdal.GetDriverByName(driver) 
 
        x = int(self.size_x*over_sample)
        y = int(self.size_y*over_sample)

        out = driver.Create(file_, 
                            x,y,
                            bands, 
                            data_type, 
                            options = options)  
        
        # Note that Y pixel height is negative to account for increasing
        # Y going down the image, rather than up. 
        transform = [self.eastmin, # self.lower_left[0] ,  # Upper Left X postion
                     self.pixel_size/over_sample ,  # Pixel Width 
                     0 ,     # rotation, 0 if image is "north up" 
                     self.northmax, #self.lower_left[1] ,  # Upper Left Y Position
                     0 ,     # rotation, 0 if image is "north up"
                     -self.pixel_size/over_sample # Pixel Height
                     ]

        out.SetGeoTransform(transform)  
        
        for i in range(bands):
            out.GetRasterBand(i+1).SetNoDataValue(nodata)
            
        out.SetProjection( self.srs.ExportToWkt() )
        
        return out

    

    
    def get_rasterlayer(self, file_=None, data_type=GDT_Byte):
        """Return a GDAL layer that can be rasterized. """

        return  _RasterLayer(self, file_, data_type=data_type)
    
    
    def write_geotiff(self, file_,  a, data_type=DEFAULT_D_TYPE, nodata=0):
        """
        Args:
            file_: Name of file to write to
            aa: Analysis Area object
            a: numpy array
        """
        

        out = self.get_geotiff( file_,  data_type=data_type)
     
        out.GetRasterBand(1).SetNoDataValue(nodata)
        out.GetRasterBand(1).WriteArray(flipud(a))
      
        return file_

    @staticmethod
    def rd(v):
        """Round down, to the nearest even 100"""
        import math
        return math.floor(v/100.0) * 100
    
    @staticmethod
    def ru(v):
        """Round up, to the nearest even 100"""
        import math
        return math.ceil(v/100.0) * 100
        
    @classmethod
    def new_from_envelope(self, envelope_srs,  envelope, name=None, geoid=None, **kwargs):
        """Create a new AA given the envelope from a GDAL geometry."""
        import util
        d_srs =  ogr.osr.SpatialReference()
        d_srs.ImportFromEPSG(4326) # Lat/Long in WGS84

        env1_bb = util.create_bb(envelope, envelope_srs)
        env1_bb.TransformTo(d_srs)       
        env2_bb = util.create_bb(env1_bb.GetEnvelope(), env1_bb.GetSpatialReference()).GetEnvelope()

        d = {
             'lonmin': env2_bb[0],
             'lonmax': env2_bb[1],
             'latmin': env2_bb[2],
             'latmax': env2_bb[3],
            
             'eastmin': self.rd(envelope[0]),
             'eastmax': self.ru(envelope[1]),
             'northmin': self.rd(envelope[2]),
             'northmax': self.ru(envelope[3])
             }      

        d = dict(d.items() + kwargs.items())

        return AnalysisArea( 
                  name,
                  geoid , # 'name' is used twice, pick the first. 
                  srid=int(envelope_srs.GetAuthorityCode('GEOGCS')),                       
                  srswkt=envelope_srs.ExportToWkt(),
                  **d)


    @classmethod
    def new_from_geometry(cls,g, name=None, geoid=None):
        
        e = g.GetEnvelope()
        srs = g.GetSpatialReference()  

        return cls.new_from_envelope(srs, e, name, geoid)


    @property
    def ll_envelope(self):
        """Lat/Lon envelope"""
        return (self.lonmin, self.lonmax, self.latmin, self.latmax)
    
    @property
    def ne_envelope(self):
        """Norhting/Easting envelope"""
        return (self.eastmin, self.eastmax, self.northmin, self.northmax)
    
    def to_json(self):
        import json
        return json.dumps(self.__dict__)
    
    @classmethod
    def from_json(cls, jstr):
        import json
        
        d = json.loads(jstr)
      
        return AnalysisArea( **d)
   
    
    def __str__(self):
        return ("AnalysisArea    : {name} \n"+
                "WGS84  Extents  : ({lonmin},{latmin}) ({lonmax},{latmax})\n"+
                "SPZone Extents  : ({eastmin},{northmin}) ({eastmax},{northmax})\n"+
                "Size            : ({size_x}, {size_y})\n" + 
                "Scale           : {_scale}\n" + 
                "EPGS SRID:      : {srid}\n"+
                "Pro4txt: {proj4txt}"
        ).format(proj4txt=self.srs.ExportToProj4(),**self.__dict__)
        
    