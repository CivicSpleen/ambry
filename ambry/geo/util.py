'''
Created on Feb 15, 2013

@author: eric
'''
from collections import namedtuple
import random
from osgeo import gdal, ogr    


BoundingBox = namedtuple('BoundingBox', ['min_x', 'min_y','max_x', 'max_y'])

def extents(database, table_name, where=None, lat_col='_db_lat', lon_col='_db_lon'):

    '''Return the bounding box for a table in the database. The partition must specify 
    a table
    
    '''
    # Find the extents of the data and figure out the offsets for the array. 
    e= database.connection.execute
    
    if where:
        where = "WHERE "+where
    else:
        where = ''
    
    r = e("""SELECT min({lon}) as min_x, min({lat}) as min_y, 
            max({lon}) as max_x, max({lat}) as max_y from {table} {where}"""
            .format(lat=lat_col, lon=lon_col, table=table_name, where=where)
        ).first()
          
    # Convert to a regular tuple 
    o = BoundingBox(r[0], r[1],r[2],r[3])
    
    return o

#From http://danieljlewis.org/files/2010/06/Jenks.pdf
#
#  Use pysal instead!
#  http://pysal.geodacenter.org/1.2/library/esda/mapclassify.html#pysal.esda.mapclassify.Natural_Breaks
#
# Or, a cleaner Python implementation: https://gist.github.com/drewda/1299198
def jenks_breaks(dataList, numClass): 
 
    print "A"
    mat1 = [] 
    for i in range(0, len(dataList) + 1): 
        temp = [] 
        for j in range(0, numClass + 1): 
            temp.append(0) 
        mat1.append(temp) 

    print "B"
    mat2 = [] 
    for i in range(0, len(dataList) + 1): 
        temp = [] 
        for j in range(0, numClass + 1): 
            temp.append(0) 
        mat2.append(temp) 
  
    print "C"
    for i in range(1, numClass + 1): 
        mat1[1][i] = 1 
        mat2[1][i] = 0 
        for j in range(2, len(dataList) + 1): 
            mat2[j][i] = float('inf') 

    print "D"
    v = 0.0 
    # # iterations = datalist * .5*datalist * Numclass
    for l in range(2, len(dataList) + 1): 
        s1 = 0.0 
        s2 = 0.0 
        w = 0.0 
        for m in range(1, l + 1): 
            i3 = l - m + 1 
    
            val = float(dataList[i3 - 1]) 
    
            s2 += val * val 
            s1 += val 
    
            w += 1 
            v = s2 - (s1 * s1) / w 
            i4 = i3 - 1 
    
            if i4 != 0: 
                for j in range(2, numClass + 1): 
                    if mat2[l][j] >= (v + mat2[i4][j - 1]): 
                        mat1[l][j] = i3 
                        mat2[l][j] = v + mat2[i4][j - 1] 
        mat1[l][1] = 1 
        mat2[l][1] = v 
    k = len(dataList) 
    kclass = [] 
    print "E"
    for i in range(0, numClass + 1): 
        kclass.append(0) 
    
    kclass[numClass] = float(dataList[len(dataList) - 1]) 
    
    countNum = numClass 
    
    print 'F'
    while countNum >= 2: 
        #print "rank = " + str(mat1[k][countNum]) 
        id_ = int((mat1[k][countNum]) - 2) 
        #print "val = " + str(dataList[id]) 
    
        kclass[countNum - 1] = dataList[id_] 
        k = int((mat1[k][countNum] - 1)) 
        countNum -= 1 
    
    return kclass 
 
def getGVF( dataList, numClass ): 
    """ The Goodness of Variance Fit (GVF) is found by taking the 
    difference between the squared deviations from the array mean (SDAM) 
    and the squared deviations from the class means (SDCM), and dividing by the SDAM 
    """ 
    breaks = jenks_breaks(dataList, numClass) 
    dataList.sort() 
    listMean = sum(dataList)/len(dataList) 
    print listMean 
    SDAM = 0.0 
    for i in range(0,len(dataList)): 
            sqDev = (dataList[i] - listMean)**2 
            SDAM += sqDev 
             
    SDCM = 0.0 
    for i in range(0,numClass): 
            if breaks[i] == 0: 
                    classStart = 0 
            else: 
                    classStart = dataList.index(breaks[i]) 
                    classStart += 1 
            classEnd = dataList.index(breaks[i+1]) 
    
            classList = dataList[classStart:classEnd+1] 
    
            classMean = sum(classList)/len(classList) 
            print classMean 
            preSDCM = 0.0 
            for j in range(0,len(classList)): 
                    sqDev2 = (classList[j] - classMean)**2 
                    preSDCM += sqDev2 
    
            SDCM += preSDCM 
    
    return (SDAM - SDCM)/SDAM 


def rasterize(pixel_size=25):
    # Open the data source
    
    RASTERIZE_COLOR_FIELD = "__color__"
    
    orig_data_source = ogr.Open("test.shp")
    # Make a copy of the layer's data source because we'll need to 
    # modify its attributes table
    source_ds = ogr.GetDriverByName("Memory").CopyDataSource(orig_data_source, "")
    source_layer = source_ds.GetLayer(0)
    source_srs = source_layer.GetSpatialRef()
    x_min, x_max, y_min, y_max = source_layer.GetExtent()
    
    # Create a field in the source layer to hold the features colors
    field_def = ogr.FieldDefn(RASTERIZE_COLOR_FIELD, ogr.OFTReal)
    source_layer.CreateField(field_def)
    source_layer_def = source_layer.GetLayerDefn()
    field_index = source_layer_def.GetFieldIndex(RASTERIZE_COLOR_FIELD)
    
    # Generate random values for the color field (it's here that the value
    # of the attribute should be used, but you get the idea)
    
    for feature in source_layer:
        feature.SetField(field_index, random.randint(0, 255))
        source_layer.SetFeature(feature)
        
    # Create the destination data source
    x_res = int((x_max - x_min) / pixel_size)
    y_res = int((y_max - y_min) / pixel_size)
    target_ds = gdal.GetDriverByName('GTiff').Create('test.tif', x_res,
            y_res, 3, gdal.GDT_Byte)
    
    target_ds.SetGeoTransform(( x_min, pixel_size, 0, y_max, 0, -pixel_size,))
    
    if source_srs:
        # Make the target raster have the same projection as the source
        target_ds.SetProjection(source_srs.ExportToWkt())
    else:
        # Source has no projection (needs GDAL >= 1.7.0 to work)
        target_ds.SetProjection('LOCAL_CS["arbitrary"]')
        
    # Rasterize
    err = gdal.RasterizeLayer(target_ds, (3, 2, 1), source_layer,
            burn_values=(0, 0, 0),
            options=["ATTRIBUTE=%s" % RASTERIZE_COLOR_FIELD])
    
    if err != 0:
        raise Exception("error rasterizing layer: %s" % err)


def create_poly( points, srs):
    """Create a polygon from a list of points"""

    #create polygon object:
    ring = ogr.Geometry(type=ogr.wkbLinearRing)
    for x,y in points:
        ring.AddPoint(x, y)#LowerLeft
        
    # Close
    ring.AddPoint(points[0][0], points[0][1])

    poly = ogr.Geometry(type=ogr.wkbPolygon)
    poly.AssignSpatialReference(srs)
    poly.AddGeometry(ring)

    return poly

def create_bb( corners, srs):
    """Create a boundingbox from a list or tuple of the four corners
    Corners has four values:  x_min, x_max, y_min, y_max
    
    The input can be taken directory from Feature.GetEnvelope()
    
    """
    
    c = corners
    
    return create_poly(((c[0], c[2]),
                            (c[0], c[3]),
                            (c[1], c[3]),
                            (c[1], c[2]),
                              ), srs)

      
def combine_envelopes( geos, use_bb=True, use_distance=False):
    """Find geometries that intersect"""
    loops = 0   
    while True: 
        i, new_geos = _combine_envelopes(geos, use_bb, use_distance)
        old = len(geos)
        geos = None
        geos = [g.Clone() for g in new_geos]
        loops += 1
        print "{}) {} reductions. {} old, {} new".format(loops, i, old, len(geos))
        if old == len(geos):
            break
      
    return geos
      
def _combine_envelopes(geometries, use_bb = True, use_distance=False):
    """Inner support function for combine_envelopes"""
    import ambry.geo as dg
    reductions = 0
    new_geometries = []
    
    accum = None
    reduced = set()

    for i1 in range(len(geometries)):
        if i1 in reduced:
            continue
        g1 = geometries[i1]
        for i2 in range(i1+1, len(geometries)):
            if i2 in reduced:
                continue

            g2 = geometries[i2]


            intersects = False
            
            if (g1.Intersects(g2) or  g1.Contains(g2) or g2.Contains(g1) or g1.Touches(g2)):
                intersects = True
      
            # If the final output is to onvert the reduced geometries to bounding boxes, it
            # can have BBs that intersect that were not reduced, because the underlying geometries
            # didn't intersect
            if use_bb and not intersects:
                bb1 =  dg.create_bb(g1.GetEnvelope(), g1.GetSpatialReference())
                bb2 =  dg.create_bb(g2.GetEnvelope(), g2.GetSpatialReference())
                if bb1.Intersects(bb2):  
                    intersects = True
                    
            if use_distance and not intersects:
                if use_bb:
                    if bb1.Distance(bb2) < use_distance:
                        intersects = True                        
                else:
                    if g1.Distance(g2) < use_distance:
                        intersects = True         

            if intersects:
                reductions += 1
                reduced.add(i2)
                if not accum:
                    accum = g1.Union(g2)
                else:
                    accum = accum.Union(g2)
        
        if accum is not None:
            new_geometries.append(accum.Clone())
            accum = None
        else:
            new_geometries.append(g1.Clone())

    return reductions, new_geometries
            
def bound_clusters_in_raster( a, aa, shape_file_dir, 
                                 contour_interval,contour_value, use_bb=True, use_distance=False):
        """Create a shapefile that contains contours and bounding boxes for clusters
        of contours.
        
        :param a: A numpy array that contains the data inwhich to find clusters
        :type a: Numpy array
        
        :param aa: The analysis object that sets the coordinate system for the area that contains the array
        :type aa: ambry.geo.AnalysisArea
        
        :param shape_file_dir: The path to a directory where generated files will be stored. 
        :type shape_file_dir: string
        
        :param contour_interval: The difference between successive contour intervals. 
        :type contour_interval: float

        :param contour_value: 
        :type contour_value: float
  
        :param use_bb: If True, compute nearness and intersection using the contours bounding boxes, not the geometry
        :type use_bb: bool
        
        :param use_distance: If not False, consider contours that are closer than this value to be overlapping. 
        :type : number
        
        :rtype: Returns a list of dictionaries, one for each of the combined bounding boxes
        
        This method will store, in the `shape_file_dir` directory:
        
        * a GeoTIFF representation of the array `a`
        * An ERSI shapefile layer  named `countours`, holding all of the countours. 
        * A layer named `contour_bounds` with the bounding boxes for all of the contours with value `contour_value`
        * A layer named `combined_bounds` with bounding boxes of intersecting and nearby boxes rom `contour_bounds`
        
        The routine will iteratively combine contours that overlap. 
        
        If `use_distance` is set to a number, and contours that are closer than this value will be joined. 
        
        If `use_bb` is set, the intersection and distance computations use the bounding boxes of the contours, 
        not the contours themselves. 
        
     
        """
        
        import ambry.geo as dg
        from osgeo.gdalconst import GDT_Float32
        import ambry.util as util
        
        from osgeo import gdal
        import ogr, os
        import numpy as np
       
        if os.path.exists(shape_file_dir):
            util.rm_rf(shape_file_dir)
            os.makedirs(shape_file_dir)
        
        rasterf = os.path.join(shape_file_dir,'contour.tiff')

        ogr_ds = ogr.GetDriverByName('ESRI Shapefile').CreateDataSource(shape_file_dir)
        
        # Setup the countour layer. 
        ogr_lyr = ogr_ds.CreateLayer('contours', aa.srs)
        ogr_lyr.CreateField(ogr.FieldDefn('id', ogr.OFTInteger))
        ogr_lyr.CreateField(ogr.FieldDefn('value', ogr.OFTReal))
        
        # Create the contours from the GeoTIFF file. 
        ds = aa.get_geotiff(rasterf, data_type=GDT_Float32)
        ds.GetRasterBand(1).SetNoDataValue(0)
        ds.GetRasterBand(1).WriteArray(np.flipud(a))
        
        gdal.ContourGenerate(ds.GetRasterBand(1), 
                             contour_interval,  # contourInterval
                             0,   # contourBase
                             [],  # fixedLevelCount
                             0, # useNoData
                             0, # noDataValue
                             ogr_lyr, #destination layer
                             0,  #idField
                             1 # elevation field
                             )

 
        # Get buffered bounding boxes around each of the hotspots, 
        # and put them into a new layer. 
 
        bound_lyr = ogr_ds.CreateLayer('contour_bounds', aa.srs)
        for i in range(ogr_lyr.GetFeatureCount()):
            f1 = ogr_lyr.GetFeature(i)
            if f1.GetFieldAsDouble('value') != contour_value:
                continue
            g1 = f1.GetGeometryRef()
            bb = dg.create_bb(g1.GetEnvelope(), g1.GetSpatialReference())
            f = ogr.Feature(bound_lyr.GetLayerDefn())
            f.SetGeometry(bb)
            bound_lyr.CreateFeature(f)
            
    
        # Doing a full loop instead of a list comprehension b/c the way that comprehensions
        # compose arrays results in segfaults, probably because a copied geometry
        # object is being released before being used. 
        geos = []
        for i in range(bound_lyr.GetFeatureCount()):
            f = bound_lyr.GetFeature(i)
            g = f.geometry()
            geos.append(g.Clone())
    
        # Combine hot spots that have intersecting bounding boxes, to get larger
        # areas that cover all of the adjacent intersecting smaller areas. 
        geos = dg.combine_envelopes(geos, use_bb=use_bb, use_distance = use_distance) 

        # Write out the combined bounds areas. 
        lyr = ogr_ds.CreateLayer('combined_bounds', aa.srs)
        lyr.CreateField(ogr.FieldDefn('id', ogr.OFTInteger))
        lyr.CreateField(ogr.FieldDefn('area', ogr.OFTReal))
        lyr.CreateField(ogr.FieldDefn('name', ogr.OFTString))
        lyr.CreateField(ogr.FieldDefn('code', ogr.OFTString))
        
        envelopes = []
        id = 1
        for env in geos:
            f = ogr.Feature(lyr.GetLayerDefn())
            bb = dg.create_bb(env.GetEnvelope(), env.GetSpatialReference())
            f.SetGeometry(bb)
            f.SetField(0, id)
            f.SetField(1, bb.Area())
            f.SetField(2, None)
            f.SetField(3, None)   
            id += 1         
            lyr.CreateFeature(f)
            envelopes.append({'id':id, 'env':bb.GetEnvelope(), 'area':bb.Area()})
            

        return envelopes

geo_type_map = {'1': 'GEOMETRY',
            '2': 'GEOMETRYCOLLECTION',
            '3': 'POINT',
            'Point': 'POINT',
            '4': 'MULTIPOINT',
            '5': 'POLYGON',
            '6': 'MULTIPOLYGON',
            '7': 'LINESTRING',
            'Line String': 'LINESTRING',
            '3D Line String': 'LINESTRING',
            '8': 'MULTILINESTRING',
            '3D Multi Line String': 'MULTILINESTRING',
            'Multi Line String': 'MULTILINESTRING',
            '3D Point': 'POINT',
            '3D Multi Point': 'MULTIPOINT',
            'Polygon': 'POLYGON',
            '3D Polygon': 'POLYGON',
            'Multi Polygon': 'MULTIPOLYGON',
            '3D Multi Polygon': 'MULTIPOLYGON',
}

def get_type_from_geometry(geometry):

   return  geo_type_map[ogr.GeometryTypeToName(geometry.GetGeometryType())]

def get_shapefile_geometry_types(shape_file):
    

    
        shapes = ogr.Open(shape_file)
        layer = shapes.GetLayer(0)

        types = set()
        type_ = None
        
        limit = 20000
        count = layer.GetFeatureCount()
        if count > limit:
            skip = layer.GetFeatureCount() / limit
        else:
            skip = 1
            
        checked = 0
        for i in range(0,layer.GetFeatureCount(),skip):
            feature = layer.GetFeature(i)
            geometry = feature.geometry()
            if geometry:
                types.add(geo_type_map[ogr.GeometryTypeToName(geometry.GetGeometryType())])
                checked += 1
            else:
                # This happens rarely, seems to be a problem with the source shapefiles.
                pass

        if len(types) == 1:
            type_ = list(types).pop()
        elif len(types) == 2:
            if set(('POLYGON','MULTIPOLYGON')) & types == set(('POLYGON','MULTIPOLYGON')):
                type_ = 'MULTIPOLYGON'
            elif set(('POINT', 'MULTIPOINT')) & types == set(('POINT', 'MULTIPOINT')):
                type_ = 'MULTIPOINT'
            elif set(('LINESTRING', 'MULTILINESTRING')) & types == set(('LINESTRING', 'MULTILINESTRING')):
                type_ = 'MULTILINESTRING'
            else:
                raise Exception("Didn't get valid combination of types: "+str(types))
        else:
            raise Exception("Can't deal with files that have three more type_ different geometry types, or less than one: "+str(types))
     
    
        return types, type_
    
def segment_points(areas,table_name=None,  query_template=None, places_query=None, bb_clause=None, bb_type='ll'):
    """A generator that yields information that can be used to classify
    points into areas
    
    :param areas: A `Bundle`or `partition` object with access to the places database
    :param query: A Query to return places. Must return,  for each row,  fields names 'id' ,'name'
    and 'wkt'
    :param bb_type: Either 'll' to use lon/lat for the bounding box query, or 'xy' to use x/y for the query
    :rtype: a `LibraryDb` object
    
    
    The 'wkt' field returned by the query is the Well Know Text representation of the area
    geometry
    
    """
    
    import osr
  
    dest_srs = ogr.osr.SpatialReference()
    dest_srs.ImportFromEPSG(4326)

    source_srs = areas.get_srs()

    transform = osr.CoordinateTransformation(source_srs, dest_srs)
    
    if query_template is None:
        # Took the 'empty_clause' out because it is really slow if there is no index. 
        empty_clause = "AND ({target_col} IS NULL OR {target_col} = 'NONE' OR {target_col} = '-')"
        query_template =  "SELECT * FROM {table_name} WHERE {bb_clause}  "
    
    if places_query is None:
        places_query = "SELECT *, AsText(geometry) AS wkt FROM {} ORDER BY area ASC".format(areas.identity.table)
    
    if bb_clause is None:
        if bb_type == 'll':
            bb_clause = "lon BETWEEN {x1} AND {x2} AND lat BETWEEN {y1} and {y2}"
        elif bb_type == 'xy':
            bb_clause = "x BETWEEN {x1} AND {x2} AND y BETWEEN {y1} and {y2}"
        else:
            raise ValueError("Must use 'll' or 'xy' for bb_type. got: {}".format(bb_type))
   
    for area in areas.query(places_query):
     
        g = ogr.CreateGeometryFromWkt(area['wkt'])
        g.Transform(transform)
        
        e = g.GetEnvelope()

        bb = bb_clause.format(x1=e[0], x2=e[1], y1=e[2], y2=e[3])
        query = query_template.format(bb_clause=bb, table_name = table_name, target_col=area['type'])      
        
        def is_in(x, y):
            """Clients call this closure to make the determination if the
            point is in the area"""
            p = ogr.Geometry(ogr.wkbPoint)
            p.SetPoint_2D(0, x, y)

            if g.Contains(p):
                return True
            else:
                return False
        
        area = dict(area)

        yield area, query, is_in

def find_geo_containment(containers, containeds, sink, method = 'contains'):
    """Call a callback for each point that is contained in a geometry.

    `containers` is an iterable that holds  -- or an generator that yields -- the geometry of the containing objects  :

            ( id, poly_obj, WKT)

    `Id` must be an integer that is unique for the polygon. `Poly_obj` can be any object to return to the callback.
    `WKT` is the polygon in WKT format.

    `containeds` holds or yields  the contained gemoetries. :

            ( coord, contained_obj )

    `Coords` is a tuple of floats and `contained_obj` is any object.

    If coords is two floats, they are the X and Y for a point. If it is four, they are the  (minx, miny, maxx, maxy) for the
    bounding box of a geometry.


    For each point that is contained in a polygon, the routine calls sends to the `sink` generator, which should have a line like:

        Point(x,y), contained_obj, poly_obj)  = yield

    Where `Point` is a shapely point.

    """
    from rtree import index
    from shapely.geometry import Point, Polygon
    from shapely.wkt import loads
    from collections import Iterable

    # Maybe this is only a performance improvement if the data is sorted in the generator ...
    def gen_index():
        for i, container_obj, wkt in containers:
            container_geometry = loads(wkt)

            yield (i, container_geometry.bounds, (container_obj, container_geometry))

    idx = index.Index(gen_index())

    # Start the sink generator
    sink.send(None)

    # Find the point containment
    for contained_coords, contained_obj in containeds:

        locations = idx.intersection(contained_coords, objects=True)

        if len(contained_coords) == 2:
            contained = Point(contained_coords)
        elif len(contained_coords) == 4 and not isinstance(contained_coords[0], Iterable):
            # Assume it is bounding box coords. If the elements were iterables ( x,y points ) it
            # could be something else.
            # bounding boxes are: (minx, miny, maxx, maxy)
            (minx, miny, maxx, maxy) = contained_coords
            shape = (
                (minx, miny),
                (minx, maxy),
                (maxx, maxy),
                (maxx, miny)
            )
            contained = Polygon(shape)

        for r in locations:
            container_obj, container_geometry = r.object

            if method == 'contains':
                test = container_geometry.contains(contained)
            elif method == 'intersects':
                test = container_geometry.intersects(contained)

            if test:
                sink.send( (contained, contained_obj, container_geometry, container_obj) )

    sink.close()

def find_containment(containers, containeds, method = 'contains'):
    """Generator version of find_geo_containment, yielding for each point that is contained in a geometry.

    `containers` is an iterable that holds  -- or an generator that yields -- the geometry of the containing objects  :

            ( id, poly_WKT,  poly_obj)

    `Id` must be an integer that is unique for the polygon. `Poly_obj` can be any object to return to the callback.
    `WKT` is the polygon in WKT format.

    `containeds` holds or yields  the contained gemoetries. :

            ( coord, contained_obj )

    `Coords` is a tuple of floats and `contained_obj` is any object.

    If coords is two floats, they are the X and Y for a point. If it is four, they are the  (minx, miny, maxx, maxy) for the
    bounding box of a geometry.


    For each point that is contained in a polygon, the routine yields:

        coord_geometry, contained_obj, WKT_geometry, poly_obj

    Where coord_geometry is a Shapely gepmetry constructed by the contained coords, and WKT_geometry is a shapely object
    constructed from the container WKT.

    """
    from rtree import index
    from rtree.core import RTreeError
    from shapely.geometry import Point, Polygon
    from shapely.wkt import loads
    from collections import Iterable
    from ..dbexceptions import GeoError


    # Maybe this is only a performance improvement if the data is sorted in the generator ...
    def gen_index():
        for i, wkt, container_obj in containers:
            container_geometry = loads(wkt)

            yield (i, container_geometry.bounds, (container_obj, container_geometry))

    try:
        idx = index.Index(gen_index())
    except RTreeError:
        raise GeoError("Failed to create RTree Index. Check that the container generator produced valud output")



    # Find the point containment
    for contained_coords, contained_obj in containeds:

        locations = idx.intersection(contained_coords, objects=True)

        if len(contained_coords) == 2:
            contained = Point(contained_coords)
        elif len(contained_coords) == 4 and not isinstance(contained_coords[0], Iterable):
            # Assume it is bounding box coords. If the elements were iterables ( x,y points ) it
            # could be something else.
            # bounding boxes are: (minx, miny, maxx, maxy)
            (minx, miny, maxx, maxy) = contained_coords
            shape = (
                (minx, miny),
                (minx, maxy),
                (maxx, maxy),
                (maxx, miny)
            )
            contained = Polygon(shape)

        for r in locations:
            container_obj, container_geometry = r.object

            if method == 'contains':
                test = container_geometry.contains(contained)
            elif method == 'intersects':
                test = container_geometry.intersects(contained)

            if test:
                yield (contained, contained_obj, container_geometry, container_obj)


def recover_geometry(connection, table_name, column_name, geometry_type=None, srs=None):
    from ..orm import Geometry

    assert table_name
    assert column_name

    if not srs:
        srs = Geometry.DEFAULT_SRS

    if geometry_type is None or geometry_type.lower() == 'blob':
        # If the geometry type isn't defined, us the type of the first record.
        row = connection.execute('SELECT GeometryType({}) FROM {} LIMIT 1'.format(column_name, table_name)).fetchone()
        geometry_type = row[0]

    connection.execute(
        'UPDATE {} SET {} = SetSrid({}, {});'.format(table_name, column_name, column_name, srs))

    q = "SELECT RecoverGeometryColumn('{}', '{}', {}, '{}', 2);".format(table_name, column_name, srs, geometry_type)

    connection.execute(q)