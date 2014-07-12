"""Classes for converting warehouse databases to other formats.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import ogr

def extract(database, table, format, cache, dest):

    from ambry.warehouse.extractors import CsvExtractor


    ex = dict(
        csv=CsvExtractor(),
        shapefile = ShapeExtractor()
    ).get(format, False)


    if not ex:
        raise ValueError("Unknown format name '{}'".format(format))

    if not cache.has(dest):

        ex.extract(database, table, cache, dest)

    return cache.path(dest)

class CsvExtractor(object):

    def __init__(self):
        pass

    def extract(self, database, table, cache, dest):

        import unicodecsv

        row_gen = database.connection.execute("SELECT * FROM {}".format(table))

        w = unicodecsv.writer(cache.put_stream(dest))

        for i,row in enumerate(row_gen):
            if i == 0:
                w.writerow(row.keys())

            w.writerow(row)

class ShapeExtractor(object):

    def __init__(self):
        pass


    def geometry_type(self, database, table):
        """Return the name of the most common geometry type and the coordinate dimensions"""
        ce = database.connection.execute

        types = ce('SELECT count(*) AS count, GeometryType(geometry) AS type,  CoordDimension(geometry) AS cd '
                   'FROM {} GROUP BY type ORDER BY type desc;'.format(table)).fetchall()

        t = types[0][1]
        cd = types[0][2]

        return t, cd

    geo_map = {
        'POLYGON': ogr.wkbPolygon,
        'MULTIPOLYGON': ogr.wkbMultiPolygon,
        'POINT': ogr.wkbPoint,
        'MULTIPOINT': ogr.wkbMultiPoint,
        # There are a lot more , add them as they are encountered.
    }

    ogr_type_map = {
        None: ogr.OFTString,
        '': ogr.OFTString,
        'TEXT': ogr.OFTString,
        'INT': ogr.OFTInteger,
        'REAL': ogr.OFTReal,

    }

    def create_schema(self, database, table, layer):
        ce = database.connection.execute

        for row in ce('PRAGMA table_info({})'.format(table)).fetchall():

            if row['name'].lower() in ('geometry', 'wkt','wkb'):
                continue

            fdfn = ogr.FieldDefn(str(row['name']), self.ogr_type_map[row['type']])

            if row['type'] == '':
                fdfn.SetWidth(254) # FIXME Wasteful, but would have to scan table for max value.

            layer.CreateField(fdfn)

    def extract(self, database, table, cache, dest):

        import ogr

        epsg = 4326

        q = """
        SELECT *, AsText(Transform(geometry, {} )) AS _wkt
        FROM {}
        """.format(epsg, table)

        abs_dest = cache.path(dest, missing_ok = True)

        driver = ogr.GetDriverByName('Esri Shapefile')
        ds = driver.CreateDataSource(abs_dest)

        srs = ogr.osr.SpatialReference()
        srs.ImportFromEPSG(epsg)

        layer = ds.CreateLayer(table, srs, self.geo_map[self.geometry_type(database, table)[0]])

        self.create_schema(database, table, layer)

        for i,row in enumerate(database.connection.execute(q)):

            feature = ogr.Feature(layer.GetLayerDefn())

            for name, value in row.items():
                if name.lower() in ('geometry', 'wkt', 'wkb', '_wkt'):
                    continue
                if value:
                    try:
                        if isinstance(value, unicode):
                            value = str(value)

                        feature.SetField(str(name), value)
                    except Exception as e:
                        print 'Failed for {}={} ({})'.format(name, value, type(value))
                        raise


            geometry = ogr.CreateGeometryFromWkt(row['_wkt'])

            feature.SetGeometryDirectly(geometry)
            if layer.CreateFeature(feature) != 0:
                import gdal
                raise Exception(
                    'Failed to add feature: {}: geometry={}'.format(gdal.GetLastErrorMsg(), geometry.ExportToWkt()))

            feature.Destroy()

        ds.SyncToDisk()
        ds.Release()


