"""Classes for converting warehouse databases to other formats.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import ogr


class ExtractError(Exception):
    pass


class ExtractEntry(object):

    def __init__(self, extracted, rel_path, abs_path, data=None):
        self.extracted = extracted
        self.rel_path = rel_path
        self.abs_path = abs_path
        self.data = data
        self.time = None

    def __str__(self):
        return 'extracted={} rel={} abs={} data={}'.format(
            self.extracted,
            self.rel_path,
            self.abs_path,
            self.data)


def new_extractor(format, warehouse, cache, force=False):

    ex_class = extractors.get(format.lower(), False)

    if not ex_class:
        raise ValueError("Unknown format: {} ".format(format))

    return ex_class(warehouse, cache, force=force)


def get_extractors(t):

    return [format for format, ex in extractors.items() if ex.can_extract(t)]


class Extractor(object):

    is_geo = False

    def __init__(self, warehouse, cache, force=False):

        self.warehouse = warehouse
        self.database = self.warehouse.database
        self.cache = cache
        self.force = force
        self.hash = None

    def mangle_path(self, rel_path):
        return rel_path

    def extract(self, table, rel_path, update_time=None):
        import time

        e = ExtractEntry(
            False,
            rel_path,
            self.cache.path(
                self.mangle_path(rel_path),
                missing_ok=True),
            (table,
             self.__class__))

        force = self.force

        md = self.cache.metadata(self.mangle_path(rel_path))

        # If the table was created after
        if md and 'time' in md and update_time and int(md['time']) - int(update_time) < 0:

            force = True

        if self.cache.has(self.mangle_path(rel_path)):
            if force:
                self.cache.remove(self.mangle_path(rel_path), True)
            else:
                return e

        md = {
            'time': time.time()
        }

        self._extract(table, rel_path, md)
        e.time = time.time()
        e.extracted = True
        return e


class CsvExtractor(Extractor):

    mime = 'text/csv'

    def __init__(self, warehouse, cache, force=False):
        super(CsvExtractor, self).__init__(warehouse, cache, force=force)

    @classmethod
    def can_extract(cls, t):
        return True

    def _extract(self, table, rel_path, metadata):

        import unicodecsv

        rel_path = self.mangle_path(rel_path)

        row_gen = self.warehouse.database.connection.execute(
            "SELECT * FROM {}".format(table))

        with self.cache.put_stream(rel_path, metadata=metadata) as stream:
            w = unicodecsv.writer(stream)

            for i, row in enumerate(row_gen):
                if i == 0:
                    w.writerow(row.keys())

                w.writerow(row)

        return True, self.cache.path(rel_path)


class JsonExtractor(Extractor):

    mime = 'application/json'

    def __init__(self, warehouse, cache, force=False):
        super(JsonExtractor, self).__init__(warehouse, cache, force=force)

    @classmethod
    def can_extract(cls, t):
        return True

    def _extract(self, table, rel_path, metadata):
        import json

        rel_path = self.mangle_path(rel_path)

        row_gen = self.warehouse.database.connection.execute(
            "SELECT * FROM {}".format(table))

        # A template to ensure the JSON head and tail are properly formatted
        head, mid, tail = json.dumps(
            {'header': [0], 'rows': [[0]]}).split('[0]')

        with self.cache.put_stream(rel_path, metadata=metadata) as stream:

            stream.write(head)

            for i, row in enumerate(row_gen):
                if i == 0:
                    stream.write(json.dumps(row.keys()))
                    stream.write(mid)
                else:
                    stream.write(',\n')

                stream.write(json.dumps(list(row)))

            stream.write(tail)

        return True, self.cache.path(rel_path)


class OgrExtractor(Extractor):

    epsg = 4326

    is_geo = True

    def __init__(self, warehouse, cache, force=False):

        super(OgrExtractor, self).__init__(warehouse, cache, force=force)

        self.mangled_names = {}

    def geometry_type(self, database, table):
        """Return the name of the most common geometry type and the coordinate dimensions"""
        ce = database.connection.execute

        # Only deal with the first geometry column per table. Unfortunately, the table is often
        # actually a view, so you can't use it to look up the geometry column in the
        # 'geometry_columns' table. Instead, we'll have to get al of the geometry columns, and see
        # which ones we have in out table.
        q = "SElECT f_geometry_column FROM geometry_columns " .format(table)
        geo_cols = [row['f_geometry_column'] for row in ce(q).fetchall()]

        all_cols = ce(
            "SELECT * FROM {} LIMIT 1".format(table)).fetchone().keys()

        geo_col = None
        for col in all_cols:
            if col in geo_cols:
                geo_col = col
                break

        if not geo_col:
            print all_cols
            print geo_cols

        types = ce(
            'SELECT count(*) AS count, GeometryType({geo}) AS type,  CoordDimension({geo}) AS cd '
            'FROM {table} GROUP BY type ORDER BY type desc;' .format(
                geo=geo_col,
                table=table)).fetchall()

        t = types[0][1]
        cd = types[0][2]

        if not t:
            raise ExtractError("No geometries in {}".format(table))

        return t, cd, geo_col

    geo_map = {
        'POLYGON': ogr.wkbPolygon,
        'MULTIPOLYGON': ogr.wkbMultiPolygon,
        'POINT': ogr.wkbPoint,
        'MULTIPOINT': ogr.wkbMultiPoint,
        # There are a lot more , add them as they are encountered.
    }

    _ogr_type_map = {
        None: ogr.OFTString,
        '': ogr.OFTString,
        'TEXT': ogr.OFTString,
        'VARCHAR': ogr.OFTString,
        'INT': ogr.OFTInteger,
        'INTEGER': ogr.OFTInteger,
        'REAL': ogr.OFTReal,
        'FLOAT': ogr.OFTReal,
    }

    def ogr_type_map(self, v):
        return self._ogr_type_map[
            v.split(
                '(', 1)[0]]  # Sometimes 'VARCHAR', sometimes 'VARCHAR(10)'

    @classmethod
    def can_extract(cls, t):

        for c in t.columns:
            if c.name == 'geometry':
                return True

        return False

    def create_schema(self, database, table, layer):
        ce = database.connection.execute

        # TODO! pragma only works in sqlite
        for row in ce('PRAGMA table_info({})'.format(table)).fetchall():

            if row['name'].lower() in ('geometry', 'wkt', 'wkb'):
                continue

            name = self.mangle_name(str(row['name']))

            try:
                fdfn = ogr.FieldDefn(name, self.ogr_type_map(row['type']))
            except KeyError:
                continue

            print "CREATE", name, self.ogr_type_map(row['type'])

            if row['type'] == '':
                # FIXME Wasteful, but would have to scan table for max value.
                fdfn.SetWidth(254)

            layer.CreateField(fdfn)

    def new_layer(self, abs_dest, name, t):

        ogr.UseExceptions()

        driver = ogr.GetDriverByName(self.driver_name)

        ds = driver.CreateDataSource(abs_dest)

        if ds is None:
            raise ExtractError(
                "Failed to create data source for driver '{}' at dest '{}'" .format(
                    self.driver_name,
                    abs_dest))

        srs = ogr.osr.SpatialReference()
        srs.ImportFromEPSG(self.epsg)

        # Gotcha! You can't create a layer with a unicode layername!
        # http://gis.stackexchange.com/a/53939/12543
        layer = ds.CreateLayer(name.encode('utf-8'), srs, self.geo_map[t])

        return ds, layer

    def mangle_name(self, name):

        if '_' in name:
            _, name = name.split('_', 1)

        if len(name) <= self.max_name_len:
            return name

        if name in self.mangled_names:
            return self.mangled_names[name]

        for i in range(0, 20):
            mname = name[:self.max_name_len] + str(i)
            if mname not in self.mangled_names.values():
                self.mangled_names[name] = mname
                return mname

        raise Exception(
            "Ran out of names {} is still in {}".format(
                name,
                self.mangled_names.values()))

    def _extract_shapes(self, abs_dest, table):

        import ogr
        import os

        t, cd, geo_col = self.geometry_type(self.database, table)

        ds, layer = self.new_layer(abs_dest, table, t)

        self.create_schema(self.database, table, layer)

        # TODO AsTest, etc, will have to change to ST_AsText for Postgis
        q = "SELECT *, AsText(Transform({}, {} )) AS _wkt FROM {}".format(
            geo_col,
            self.epsg,
            table)

        for i, row in enumerate(self.database.connection.execute(q)):

            feature = ogr.Feature(layer.GetLayerDefn())

            for name, value in row.items():
                if name.lower() in (geo_col, 'geometry', 'wkt', 'wkb', '_wkt'):
                    continue

                if value:
                    try:
                        if isinstance(value, unicode):
                            value = str(value)

                        name = self.mangle_name(str(name))

                        feature.SetField(name, value)
                    except Exception as e:
                        print 'Failed for {}={} ({})'.format(name, value, type(value))
                        raise
                    except NotImplementedError as e:
                        print e
                        raise

            geometry = ogr.CreateGeometryFromWkt(row['_wkt'])

            feature.SetGeometryDirectly(geometry)
            if layer.CreateFeature(feature) != 0:
                import gdal
                raise Exception(
                    'Failed to add feature: {}: geometry={}'.format(
                        gdal.GetLastErrorMsg(),
                        geometry.ExportToWkt()))

            feature.Destroy()

        ds.SyncToDisk()
        ds.Release()

        return True, abs_dest


class ShapeExtractor(OgrExtractor):

    mime = 'application/zip'

    driver_name = 'Esri Shapefile'
    max_name_len = 8  # For ESRI SHapefiles

    def mangle_path(self, rel_path):
        if not rel_path.endswith('.zip'):
            rel_path += '.zip'

        return rel_path

    def zip_dir(self, layer_name, source_dir, dest_path):
        """
        layer_name The name of the top level directory in
        """
        import zipfile
        import os

        zf = zipfile.ZipFile(dest_path, 'w', zipfile.ZIP_DEFLATED)

        for root, dirs, files in os.walk(source_dir):
            for f in files:
                zf.write(os.path.join(root, f), os.path.join(layer_name, f))

            zf.close()

    def _extract(self, table, rel_path, metadata):

        from ambry.util import temp_file_name
        from ambry.util.flo import copy_file_or_flo
        import shutil
        import os

        rel_path = self.mangle_name(rel_path)

        shapefile_dir = temp_file_name()

        self._extract_shapes(shapefile_dir, table)

        zf = temp_file_name()

        self.zip_dir(table, shapefile_dir, zf)

        copy_file_or_flo(
            zf,
            self.cache.put_stream(
                rel_path,
                metadata=metadata))

        shutil.rmtree(shapefile_dir)
        os.remove(zf)

        return self.cache.path(rel_path)


class GeoJsonExtractor(OgrExtractor):

    mime = 'application/json'

    driver_name = 'GeoJSON'
    max_name_len = 40

    def temp_dest(self):
        from ambry.util import temp_file_name
        return temp_file_name()

    def _extract(self, table, rel_path, metadata):
        from ambry.util import temp_file_name
        from ambry.util.flo import copy_file_or_flo
        import os

        rel_path = self.mangle_name(rel_path)

        tf = temp_file_name() + '.geojson'

        self._extract_shapes(tf, table)

        copy_file_or_flo(
            tf,
            self.cache.put_stream(
                rel_path,
                metadata=metadata))

        os.remove(tf)

        return self.cache.path(rel_path)


class KmlExtractor(OgrExtractor):

    mime = 'application/vnd.google-earth.kml+xml'

    driver_name = 'KML'
    max_name_len = 40

    def _extract(self, table, rel_path, metadata):
        import tempfile
        from ambry.util import temp_file_name
        from ambry.util.flo import copy_file_or_flo
        import os

        rel_path = self.mangle_name(rel_path)

        tf = temp_file_name()

        self._extract_shapes(tf, table)

        copy_file_or_flo(
            tf,
            self.cache.put_stream(
                rel_path,
                metadata=metadata))

        os.remove(tf)

        return self.cache.path(rel_path)


extractors = dict(
    csv=CsvExtractor,
    json=JsonExtractor,
    shapefile=ShapeExtractor,
    geojson=GeoJsonExtractor,
    kml=KmlExtractor
)


def geo_extractors():
    return [f for f, e in extractors if e.is_geo]


def table_extractors():
    return [f for f, e in extractors if not e.is_geo]
