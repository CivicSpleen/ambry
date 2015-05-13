"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""

# import sys
from ..identity import PartitionIdentity, PartitionName
from sqlite import SqlitePartition


def _geo_db_class():  # To break an import dependency

    from ambry.database.geo import GeoDb

    return GeoDb


class GeoPartitionName(PartitionName):
    PATH_EXTENSION = '.geodb'
    FORMAT = 'geo'


class GeoPartitionIdentity(PartitionIdentity):
    _name_class = GeoPartitionName


class GeoPartition(SqlitePartition):

    """A Partition that hosts a Spatialite for geographic data."""

    _id_class = GeoPartitionIdentity

    is_geo = True

    def __init__(self, bundle, record, **kwargs):
        super(GeoPartition, self).__init__(bundle, record)

    @property
    def database(self):
        if self._database is None:
            _db_class = _geo_db_class()
            self._database = _db_class(self.bundle, self, base_path=self.path)
        return self._database

    def _get_srs_wkt(self):

        #
        # !! Assumes only one layer!

        try:
            q = "select srs_wkt, spatial_ref_sys.srid " \
                "from geometry_columns, spatial_ref_sys " \
                "where spatial_ref_sys.srid == geometry_columns.srid;"
            return self.database.query(q).first()
        except:
            q = "select srtext, spatial_ref_sys.srid " \
                "from geometry_columns, spatial_ref_sys " \
                "where spatial_ref_sys.srid == geometry_columns.srid;"
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
        """Get an ogr transform object to convert from the SRS of this
        partition to another."""
        import ogr
        import osr

        srs2 = ogr.osr.SpatialReference()
        srs2.ImportFromEPSG(dest_srs)
        transform = osr.CoordinateTransformation(self.get_srs(), srs2)

        return transform

    def add_tables(self, tables):
        """Declare geometry columns to spatialite.

        :param tables:
        :return:

        """

        super(GeoPartition, self).add_tables(tables)

        for table_name in tables:
            t = self.bundle.schema.table(table_name)
            for c in t.columns:
                if c.name == 'geometry':
                    self.database.recover_geometry(
                        t.name,
                        c.name,
                        c.datatype.upper())

    def convert(self, table_name, progress_f=None):
        """Convert a spatialite geopartition to a regular arg
        by extracting the geometry and re-projecting it to WGS84

        # :param config: a `RunConfig` object
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
            self.bundle.schema.add_column(t, c.name, datatype=c.datatype)

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
            raise ConfigurationError(
                'Did not find spatialite on path. Install spatialite')

        # Check the type of geometry:
        p = subprocess.Popen(
            ('spatialite {file} "select GeometryType(geometry) FROM {table} LIMIT 1;"' .format(
                file=self.database.path,
                table=self.identity.table)),
            stdout=subprocess.PIPE,
            shell=True)

        out, _ = p.communicate()
        out = out.strip()

        if out == 'POINT':
            self.bundle.schema.add_column(
                t,
                '_db_lon',
                datatype=Column.DATATYPE_REAL)
            self.bundle.schema.add_column(
                t,
                '_db_lat',
                datatype=Column.DATATYPE_REAL)

            command_template = """spatialite -csv -header {file} "select *,
            X(Transform(geometry, 4326)) AS _db_lon, Y(Transform(geometry, 4326)) AS _db_lat
            FROM {table}" """
        else:
            self.bundle.schema.add_column(
                t,
                '_wkb',
                datatype=Column.DATATYPE_TEXT)

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
                                          table=self.identity.table)

        self.bundle.log("Running: {}".format(command))

        p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
        stdout, stderr = p.communicate()

        #
        # Finally we can copy the data.
        #

        # local csv module shadows root #@UndefinedVariable
        rdr = csv.reader(stdout.decode('ascii').splitlines())
        # header = rdr.next()
        rdr.next()

        if not progress_f:
            progress_f = lambda x: x

        with arg.database.inserter(table_name) as ins:
            for i, line in enumerate(rdr):
                ins.insert(line)
                progress_f(i)

    def convert_dates(self, table_name):
        """Remove the 'T' at the end of dates that OGR adds erroneously."""
        from ambry.orm import Column

        table = self.bundle.schema.table(table_name)

        clauses = []

        for column in table.columns:
            if column.datatype == Column.DATATYPE_DATE:
                clauses.append(
                    "{col} = REPLACE({col},'T','')".format(
                        col=column.name))

        if clauses:

            self.database.connection.execute("UPDATE {} SET {}".format(table.name,  ','.join(clauses)))

    def load_shapefile(self, path, logger=None):
        """Load a shapefile into the partition. Loads the features and inserts
        them using an inserter.

        :param path:
        :return:

        """

        from osgeo import ogr  # , osr
        from ..geo.sfschema import ogr_inv_type_map, mangle_name
        from ..orm import Column, Geometry
        from ..geo.util import get_type_from_geometry

        if path.startswith('http'):
            shape_url = path
            path = self.bundle.filesystem.download_shapefile(shape_url)

        driver = ogr.GetDriverByName("ESRI Shapefile")

        dataSource = driver.Open(path, 0)

        layer = dataSource.GetLayer()

        to_srs = ogr.osr.SpatialReference()
        to_srs.ImportFromEPSG(Geometry.DEFAULT_SRS)

        dfn = layer.GetLayerDefn()

        col_defs = []

        for i in range(0, dfn.GetFieldCount()):
            field = dfn.GetFieldDefn(i)

            col_defs.append(
                (Column.mangle_name(
                    mangle_name(
                        field.GetName())),
                    Column.types[
                        ogr_inv_type_map[
                            field.GetType()]][1]))

        col_type = None
        for c in self.table.columns:
            if c.name == 'geometry':
                col_type = c.datatype.upper()
                break

        assert col_type is not None

        with self.inserter() as ins:
            for feature in layer:
                d = {}
                for i in range(0, dfn.GetFieldCount()):
                    name, type_ = col_defs[i]
                    try:
                        d[name] = feature.GetFieldAsString(i)
                    except TypeError as e:
                        self.bundle.logger.error(
                            "Type error for column '{}', type={}: {}".format(
                                name,
                                type_,
                                e))
                        raise

                g = feature.GetGeometryRef()
                g.TransformTo(to_srs)

                type_ = get_type_from_geometry(g)

                if type_ != col_type:
                    if type_ == 'POLYGON' and col_type == 'MULTIPOLYGON':
                        g = ogr.ForceToMultiPolygon(g)
                    else:
                        raise Exception(
                            "Don't know how to handle this conversion case : {} -> {}".format(type_, col_type))

                d['geometry'] = g.ExportToWkt()

                ins.insert(d)

                if logger:
                    logger(
                        "Importing shapefile to '{}'".format(
                            self.identity.name))

    def __repr__(self):
        return "<geo partition: {}>".format(self.name)

    @property
    def info(self):
        """Returns a human readable string of useful information."""

        try:
            srid = self.get_srid()
        except Exception as e:
            self.bundle.error(e)
            srid = 'error'

        return super(GeoPartition, self).info + \
            '{:10s}: {}\n'.format('SRID', srid)
