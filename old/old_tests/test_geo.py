"""
Created on Jan 17, 2013

@author: eric
"""
import unittest

from osgeo.gdalconst import GDT_Float32
from testbundle.bundle import Bundle
from ambry.identity import *  # @UnusedWildImport
from test_base import TestBase


class Test(TestBase):
    def setUp(self):
        self.copy_or_build_bundle()

        self.bundle = Bundle()
        self.bundle_dir = self.bundle.bundle_dir

    def tearDown(self):
        pass

    def test_geo_schema(self):

        import support
        import os.path
        from ambry.geo.sfschema import copy_schema

        def geo_file(p):
            return os.path.join(os.path.dirname(support.__file__), 'neighborhoods', p)

        url = "http://rdw.sandag.org/file_store/Business/Business_Sites.zip"
        path = geo_file("Neighborhoods_SD.shp")

        with self.bundle.session:
            copy_schema(self.bundle.schema, url)

        print self.bundle.schema.as_csv()

    def x_test_basic(self):
        from old.analysisarea import get_analysis_area
        from ambry.geo import Point
        from ambry.geo.kernel import GaussianKernel

        aa = get_analysis_area(self.bundle.library, geoid='CG0666000')

        a = aa.new_array()

        # draw_edges(a)
        print a.shape, a.size

        gaussian = GaussianKernel(11, 6)

        for i in range(0, 400, 20):
            p = Point(100 + i, 100 + i)
            gaussian.apply_add(a, p)

        aa.write_geotiff('/tmp/box.tiff', a, data_type=GDT_Float32)

    def test_sfschema(self):
        from ambry.geo.sfschema import TableShapefile
        from old.analysisarea import get_analysis_area

        _, communities = self.bundle.library.dep('communities')

        csrs = communities.get_srs()

        gp = self.bundle.partitions.new_geo_partition(table='geot2')
        with gp.database.inserter(source_srs=csrs) as ins:
            for row in communities.query("""
            SELECT *, 
            X(Transform(Centroid(geometry), 4326)) AS lon, 
            Y(Transform(Centroid(geometry), 4326)) as lat,
            AsText(geometry) as wkt,
            AsBinary(geometry) as wkb
            FROM communities"""):
                r = {'name': row['cpname'], 'lat': row['lat'], 'lon': row['lon'], 'wkt': row['wkt']}
                ins.insert(r)

        return

        aa = get_analysis_area(self.bundle.library, geoid='CG0666000')

        path1 = '/tmp/geot1.kml'
        if os.path.exists(path1):
            os.remove(path1)
        sfs1 = TableShapefile(self.bundle, path1, 'geot1')

        path2 = '/tmp/geot2.kml'
        if os.path.exists(path2):
            os.remove(path2)

        sfs2 = TableShapefile(self.bundle, path2, 'geot2', source_srs=communities.get_srs())

        print sfs1.type, sfs2.type

        for row in communities.query("""
         SELECT *, 
         X(Transform(Centroid(geometry), 4326)) AS lon, 
         Y(Transform(Centroid(geometry), 4326)) as lat,
         AsText(geometry) as wkt,
         AsBinary(geometry) as wkb
         FROM communities"""):
            sfs1.add_feature({'name': row['cpname'], 'lat': row['lat'], 'lon': row['lon'], 'wkt': row['wkt']})
            sfs2.add_feature({'name': row['cpname'], 'lat': row['lat'], 'lon': row['lon'], 'wkt': row['wkt']})

        sfs1.close()
        sfs2.close()

    def demo2(self):
        import ambry.geo as dg
        import numpy as np
        from matplotlib import pyplot as plt

        k = dg.ConstantKernel(11)
        b = np.zeros((50, 50))

        k.apply_add(b, dg.Point(0, 0))
        k.apply_add(b, dg.Point(0, b.shape[1]))
        k.apply_add(b, dg.Point(b.shape[0], b.shape[1]))
        k.apply_add(b, dg.Point(b.shape[0], 0))

        k.apply_add(b, dg.Point(45, 0))  # for a specific bug

        for i in range(-5, 55):
            for j in range(-5, 55):
                k.apply_add(b, dg.Point(i, j))

        b /= np.max(b)

        print "Done, Rendering"

        img = plt.imshow(b, interpolation='nearest')
        img.set_cmap('gist_heat')
        plt.colorbar()
        plt.show()

    def demo3(self):
        import ambry.library as dl
        import ambry.geo as dg
        from matplotlib import pyplot as plt
        import numpy as np

        l = dl.get_library()
        aa = dg.get_analysis_area(l, geoid='CG0666000')
        r = l.find(dl.QueryCommand().identity(id='a2z2HM').partition(table='incidents', space=aa.geoid)).pop()
        p = l.get(r.partition).partition
        a = aa.new_array()
        k = dg.ConstantKernel(9)

        print aa

        k.apply_add(a, dg.Point(400, 1919))
        k.apply_add(a, dg.Point(400, 1920))
        k.apply_add(a, dg.Point(400, 1921))
        k.apply_add(a, dg.Point(400, 1922))
        k.apply_add(a, dg.Point(400, 1923))

        for row in p.query("select date, time, cellx, celly from incidents"):
            p = dg.Point(row['cellx'], row['celly'])
            k.apply_add(a, p)

        a /= np.max(a)
        print np.sum(a)

        img = plt.imshow(a, interpolation='nearest')
        img.set_cmap('spectral_r')
        plt.colorbar()
        plt.show()


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()