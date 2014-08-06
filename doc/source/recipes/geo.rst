.. _recipes_geo_toplevel:

=============
Bulding
=============

Locating region that cointains a point
-------------------------------------

A common geographic task is to find the census block that a point is within.

Using a partition that has a boundaries for a census block, clarinova.com-places-casnd-blocks-geo

You can also do this within Sqlite, but I find the queries very confusing. Doing it in Python is easier to understand.

The code example is in sandiego.gov-businesses-0.0.1~d02R001

First, load all of the records into

.. sourcecode:: python

    def build_geocode(self):
        from rtree import index
        from shapely.geometry import Point
        from shapely.wkt import loads

        idx = index.Index()

        q = """
        SELECT
            blocks.geoid,
            MbrMinX(blocks.geometry) AS x_min,
            MbrMinY(blocks.geometry) AS y_min,
            MbrMaxX(blocks.geometry) AS x_max,
            MbrMaxY(blocks.geometry) AS y_max,
            AsText(blocks.geometry) AS wkt

        FROM blocks
        """

        with self.session as s:
            for i, row in enumerate(blocks.query("SELECT geoid, AsText(blocks.geometry) AS wkt")):

                idx.insert (row['geoid'], # The value that will be returned by the index
                            tuple([ float(f) for f in row[1:5]]),  # The bounding box for the shape, (x_min, y_min, x_max, y_max)
                            obj=loads(row['wkt'])) # A Shapely object, generated from WKT



Then, to get a list of geoids that contain a point:

.. sourcecode:: python

    locations = idx.intersection((lon, lat), objects = True)