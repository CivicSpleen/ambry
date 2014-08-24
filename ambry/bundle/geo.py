"""Geo build  bundles load shapefiles into sqlite databases.

Copyright (c) 2014 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

__author__ = 'eric'

from bundle import BuildBundle

class GeoBuildBundle(BuildBundle):

    def __init__(self, bundle_dir=None):
        '''
        '''

        super(GeoBuildBundle, self).__init__()


    def meta(self):
        from ambry.geo.sfschema import copy_schema

        self.database.create()

        self._prepare_load_schema()

        def log(x):
            self.log(x)

        for table, item in self.metadata.sources.items():

            with self.session:
                copy_schema(self.schema, table_name=table, path=item.url, logger=log)

        self.schema.write_schema()

        return True


    def build(self):

        for table, item in self.metadata.sources.items():
            self.log("Loading table {} from {}".format(table, item.url))

            # Set the source SRS, if it was not set in the input file
            if self.metadata.build.get('s_srs', False):
                s_srs = self.metadata.build.s_srs
            else:
                s_srs = None

            p = self.partitions.new_geo_partition(table=table, shape_file=item.url, s_srs=s_srs)
            self.log("Loading table {}. Done".format(table))

        for p in self.partitions:
            print p.info

        return True