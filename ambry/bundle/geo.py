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

        for table, item in self.config.build.sources.items():

            with self.session:
                copy_schema(self.schema, table_name=table, path=item.url, logger=log)

        self.schema.write_schema()

        return True


    def build(self):

        for table, item in self.metadata.sources.items():
            self.log("Loading table {} from {}".format(table, item.url))
            p = self.partitions.new_geo_partition(table=table, shape_file=item.url)
            self.log("Loading table {}. Done".format(table))

        for p in self.partitions:
            print p.info

        return True