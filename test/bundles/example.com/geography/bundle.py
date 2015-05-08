'''
'''

from ambry.bundle.loader import GeoBuildBundle


class Bundle(GeoBuildBundle):
    ''' '''

    def build(self):
        super(Bundle, self).build()

        self.build_geometry()

        return True

    def build_geometry(self):
        boundaries = self.partitions.find(table='municipal')

        q = "SELECT *, AsText(geometry) AS wkt, X(Centroid(geometry)) as lon, " \
            "Y(Centroid(geometry)) as lat FROM municipal"

        p = self.partitions.find_or_new_geo(table='geometry')
        with p.inserter() as ins:
            for row in boundaries.query(q):
                row = dict(row)
                row['geometry'] = row['wkt']
                ins.insert(row)
        


