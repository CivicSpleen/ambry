"""
"""

from ambry.bundle import Bundle

class Bundle(Bundle):
    """ """

    def build(self):
        import uuid
        import random
        from datetime import date
        from geoid import civick

        table = 'example'

        categorical = ['red', 'blue', 'green', 'yellow', 'black']
        year = range(2000,2003)

        counties = [1,2]
        tracts = range(1,7)
        bgs = range(1, 7)

        p = self.partitions.new_partition(table=table)
        p.clean()

        rc = random.choice


        with p.inserter() as ins:
            
            self.log("Building: {}ls -la".format(p.identity.name))
            
            for i in range(10000):
                row = dict()

                row['uuid'] = str(uuid.uuid4())
                row['int'] = random.randint(0, 100)
                row['float'] = random.random() * 100
                row['categorical'] = rc(categorical)
                row['ordinal'] = random.randint(0, 10)
                row['gaussian'] = random.gauss(100,15)
                row['triangle'] = random.triangular(500,1500,1000)
                row['exponential'] = random.expovariate(.001)
                row['year'] = rc(year)
                row['date'] = date(rc(year), random.randint(1,12), random.randint(1,28))
                row['bg_gvid'] = str(civick.Blockgroup(1,rc(counties),rc(tracts),rc(bgs)))
                ins.insert(row)


        return True