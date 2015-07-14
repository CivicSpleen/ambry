"""
"""

from ambry.bundle import Bundle

class Bundle(Bundle):
    """ """

    def build(self):

        from ambry.etl.pipeline import Pipe

        class RandomSourcePipe(Pipe):

            def __init__(self, j, space):
                self.j = j
                self.space = space

            def __iter__(self):

                import uuid
                import random
                from datetime import date
                from geoid import civick
                from collections import OrderedDict

                categorical = ['red', 'blue', 'green', 'yellow', 'black']
                year = range(2000,2003)
                states = range(2)
                counties = range(1,4)
                tracts = range(1,6)
                bgs = range(1, 6)

                rc = random.choice

                for i in range(6000):
                    row = OrderedDict()

                    row['uuid'] = str(uuid.uuid4())
                    row['int'] = random.randint(0, 100)
                    row['float'] = random.random() * 100
                    row['categorical'] = rc(categorical)
                    row['ordinal'] = random.randint(0, 10)
                    row['gaussian'] = random.gauss(100,15)
                    row['triangle'] = random.triangular(500,1500,1000)
                    row['exponential'] = random.expovariate(.001)
                    row['year'] = rc(year)-self.j
                    row['date'] = date(rc(year)+self.j, random.randint(1,12), random.randint(1,28))

                    row['bg_gvid'] = str(civick.Blockgroup(rc(states),rc(counties),rc(tracts),rc(bgs)))

                    if i == 0:
                        yield row.keys()

                    yield row.values()

        for j, space in enumerate(['nv', 'ut', 'ca']):
            p = self.partitions.new_partition(table='example', space=space, time=2010 + j)
            p.clean()

            p.run(source=RandomSourcePipe(j,space))

        return True
