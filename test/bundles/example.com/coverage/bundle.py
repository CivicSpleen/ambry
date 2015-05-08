'''

'''

from ambry.bundle.loader import CsvBundle


class Bundle(CsvBundle):
    ''' '''

    def __init__(self, directory=None):
        super(Bundle, self).__init__(directory)

    def build(self):
        import random

        super(Bundle, self).build()

        gvids = "0O0F0b 0O0F0B 0O0F0D".split()  # Counties in Oregon
        years = range(1995, 1999)

        p = self.partitions.find_or_new(table="counties")
        p.clean()

        with p.inserter() as ins:
            for gvid in gvids:
                for year in years:
                    ins.insert(dict(
                        gvid=gvid,
                        year=year,
                        float=random.random() * 100
                    ))

        states = "WA AZ UT".split()

        source = self.partitions.find(table='random1')

        for state in states:
            for year in years:

                p = self.partitions.find_or_new(table='states', time=year,
                                                space=state)
                p.clean()

                with p.inserter() as ins:
                    for row in source.query("SELECT * FROM random1 LIMIT 10"):
                        ins.insert(row)

        return True
