'''

'''

from  ambry.bundle import BuildBundle

# The CodeCastErrorHandler catches all conversion errors and turns them into
# _code field entries. So, if there is a '(3)' as a flag in the wages column ( an integer),
# it gets stored in the varchar wages_code column.
from ambry.database.inserter import CodeCastErrorHandler


class Bundle(BuildBundle):
    ''' '''

    def __init__(self,directory=None):

        super(Bundle, self).__init__(directory)

    def gen_rows(self,  as_dict=False, prefix_headers = ['id']):
        import uuid
        import random
        from collections import OrderedDict
        import numpy as np

        colors = 'red blue green yellow orange'.split()
        codes = 'a b c d e f'.split()

        norm = np.random.normal(500, 100, 10000)
        rayleigh = np.random.rayleigh(300,10000)
        poisson = np.random.poisson(300, 10000)
        pareto = np.random.pareto(2.0, 20000)
        pareto = pareto[pareto < 8 ] # Tuncate so distribution histograms are interesting.

        for i in range(10000):
            row = OrderedDict()

            row['int'] = random.randint(0, 1000)
            row['intwcode'] = random.randint(0, 1000)

            if random.randint(0, 50) == 0:
                row['intwcode'] = random.choice(codes)

            row['uuid'] = str(uuid.uuid4())
            row['intcode'] = random.randint(0, 50)
            row['color'] = random.choice(colors)

            row['normal'] = norm[i]
            row['rayleigh'] = rayleigh[i%len(rayleigh)]
            row['pareto'] = pareto[i]

            if as_dict:

                yield dict(row.items())
            else:

                yield prefix_headers + row.keys(), [None]*len(prefix_headers)+row.values()

    def meta(self):
        from ambry.dbexceptions import NotFoundError

        self.prepare()
        table_name = 'stats'

        with self.session as s:

            try:
                t = self.schema.table(table_name)
            except NotFoundError:
                t = self.schema.add_table(table_name, add_id = True)

            header, row = self.gen_rows(as_dict=False).next()

            self.schema.update_from_iterator(table_name,
                                             header=header,
                                             iterator=self.gen_rows(as_dict=False),
                                             max_n=5000,
                                             logger=self.init_log_rate(500))

        self.schema.write_schema()

        return True

    def build(self):

        p = self.partitions.find_or_new(table='stats')
        p.clean()

        lr = self.init_log_rate()

        with p.inserter(cast_error_handler = CodeCastErrorHandler) as ins:
            for row in self.gen_rows(as_dict=True):

                lr(str(p.identity.name))

                e = ins.insert(row)

                if e:
                    pass #print "Insert Error", e

        return True

    def test(self):

        p = self.partitions.all.pop(0)
        s = p.stats

        assert s.color.count == 10000
        assert s.color.nuniques == 5



