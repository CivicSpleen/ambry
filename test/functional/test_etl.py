import unittest

from test.test_base import TestBase


def cast_str(v):
    return str(v)


def cast_int(v):
    return int(v)


def cast_float(v):
    return float(v)


class Test(TestBase):

    def setup_temp_dir(self):
        import shutil
        import os
        build_url = '/tmp/ambry-build-test'

        try:
            shutil.rmtree(build_url)
        except OSError:

            pass

        os.makedirs(build_url)

        return build_url

    def setUp(self):
        from fs.opener import fsopendir
        super(Test,self).setUp()
        self.fs = fsopendir('mem://test')

    @unittest.skip('Timing test')
    def test_csv_time(self):
        """Time writing rows with a PartitionDataFile.

        """
        from ambry.etl.partition import new_partition_data_file

        fs = self.fs

        data = []
        ncols = 30

        types = (int, float, str)

        schema = [(i, types[i % 3]) for i in range(ncols)]

        #
        # Two different mungers, one uses a loop, one unrolls the loop in a lambda

        def munger1(schema):
            """Create a function to call casters on a row. Using an eval is about 11% faster than
            using a loop """

            funcs = []

            for name, t in schema:
                funcs.append('cast_{}(row[{}])'.format(t.__name__, name))

            return eval('lambda row: [{}]'.format(','.join(funcs)))

        row_munger1 = munger1(schema)

        def row_munger2(row):
            out = []
            for name, t in schema:
                if t == str:
                    out.append(cast_str(row[name]))
                elif t == int:
                    out.append(cast_int(row[name]))
                else:
                    out.append(cast_float(row[name]))

            return out

        data.append([str(j) for j in range(ncols)])

        def randdata(t):
            import random
            if t == str:
                return '%020x' % random.randrange(16 ** 20)
            elif t == int:
                return random.randint(0, 100000)
            else:
                return random.random()

        for i in range(100):
            data.append([str(randdata(schema[i][1])) for i in range(ncols)])

        cdf = new_partition_data_file(fs, 'foo.msg')

        import time
        n = 30000
        s = time.time()
        for i in range(n):
            row = data[i % 100]
            row = row_munger1(row)
            cdf.insert(row)

        print('Munger 1', round(float(n)/(time.time() - s), 3), 'rows/s')

        s = time.time()
        for i in range(n):
            row = data[i % 100]
            row = row_munger2(row)
            cdf.insert(row)

        print('Munger 2', round(float(n) / (time.time() - s), 3), 'rows/s')

    def test_sample_head(self):
        from ambry.etl.pipeline import Pipeline, Pipe, PrintRows, Sample, Head

        class Source(Pipe):

            def __iter__(self):

                yield ['int', 'int']

                for i in range(10000):
                    yield([i, i])

        # Sample
        pl = Pipeline(
            source=Source(),
            first=Sample(est_length=10000),
            last=PrintRows(count=50)
        )

        pl.run()

        # head
        self.assertIn([7, 7], pl[PrintRows].rows)
        self.assertIn([2018, 2018], pl[PrintRows].rows)
        self.assertIn([9999, 9999], pl[PrintRows].rows)

        pl = Pipeline(
            source=Source(),
            first=Head(10),
            last=PrintRows(count=50)
        )

        pl.run()

        self.assertEquals(10, len(pl[PrintRows].rows))

    def test_select(self):
        from ambry.etl.pipeline import Pipeline, Pipe, PrintRows, SelectRows

        class Source(Pipe):
            def __iter__(self):
                yield ['a', 'b']

                for i in range(10000):
                    yield ([i, i])

        pl = Pipeline(
            source=Source(),
            first=SelectRows('row.a == 100 or row.b == 1000'),
            last=PrintRows(count=50)
        )

        pl.run()

        rows = pl[PrintRows].rows

        self.assertEqual(2, len(rows))
        self.assertEqual(100, rows[0][0])
        self.assertEqual(1000, rows[1][1])

    def test_slice(self):
        from ambry.etl.pipeline import Pipeline, Pipe, Slice, PrintRows

        self.assertEquals('lambda row: tuple(row[0:3])+tuple(row[10:13])+(row[9],)+(row[-1],)',
                          Slice.make_slicer((0, 3), (10, 13), 9, -1)[1])

        self.assertEquals('lambda row: tuple(row[0:3])+tuple(row[10:13])+(row[9],)+(row[-1],)',
                          Slice.make_slicer("0:3,10:13,9,-1")[1])

        return

        class Source(Pipe):
            def __iter__(self):

                yield ['col'+str(j) for j in range(20)]

                for i in range(10000):
                    yield [j for j in range(20)]

        # Sample
        pl = Pipeline(
            source=[Source(), Slice((0, 3), (10, 13), 9, -1)],
            last=PrintRows(count=50)
        )

        pl.run()

        self.assertEquals(
            [1, 0, 1, 2, 10, 11, 12, 9, 19],
            pl[PrintRows].rows[0])
        self.assertEquals(
            ['col0', 'col1', 'col2', 'col10', 'col11', 'col12', 'col9', 'col19'],
            pl[PrintRows].headers)

        self.assertEqual(
            [('0', '3'), ('10', '13'), 9, -1],
            Slice.parse("0:3,10:13,9,-1"))

        pl = Pipeline(
            source=[Source(), Slice("0:3,10:13,9,-1")],
            last=PrintRows(count=50)
        )

        pl.run()

        self.assertEquals([1, 0, 1, 2, 10, 11, 12, 9, 19], pl[PrintRows].rows[0])
        self.assertEquals(
            ['col0', 'col1', 'col2', 'col10', 'col11', 'col12', 'col9', 'col19'],
            pl[PrintRows].headers)

    def test_multi_source(self):
        from ambry.etl.pipeline import Pipeline, Pipe, PrintRows

        class Source(Pipe):

            def __init__(self, start):
                self.start = start

            def __iter__(self):

                for i in range(self.start, self.start+10):
                    if i == self.start:
                        yield ['int', 'int']  # header

                    yield ([self.start, i])

            def __str__(self):
                return 'Source {}'.format(self.start)

        # Sample
        pl = Pipeline(
            last=PrintRows(count=50)
        )

        print(pl)

        pl.run(source_pipes=[Source(0), Source(10), Source(20)])

        self.assertIn([0, 2], pl[PrintRows].rows)
        self.assertIn([10, 18], pl[PrintRows].rows)
        self.assertIn([20, 21], pl[PrintRows].rows)

    def test_source_file_pipe(self):
        from itertools import islice

        l = self.library()
        l.clean()

        b = self.import_single_bundle('ingest.example.com/variety')

        for s in b.sources:
            if s.start_line == 0:
                continue
            print '===', s.name, s.start_line, s.end_line
            b.ingest(sources=[s])

            sf, sp = b._iterable_source(s)

            for row in islice(sp, 10):
                print row
