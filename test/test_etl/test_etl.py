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

            return eval("lambda row: [{}]".format(','.join(funcs)))

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
            row = data[i%100]
            row = row_munger1(row)
            cdf.insert(row)

        print "Munger 1", round(float(n)/(time.time() - s),3), 'rows/s'

        s = time.time()
        for i in range(n):
            row = data[i % 100]
            row = row_munger2(row)
            cdf.insert(row)

        print "Munger 2", round(float(n) / (time.time() - s), 3), 'rows/s'


    def test_pipe_config(self):

        b = self.setup_bundle('simple')
        l = b._library

        import yaml

        b.sync_in()

        # Re-write the metadata to include a pipeline
        with b.source_fs.open('bundle.yaml') as f:
            config =  yaml.load(f)

        config['pipelines']['build'] = dict(
            body=["Add({'a': lambda e,r,v: 1 }) "],
            last=["PrintRows(print_at='end')"],
            store=['SelectPartition','WriteToPartition']
        )

        config['pipelines']['source'] = [
            'MergeHeader',
            "Add({'a': lambda e,r,v: 1 }) ",
            'MangleHeader()',
        ]

        config['pipelines']['schema'] = dict(
            augment=["Add({'a': lambda e,r,v: 1 }) "],
        )

        with b.source_fs.open('bundle.yaml', 'wb') as f:
            yaml.dump(config, f)

        b.sync_in() # force b/c not enough time for modtime to change
        b.ingest()
        b.schema()

        list(b.tables)[0].add_column('a',datatype = 'int')

        return

        b.run()
        print list(b.build_fs.walkfiles())

        self.assertTrue(b.build_fs.exists('/example.com/simple-0.1.3/simple.mpr'))

        p = list(b.partitions)[0]
        self.assertEquals(10000, sum(1 for e in iter(p)))

        print b.build_fs.getcontents('/pipeline/build-simple.txt')

        for i, row in enumerate(p):

            self.assertEqual(1, row.a)

            if i > 5:
                break

    def test_pipe_config_2(self):
        """Test that the = and - location specifiers work """
        b = self.setup_bundle('simple')
        l = b._library

        import yaml

        b.sync_in()
        b.ingest()

        # Re-write the metadata to include a pipeline
        with b.source_fs.open('bundle.yaml') as f:
            config = yaml.load(f)

        config['pipelines']['build'] = dict(
            body=["+Add({'a': lambda e,r,v: 1 }) "],
            last=["PrintRows(print_at='end')"],
        )

        config['pipelines']['source'] = dict(
            intuit =["-Add({'a': lambda e,r,v: 1 })"]
        )

        config['pipelines']['schema'] = dict(
            body=["+Add({'a': lambda e,r,v: 1 }) "],
        )

        with b.source_fs.open('bundle.yaml', 'wb') as f:
            yaml.dump(config, f)

        b.sync_in()  # force b/c not enough time for modtime to change
        b.ingest()
        b.schema()
        list(b.tables)[0].add_column('a', datatype='int')

        b.run()

        print list(b.build_fs.walkfiles())

        self.assertTrue(b.build_fs.exists('/example.com/simple-0.1.3/simple.mpr'))

        p = list(b.partitions)[0]
        self.assertEquals(10000, len(list(iter(p))))

        print b.build_fs.getcontents('/pipeline/build-simple.txt')

        for i, row in enumerate(iter(p)):

            self.assertEqual(1, row['a'])

            if i > 5:
                break

    @unittest.skip('broken')
    def test_edit(self):
        """Test the Edit pipe, for altering the structure of data"""
        from dateutil.parser import parse
        from ambry.etl.pipeline import PrintRows, AddDeleteExpand, Delete
        from collections import OrderedDict

        d = self.setup_temp_dir()

        # Need to set the source_url because this one creates a source schema
        b = self.setup_bundle('complete-ref', build_url=d, source_url=d)
        b.sync_in()
        b = b.cast_to_subclass()

        b.run_stages()

        pl = b.pipeline(source=b.source('stage1'))

        pl.last.append(AddDeleteExpand(
            delete = ['ordinal'],
            add={ "a": lambda e,r,v: r.int, "b": lambda e,r,v: r['float']},
            edit = {'categorical': lambda e,r,v: v.upper(), 'int' : lambda e,r,v: int(r.float) },
            expand = { ('x','y') : lambda e, r, v: [ 1, 2] } ) ) #  [ parse(r.time).hour, parse(r.time).minute ] } ))
        pl.last.append(PrintRows)
        pl.last.prepend(PrintRows)

        pl.run()

        # The PrintRows Pipes save the rows they print, so lets check that the before doesn't have the edited
        # row and the after does.

        print pl

        for row in  pl.last[-1].rows:
            d = OrderedDict(zip(pl.last[-1].headers, row))

            self.assertEquals(d['categorical'], d['categorical'].upper())
            self.assertNotEquals(d['a'], d['b'])
            self.assertNotEquals(d['a'], d['int'])
            self.assertEquals(d['int'], int(d['float']))
            self.assertEquals(d['b'], d['float'])
            self.assertNotIn('ordinal',d)


    def test_sample_head(self):
        from ambry.etl.pipeline import Pipeline, Pipe, PrintRows, Sample, Head

        class Source(Pipe):

            def __iter__(self):

                yield ['int','int']

                for i in range(10000):
                    yield([i,i])

        # Sample
        pl = Pipeline(
            source=Source(),
            first = Sample(est_length=10000),
            last = PrintRows(count=50)
        )

        pl.run()

        # head
        self.assertIn([7, 7],pl[PrintRows].rows)
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
            first= SelectRows("row.a == 100 or row.b == 1000"),
            last=PrintRows(count=50)
        )

        pl.run()

        rows = pl[PrintRows].rows

        self.assertEqual(2, len(rows))
        self.assertEqual(100, rows[0][0])
        self.assertEqual(1000, rows[1][1])

    def test_skip(self):
        from ambry.etl.pipeline import Pipeline, Pipe, PrintRows, Skip

        b = self.setup_bundle('casters')
        b.sync_in();  # Required to get bundle for cast_to_subclass to work.
        b = b.cast_to_subclass()

        class Source(Pipe):
            def __iter__(self):
                yield ['a', 'b']

                for i in range(10000):
                    yield ([i, i])

        for pred_str in ('skip_even','skip_even_meth', " row.a % 2 == 0"):

            # Static func
            pl = Pipeline(b,
                          source=Source(),
                          first=Skip(pred_str),
                          last=PrintRows(count=50)
                          )

            try:
                pl.run()
                rows = pl[PrintRows].rows
            except:
                print "Test error with predicate: '{}' ".format(pred_str)
                raise

            self.assertEqual(49, len(rows))
            self.assertEqual(1, rows[0][0])
            self.assertEqual(3, rows[1][0])
            self.assertEqual(5, rows[2][0])



    def test_slice(self):
        from ambry.etl.pipeline import Pipeline, Pipe, Slice, PrintRows

        self.assertEquals('lambda row: tuple(row[0:3])+tuple(row[10:13])+(row[9],)+(row[-1],)',
                          Slice.make_slicer((0,3),(10,13),9,-1)[1])

        self.assertEquals('lambda row: tuple(row[0:3])+tuple(row[10:13])+(row[9],)+(row[-1],)',
                          Slice.make_slicer("0:3,10:13,9,-1")[1])

        return

        class Source(Pipe):
            def __iter__(self):

                yield [ 'col'+str(j) for j in range(20)]

                for i in range(10000):
                    yield [ j for j in range(20) ]

        # Sample
        pl = Pipeline(
            source=[Source(), Slice((0,3),(10,13),9,-1) ],
            last=PrintRows(count=50)
        )

        pl.run()

        self.assertEquals([1, 0, 1, 2, 10, 11, 12, 9, 19], pl[PrintRows].rows[0])
        self.assertEquals(['col0', 'col1', 'col2', 'col10', 'col11', 'col12', 'col9', 'col19'], pl[PrintRows].headers)

        self.assertEqual([('0', '3'), ('10', '13'), 9, -1], Slice.parse("0:3,10:13,9,-1"))

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
                        yield ['int', 'int'] # header

                    yield ([self.start, i])

            def __str__(self):
                return 'Source {}'.format(self.start)

        # Sample
        pl = Pipeline(
            last=PrintRows(count=50)
        )

        print pl

        pl.run(source_pipes=[Source(0), Source(10), Source(20)])

        self.assertIn([0, 2], pl[PrintRows].rows)
        self.assertIn([10, 18], pl[PrintRows].rows)
        self.assertIn([20, 21], pl[PrintRows].rows)

