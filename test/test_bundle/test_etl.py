import unittest

from test.test_base import TestBase


def cast_str(v):
    return str(v)


def cast_int(v):
    return int(v)


def cast_float(v):
    return float(v)

class Test(TestBase):

    def setUp(self):
        from fs.opener import fsopendir

        self.fs = fsopendir('mem://test')

    def test_csv(self):
        from ambry.etl.partition import new_partition_data_file

        data = []
        for i in range(6):
            data.append(['abcdefghij'[j] if i == 0 else str(j) for j in range(10)])


        for i in range(3):
            cdf = new_partition_data_file(self.fs, 'foo.csv')

            cdf.insert_header(data[0])

            # Put the data in
            for row in data[1:]:
                cdf.insert_body(row)

            cdf.close()

        print self.fs.getcontents('foo.csv')

        self.assertEqual(16, len(self.fs.getcontents('foo.csv').splitlines()))

        # Take it out
        for i, row in enumerate(cdf.rows):
            if i < len(data):
                self.assertEquals(data[i], row)

        for i, row in enumerate(cdf.dict_rows,1):
            if i < len(data):
                self.assertEquals(sorted(data[0]), sorted(row.keys()))
                self.assertEquals(sorted(data[i]), sorted(row.values()))

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

        row_munger1 =  munger1(schema)

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
            data.append([ str(randdata(schema[i][1])) for i in range(ncols)])

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

    def test_dict_caster(self):
        from ambry.etl.transform import DictTransform, NaturalInt
        import datetime

        ctb = DictTransform()

        ctb.append('int', int)
        ctb.append('float', float)
        ctb.append('str', str)

        row, errors = ctb({'int': 1, 'float': 2, 'str': '3'})

        self.assertIsInstance(row['int'], int)
        self.assertEquals(row['int'], 1)
        self.assertTrue(isinstance(row['float'], float))
        self.assertEquals(row['float'], 2.0)
        self.assertTrue(isinstance(row['str'], unicode))
        self.assertEquals(row['str'], '3')

        # Should be idempotent
        row, errors = ctb(row)
        self.assertTrue(isinstance(row['int'], int))
        self.assertEquals(row['int'], 1)
        self.assertTrue(isinstance(row['float'], float))
        self.assertEquals(row['float'], 2.0)
        self.assertTrue(isinstance(row['str'], unicode))
        self.assertEquals(row['str'], '3')

        ctb = DictTransform()

        ctb.append('date', datetime.date)
        ctb.append('time', datetime.time)
        ctb.append('datetime', datetime.datetime)

        row, errors = ctb({'int': 1, 'float': 2, 'str': '3'})

        self.assertIsNone(row['date'])
        self.assertIsNone(row['time'])
        self.assertIsNone(row['datetime'])

        row, errors = ctb({'date': '1990-01-01', 'time': '10:52', 'datetime': '1990-01-01T12:30'})

        self.assertTrue(isinstance(row['date'], datetime.date))
        self.assertTrue(isinstance(row['time'], datetime.time))
        self.assertTrue(isinstance(row['datetime'], datetime.datetime))

        self.assertEquals(row['date'], datetime.date(1990, 1, 1))
        self.assertEquals(row['time'], datetime.time(10, 52))
        self.assertEquals(row['datetime'], datetime.datetime(1990, 1, 1, 12, 30))

        # Should be idempotent
        row, errors = ctb(row)
        self.assertTrue(isinstance(row['date'], datetime.date))
        self.assertTrue(isinstance(row['time'], datetime.time))
        self.assertTrue(isinstance(row['datetime'], datetime.datetime))

        #
        # Custom caster types
        #

        class UpperCaster(str):
            def __new__(cls, v):
                return str.__new__(cls, v.upper())

        ctb = DictTransform()

        ctb.append('int', int)
        ctb.append('float', float)
        ctb.append('str', UpperCaster)
        ctb.add_type(UpperCaster)

        row, errors = ctb({'int': 1, 'float': 2, 'str': 'three'})

        self.assertEquals(row['str'], 'THREE')

        #
        # Handling Errors
        #

        ctb = DictTransform()

        ctb.append('int', int)
        ctb.append('float', float)
        ctb.append('str', str)
        ctb.append('ni1', NaturalInt)
        ctb.append('ni2', NaturalInt)

        ctb({'int': '.', 'float': 'a', 'str': '3', 'ni1': 0, 'ni2': 3})

    def test_row_caster(self):
        from ambry.etl.transform import ListTransform, NaturalInt
        import datetime

        ctb = ListTransform()

        ctb.append('int', int)
        ctb.append('float', float)
        ctb.append('str', str)

        row, errors = ctb([1,2.0,'3'])

        self.assertIsInstance(row[0], int)
        self.assertEquals(row[0], 1)
        self.assertTrue(isinstance(row[1], float))
        self.assertEquals(row[1], 2.0)
        self.assertTrue(isinstance(row[2], unicode))
        self.assertEquals(row[2], '3')

        # Should be idempotent
        row, errors = ctb(row)
        self.assertIsInstance(row[0], int)
        self.assertEquals(row[0], 1)
        self.assertTrue(isinstance(row[1], float))
        self.assertEquals(row[1], 2.0)
        self.assertTrue(isinstance(row[2], unicode))
        self.assertEquals(row[2], '3')

        ctb = ListTransform()

        ctb.append('date', datetime.date)
        ctb.append('time', datetime.time)
        ctb.append('datetime', datetime.datetime)

        row, errors = ctb(['1990-01-01','10:52','1990-01-01T12:30'])

        self.assertTrue(isinstance(row[0], datetime.date))
        self.assertTrue(isinstance(row[1], datetime.time))
        self.assertTrue(isinstance(row[2], datetime.datetime))

        self.assertEquals(row[0], datetime.date(1990, 1, 1))
        self.assertEquals(row[1], datetime.time(10, 52))
        self.assertEquals(row[2], datetime.datetime(1990, 1, 1, 12, 30))

        # Should be idempotent
        row, errors = ctb(row)
        self.assertEquals(row[0], datetime.date(1990, 1, 1))
        self.assertEquals(row[1], datetime.time(10, 52))
        self.assertEquals(row[2], datetime.datetime(1990, 1, 1, 12, 30))

        #
        # Custom caster types
        #

        class UpperCaster(str):
            def __new__(cls, v):
                return str.__new__(cls, v.upper())

        ctb = ListTransform()

        ctb.append('int', int)
        ctb.append('float', float)
        ctb.append('str', UpperCaster)
        ctb.add_type(UpperCaster)

        row, errors = ctb([1,  2,  'three'])

        self.assertEquals(row[2], 'THREE')

        #
        # Handling Errors
        #

        ctb = ListTransform()

        ctb.append('int', int)
        ctb.append('float', float)
        ctb.append('str', str)
        ctb.append('ni1', NaturalInt)
        ctb.append('ni2', NaturalInt)

        row, errors =  ctb(['.',  'a',  '3',  0,  3])



    def test_simple_download(self):

        b = self.setup_bundle('complete-load')

        b.clean()
        b.sync_in()
        b.prepare()

        for i,row in enumerate(b.source_pipe('simple_fixed')):
            print row
            if i > 3:
                break

        for i,row in enumerate(b.source_pipe('simple')):
            print row
            if i > 3:
                break

    def test_etl_pipeline_run(self):
        from ambry.etl.pipeline import MergeHeader, MangleHeader, MapHeader, Pipeline
        from ambry.etl.pipeline import PrintRows, augment_pipeline
        from ambry.etl.intuit import TypeIntuiter

        b = self.setup_bundle('complete-load')

        b.clean()
        b.sync_in()
        b.meta_source()

        pl = [
            b.source_pipe('rent97'),
            MergeHeader(),
            MangleHeader(),
            MapHeader({'gvid':'county','renter_cost_gt_30':'renter_cost'})
        ]

        last = reduce(lambda last, next: next.set_source_pipe(last), pl[1:], pl[0])

        for i, row in enumerate(last):

            if i == 0:
                self.assertEqual([u'id', u'county', u'renter_cost', u'renter_cost_gt_30_cv',
                                  u'owner_cost_gt_30_pct', u'owner_cost_gt_30_pct_cv'], row)
            elif i ==1:
                self.assertEqual(1.0, row[0])

            if i > 5:
                break

        pl = Pipeline(
            source = b.source_pipe('rent97'),
            body = [
                MergeHeader(),
                TypeIntuiter(),
                MangleHeader(),
                MapHeader({'gvid': 'county', 'renter_cost_gt_30': 'renter_cost'}),
                PrintRows
            ]
        )

        print pl

        pl.run()

        print pl

        return


        for i, row in enumerate(pl.iter()):

            if i == 0:
                self.assertEqual(['id', 'county', 'renter_cost', 'renter_cost_gt_30_cv',
                                  'owner_cost_gt_30_pct', 'owner_cost_gt_30_pct_cv'], row)
            elif i ==1:
                self.assertEqual(1.0, row[0])

            if i > 5:
                break

    def test_etl_pipeline(self):

        b = self.setup_bundle('simple')
        b.sync_in()
        b.prepare()
        print b.pipeline('build', 'simple')

        print '---'

        print b.pipeline('build2')

    @unittest.skip('This test needs a source that has a  bad header.')
    def test_mangle_header(self):

        # FIXME.

        from ambry.etl.pipeline import MangleHeader

        rows = [
            ['Header One',' ! Funky $ Chars','  Spaces ','1 foob ar'],
            [1, 2, 3, 4],
            [2, 4, 6, 8]

        ]

        for i, row in enumerate(MangleHeader(rows)):
            if i == 0:
                self.assertEqual(['header_one', '_funky_chars', 'spaces', '1_foob_ar'], row)

    def test_complete_load_build(self):
        """Build the complete-load bundle"""
        from ambry.etl import augment_pipeline, PrintRows

        b = self.setup_bundle('complete-load')

        b.sync_in()
        b = b.cast_to_build_subclass()
        self.assertEquals('new', b.state)

        b.meta()

        #print b.source_fs.getcontents('schema.csv')

        b.prepare()
        self.assertEquals('prepare_done', b.state)

        print b.table('rent')

        pl = b.pipeline('build', 'rent07')

        print str(pl)

        pl.run()


    def test_complete_load_meta(self):
        """"""

        b = self.setup_bundle('complete-load')
        b.sync_in()
        b = b.cast_to_meta_subclass()

        b.meta()
        b.sync_out()

        self.assertIn('position', b.source_fs.getcontents('source_schema.csv'))
        self.assertIn('renter_cost_gt_30',b.source_fs.getcontents('source_schema.csv'))

        #self.assertEquals(6, len(b.dataset.source_tables))

        print list(b.source_fs.listdir())
        #print b.source_fs.getcontents('source_schema.csv')

        s = b.source('rent07')

        #print s.column_map

    def alt_bundle_dirs(self, root):

        import glob, os

        build_url = os.path.join(root,'build')
        source_url = os.path.join(root,'source')

        for base in (source_url, os.path.join(build_url, 'pipeline'), build_url):
            for f in glob.glob(os.path.join(base, '*')):
                if os.path.isfile(f):
                    os.remove(f)
                else:
                    os.rmdir(f)

        return build_url, source_url

    def test_complete_load_meta_rent07(self):
        """"""
        from ambry.etl.pipeline import augment_pipeline, PrintRows

        if False:
            build_url, source_url = self.alt_bundle_dirs('/tmp/test/rent07')
        else:
            build_url = source_url = None

        b = self.setup_bundle('complete-load', build_url = build_url, source_url = source_url)
        b.sync_in()
        b = b.cast_to_meta_subclass()

        b.meta('rent07')

        # self.assertEquals(6, len(b.dataset.source_tables))

        # Check the source schema file.
        b.sync_out()
        self.assertIn('renter_cost_gt_30', b.source_fs.getcontents('source_schema.csv'))
        self.assertIn('rent,1,gvid,gvid,str,,,,,,',b.source_fs.getcontents('source_schema.csv'))
        self.assertIn('rpeople,1,size,size,float,,,,,,', b.source_fs.getcontents('source_schema.csv'))

        # Check a few random bits from the pipeline debugging output.
        print  b.build_fs.getcontents('pipeline/source-rent07.txt')

        return

        self.assertIn("| renter_cost_gt_30_cv    |     13 | <type 'float'>  |",
                      b.build_fs.getcontents('pipeline/meta-rent07.txt').splitlines())

        s = b.source('rent07')

        # The schema file should have a schema in it.

        schema_lines = b.source_fs.getcontents('schema.csv').splitlines()
        self.assertEqual(7, len(schema_lines) )
        self.assertIn('rent,4,owner_cost_gt_30_pct,,,c00000load01004,REAL,owner_cost_gt_30_pct,', schema_lines)

        import ambry

        class TestBundle(ambry.bundle.Bundle):

            def edit_pipeline(self, pl):

                from ambry.etl.pipeline import PrintRows

                pl.build_last = PrintRows(print_at='end')

        b = b.cast_to_subclass(TestBundle)
        b.build('rent')

    def test_type_intuition(self):

        from ambry.etl.intuit import TypeIntuiter
        from ambry.orm.column import Column
        import datetime, time

        b = self.setup_bundle('process', source_url='temp://') # So modtimes work properly

        b.sync_in()
        b = b.cast_to_build_subclass()
        self.assertEquals('new', b.state)
        b.prepare()
        self.assertEquals('prepare_done', b.state)

        pl = b.pipeline('source','types1').run()

        self.assertTrue(bool(pl.source))

        ti = pl[TypeIntuiter]

        ti_cols =  list(ti.columns)

        self.assertEqual('int float string time date'.split(), [ e.header for e in ti_cols] )
        self.assertEqual([int,float,str,datetime.time, datetime.date], [ e.resolved_type for e in ti_cols])

        t = b.dataset.new_table(pl.source.source.name)

        for c in pl[TypeIntuiter].columns:
            t.add_column(c.header, datatype = Column.convert_python_type(c.resolved_type))

        b.commit()

        self.assertEqual('id int float string time date'.split(), [c.name for c in b.dataset.tables[0].columns ])
        self.assertEqual([u'integer', u'integer', u'real', u'varchar', u'time', u'date'],
                         [unicode(c.datatype) for c in b.dataset.tables[0].columns])

        from ambry.orm.file import File
        b.build_source_files.file(File.BSFILE.SCHEMA).objects_to_record()

        time.sleep(2) # Give modtimes a chance to change
        self.assertEqual(6, sum( e[1] == 'rtf' for e in b.sync() ))

    def test_pipe_config(self):

        b = self.setup_bundle('simple')
        l = b._library

        import yaml

        b.sync_in()

        # Re-write the metadata to include a pipeline
        with b.source_fs.open('bundle.yaml') as f:
            config =  yaml.load(f)

        config['pipelines']['build'] = dict(
            augment=["Add({'a': lambda e,r,v: 1 }) "],
            last=["PrintRows(print_at='end')"],
            store=['SelectPartition','WriteToPartition']
        )

        config['pipelines']['source'] = [
            'MergeHeader',
            "Add({'a': lambda e,r,v: 1 }) ",
            'TypeIntuiter()',
            'MangleHeader()',
        ]

        config['pipelines']['schema'] = dict(
            augment=["Add({'a': lambda e,r,v: 1 }) "],
        )


        with b.source_fs.open('bundle.yaml', 'wb') as f:
            yaml.dump(config, f)

        b.sync_in() # force b/c not enough time for modtime to change

        b.run_phase('source')

        b.run_phase('schema')

        b.build()

        print list(b.build_fs.walkfiles())

        self.assertTrue(10001, b.build_fs.exists('/example.com/simple-0.1.3/simple.msg'))

        p = list(b.partitions)[0]
        self.assertEquals(10001, len(list(p.stream())))

        print b.build_fs.getcontents('/pipeline/build-simple.txt')

        for i, row in enumerate(p.stream()):

            if i == 0:
                self.assertEqual('a', row[-1])
            else:
                self.assertEqual(1, row[-1])
            if i > 5:
                break


    def test_edit(self):
        """Test the Edit pipe, for altering the structure of data"""
        from dateutil.parser import parse
        from ambry.etl.pipeline import PrintRows, AddDeleteExpand

        b = self.setup_bundle('dimensions')
        b.sync_in()
        b.prepare()

        pl = b.pipeline('source','dimensions')
        pl.last.append(AddDeleteExpand(
                            delete = ['time','county','state'],
                            add={ "a": lambda e,r,v: r[4], "b": lambda e,r,v: r[1]},
                            edit = {'stusab': lambda e,r,v: v.lower(), 'county_name' : lambda e,r,v: v.upper() },
                            expand = { ('x','y') : lambda e, r, v: [ parse(r[1]).hour, parse(r[1]).minute ] } ))
        pl.last.append(PrintRows)
        pl.last.prepend(PrintRows)
        # header: ['date', 'time', 'stusab', 'state', 'county', 'county_name']

        pl.run()

        # The PrintRows Pipes save the rows they print, so lets check that the before doesn't have the edited
        # row and the after does.
        row = [9, u'2002-03-21', u'la', u'MARIPOSA COUNTY, CALIFORNIA', u'43', u'09:11:40 PM', 21, 11]
        self.assertNotIn(row, pl.last[0].rows)
        self.assertIn(row, pl.last[2].rows)

        print pl.last[0]

        print pl.last[2]

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
        self.assertIn([2, 7, 7],pl[PrintRows].rows)
        self.assertIn([16, 2018, 2018], pl[PrintRows].rows)
        self.assertIn([20, 9999, 9999], pl[PrintRows].rows)

        pl = Pipeline(
            source=Source(),
            first=Head(10),
            last=PrintRows(count=50)
        )

        pl.run()

        self.assertEquals(10, len(pl[PrintRows].rows))

    def test_slice(self):
        from ambry.etl.pipeline import Pipeline, Pipe, Slice, PrintRows

        self.assertEquals('lambda row: row[0:3]+row[10:13]+[row[9]]+[row[-1]]', Slice.make_slicer((0,3),(10,13),9,-1)[1])

        self.assertEquals('lambda row: row[0:3]+row[10:13]+[row[9]]+[row[-1]]', Slice.make_slicer("0:3,10:13,9,-1")[1])

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
        self.assertEquals(['col0', 'col1', 'col2', 'col10', 'col11', 'col12', 'col9', 'col19'], pl[PrintRows].headers)

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

        self.assertIn([3, 0, 2], pl[PrintRows].rows)
        self.assertIn([19, 10, 18], pl[PrintRows].rows)
        self.assertIn([22, 20, 21], pl[PrintRows].rows)

    def test_segments(self):

        b = self.setup_bundle('segments')
        l = b._library

        b.sync_in()

        b.meta()

        b.prepare()

        b.build()

        p = list(b.partitions)[0]

        import time
        t1 = time.time()
        count = 0
        for row in p.stream(skip_header=True):
            count += 1

        print float(count)/ ( time.time() - t1)


    def test_row_gen(self):
        from ambry.etl.rowgen import source_pipe
        b = self.setup_bundle('complete-load')

        b.sync_in()

        # Just verify that we can actually run through all of the sources. Not
        # sure what the checks should be.
        for s in b.sources:
            sp = b.source_pipe(s)

            for i, row in enumerate(sp):
                if i > 5:
                    break
                i, row

    def test_generator(self):

        b = self.setup_bundle('generators')
        l = b._library

        b.sync_in()

        b.meta()

        b.run_phase('build2')

        p = list(b.partitions)[0]

        count = sum = 0
        for row in p.stream(as_dict=True):
            count += 1
            sum += row['number2']

        self.assertEquals(800,count)
        self.assertEquals(159200, sum)

        b.run_phase('build2')

        p = list(b.partitions)[0]

        count = sum = 0
        for row in p.stream(as_dict=True):
            count += 1
            sum += row['number2']

        self.assertEquals(800, count)
        self.assertEquals(159200, sum)

