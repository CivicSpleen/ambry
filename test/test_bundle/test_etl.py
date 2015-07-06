
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

        self.fs = fsopendir('/tmp/test/')

    def test_csv(self):
        from ambry.bundle.etl.partition import new_partition_data_file

        data = []
        for i in range(10):
            data.append(['abcdefghij'[j] if i == 0 else str(j) for j in range(10)])

        cdf = new_partition_data_file(self.fs, 'foo.csv')

        # Put the data in
        for row in data:
            cdf.insert(row)

        # Take it out
        for i, row in enumerate(cdf.rows):
            self.assertEquals(data[i], row)

        for i, row in enumerate(cdf.dict_rows,1):
            self.assertEquals(sorted(data[0]), sorted(row.keys()))
            self.assertEquals(sorted(data[i]), sorted(row.values()))

    def munger(self, schema):
        """Create a function to call casters on a row. Using an eval is about 11% faster than
        using a loop """

        funcs = []

        for name, t in schema:
            funcs.append('cast_{}(row[{}])'.format(t.__name__, name))

        return eval("lambda row: [{}]".format(','.join(funcs)))

    #@unittest.skip('Timing test')
    def test_csv_time(self):
        """Time writing rows with a PartitionDataFile.

        """
        from ambry.bundle.etl.partition import new_partition_data_file, PartitionMsgpackDataFile

        fs = self.fs

        data = []
        ncols = 30

        types = (int, float, str)

        schema = [(i, types[i % 3]) for i in range(ncols)]

        #
        # Two different munders, one uses a loop, one unrolls the loop in a lambda
        row_munger1 =  self.munger(schema)

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

        print round(float(n)/(time.time() - s),3)

    def test_dict_caster(self):
        from ambry.bundle.etl.transform import DictTransform, NaturalInt
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
        from ambry.bundle.etl.transform import ListTransform, NaturalInt
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

    def test_source_download(self):
        """Down load all of the sources from the complete-load bundle and check the extracted files against the
        declared MD5 sums. """

        from ambry.util import md5_for_stream
        from fs.opener import fsopendir

        b = self.setup_bundle('complete-load')

        b.do_clean()
        b.do_sync()
        b.do_prepare()

        cache_fs = fsopendir('mem://')

        self.assertEquals(14, len(b.dataset.sources))

        for i, source in enumerate(b.sources):
            print i, source.name

            with source.fetch().open() as f:
                self.assertEquals(source.hash, md5_for_stream(f))

            for i, row in enumerate(source.fetch().rowgen()):
                print row
                if i > 3:
                    break

    def test_simple_download(self):

        b = self.setup_bundle('complete-load')

        b.do_clean()
        b.do_sync()
        b.do_prepare()

        for i,row in enumerate(b.source('simple_fixed').fetch().rowgen()):
            print row
            if i > 3:
                break

        return

        for i,row in enumerate(b.source('simple').fetch().rowgen()):
            print row
            if i > 3:
                break






