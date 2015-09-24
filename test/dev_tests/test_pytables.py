# -*- coding: utf-8 -*-
import os
import unittest

from contexttimer import Timer
import tables

class Test(unittest.TestCase):
    @unittest.skip("Development Test")
    def test_pytables(self):
        import datetime
        from random import randint, random
        from uuid import uuid4

        N = 50000

        # Basic read/ write tests.

        epoch = datetime.date(1970, 1, 1)

        def rand_date():
            return (datetime.date(randint(2000, 2015), randint(1, 12), 10) - epoch).total_seconds()

        row = lambda: (0, 1, random(), str(uuid4()), rand_date(), rand_date())

        headers = list('abcdefghi')[:len(row())]

        rows = [row() for i in range(N)]

        class PYT(tables.IsDescription):
            a = tables.Int32Col()
            b = tables.UInt8Col()
            c = tables.Float32Col()
            d = tables.StringCol(len(str(uuid4()))*4)
            e = tables.Time32Col()
            f = tables.Time32Col()

        h5file = tables.open_file("/tmp/hdf5/tutorial1.h5", mode="w", title="Test file")

        group = h5file.create_group("/", 'detector', 'Detector information')

        table = h5file.create_table(group, 'readout', PYT, "Readout example",
                                    filters = tables.Filters(complevel=9, complib='zlib'))

        tr = table.row

        with Timer() as t:
            cache = []
            for i, row in enumerate(rows,1):
                for i,h in enumerate(headers):
                    tr[h] = row[i]

                tr.append()
            table.flush()
            h5file.close()

        print "PyTables write ", float(N) / t.elapsed, N

        h5file = tables.open_file("/tmp/hdf5/tutorial1.h5", mode="r", title="Test file")

        table = h5file.root.detector.readout

        with Timer() as t:
            count = 0
            for row in table:
                count += row['c']

        print "PyTables read  ", float(N) / t.elapsed, N

        h5file.close()


    def test_datafile_read_write(self):
        from ambry_sources.mpf import MPRowsFile
        from fs.opener import fsopendir
        import datetime
        from random import randint, random
        from uuid import uuid4

        fs = fsopendir('temp://') # fs = fsopendir('/tmp/hdf5/')

        # fs = fsopendir('/tmp/pmpf')

        N = 50000

        # Basic read/ write tests.

        row = lambda: [None, 1, random(), str(uuid4()),
                       datetime.date(randint(2000, 2015), randint(1, 12), 10),
                       datetime.date(randint(2000, 2015), randint(1, 12), 10)]
        headers = list('abcdefghi')[:len(row())]

        rows = [row() for i in range(N)]

        with Timer() as t:
            df = MPRowsFile(fs, 'foobar')
            w = df.writer

            w.headers = headers

            w.meta['source']['url'] = 'blah blah'

            for i in range(N):
                w.insert_row(rows[i])

            w.close()

        print "MSGPack write ", float(N) / t.elapsed, w.n_rows

        with Timer() as t:
            count = 0
            i = 0
            s = 0

            r = df.reader

            for i, row in enumerate(r):
                count += 1

            r.close()

        print "MSGPack read  ", float(N) / t.elapsed, i, count, s

        with Timer() as t:
            count = 0

            r = df.reader

            for row in r.rows:
                count += 1

            r.close()

        print "MSGPack rows  ", float(N) / t.elapsed

        with Timer() as t:
            count = 0

            r = df.reader

            for row in r.raw:
                count += 1

            r.close()

        print "MSGPack raw   ", float(N) / t.elapsed



