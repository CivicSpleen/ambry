from test.old.bundles.testbundle import Bundle
from test_base import TestBase


class Test(TestBase):
    def setUp(self):
        self.copy_or_build_bundle()

        self.bundle = Bundle()
        self.bundle_dir = self.bundle.bundle_dir

    def test_misc(self):
        from ambry.cache.filesystem import FsCache

        cache = FsCache('/tmp/foocache')

        with cache.get_stream('d00l005/bundle.json') as s:
            print s.read()

    def test_intuit(self):
        import os
        import csv
        from ambry.util.intuit import Intuiter

        csvf = os.path.join(os.path.dirname(__file__), 'support', 'types.csv')

        intuit = Intuiter()

        with open(csvf) as f:
            intuit.iterate(csv.DictReader(f))

        intuit.dump()

        print '--------'

        with open(csvf) as f:
            r = csv.reader(f)
            intuit = Intuiter(header=r.next())
            intuit.iterate(r, max_n=30)

        intuit.dump()

    def test_expand_to_years(self):
        from ambry.util.datestimes import expand_to_years, compress_years

        self.assertEquals([2007], expand_to_years('2007'))
        self.assertEquals([2007], expand_to_years(2007))

        self.assertEquals([2007, 2008, 2009, 2010, 2011],
                          expand_to_years('P5Y/2011'))
        self.assertEquals([2007, 2008, 2009, 2010, 2011],
                          expand_to_years('2007/P5Y'))

        self.assertEquals([2007, 2008, 2009, 2010, 2011],
                          expand_to_years('2007/2011'))

        mixed = "2001 2006 2012 P5Y/2011 P5Y/2013 1990/1993".split()

        self.assertEquals(
            [1990, 1991, 1992, 1993, 2001, 2006, 2007, 2008, 2009, 2010, 2011,
             2012, 2013],
            expand_to_years(mixed))

        self.assertEquals('1990/2013', compress_years(mixed))

        self.assertEquals('1990/2013', compress_years(
            expand_to_years(compress_years(mixed))))


