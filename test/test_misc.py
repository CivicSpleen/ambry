

from bundles.testbundle.bundle import Bundle
from test_base import  TestBase


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
        import os, csv
        from ambry.util.intuit import Intuiter

        csvf = os.path.join(os.path.dirname(__file__), 'support', 'types.csv' )

        intuit = Intuiter()

        with open(csvf) as f:
            intuit.iterate(csv.DictReader(f))

        intuit.dump()

        print '--------'


        with open(csvf) as f:
            r = csv.reader(f)
            intuit = Intuiter(header = r.next())
            intuit.iterate(r, max_n = 30)

        intuit.dump()