

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
