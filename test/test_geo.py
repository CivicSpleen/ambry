"""
Created on Aug 31, 2012

@author: eric
"""
import unittest
from  bundles.testbundle.bundle import Bundle
from ambry.run import  RunConfig
from test_base import  TestBase  # @UnresolvedImport

class Test(TestBase):

    def setUp(self):

        self.copy_or_build_bundle()

        self.bundle = Bundle()    
        self.bundle_dir = self.bundle.bundle_dir


        
    def tearDown(self):
        pass

        
    def test_wkb(self):


        from shapely.wkb import dumps, loads

        b = Bundle()
        p = b.partitions.find(table='geot2')

        for row in p.query("SELECT quote(AsBinary(GEOMETRY)) as wkb, quote(GEOMETRY) FROM geot2"):
            print row
            #g = row['GEOMETRY']
            #print g.encode('hex')
            #print type(row['GEOMETRY'])
            #pnt = loads(str(row['GEOMETRY']))

            #print pnt



        
def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())