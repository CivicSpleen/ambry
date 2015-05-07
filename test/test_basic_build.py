"""
Created on Jun 22, 2012

@author: eric
"""
import unittest

from bundles.testbundle.bundle import Bundle
from ambry.identity import *
from test_base import  TestBase


class Test(TestBase):
 
    def setUp(self):
        super(Test, self).setUp()
        self.bundle = Bundle()
        self.bundle_dir = self.bundle.bundle_dir

    def save_bundle(self):
        pass
        
    def restore_bundle(self):
        pass 



    def test_db_bundle(self):
        
        from ambry.bundle import BuildBundle, DbBundle
        
        b = BuildBundle(self.bundle_dir)
        b.clean()
        
        self.assertTrue(b.identity.id_ is not None)
        self.assertEquals('source-dataset-subset-variation', b.identity.sname)
        self.assertEquals('source-dataset-subset-variation-0.0.1', b.identity.vname)
        
        b.database.create()
        
        db_path =  b.database.path
        
        dbb = DbBundle(db_path)
        
        self.assertEqual("source-dataset-subset-variation", dbb.identity.sname)
        self.assertEqual("source-dataset-subset-variation-0.0.1", dbb.identity.vname)

        b = Bundle()
        b.database.enable_delete = True
        b.clean()
        b.database.create()

        b = Bundle()
        b.exit_on_fatal = False
        b.pre_prepare()
        b.prepare()
        b.post_prepare()
        b.pre_build()
        #b.build_db_inserter()
        b.build_geo()
        b.post_build()
        b.close()


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())