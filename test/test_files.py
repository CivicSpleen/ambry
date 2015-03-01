'''
Created on Jun 30, 2012

@author: eric
'''
import unittest
import os.path
import logging

from  bundles.testbundle.bundle import Bundle
from ambry.run import  get_runconfig, RunConfig
from ambry.library.query import QueryCommand
import ambry.util
from test_base import  TestBase


global_logger = ambry.util.get_logger(__name__)
global_logger.setLevel(logging.DEBUG)

class Test(TestBase):
 
    def setUp(self):
        import bundles.testbundle.bundle
        from shutil import rmtree



        self.bundle_dir = os.path.dirname(bundles.testbundle.bundle.__file__)
        self.rc = get_runconfig((os.path.join(self.bundle_dir,'library-test-config.yaml'),
                                 os.path.join(self.bundle_dir,'bundle.yaml'),
                                 RunConfig.USER_ACCOUNTS)
                                 )

        self.copy_or_build_bundle()

        self.bundle = Bundle()    

        print "Deleting: {}".format(self.rc.group('filesystem').root)
        try:
            rmtree(self.rc.group('filesystem').root)
        except OSError:
            pass


    def get_library(self, name='default'):
        """Clear out the database before the test run"""
        from ambry.library import new_library

        config = self.rc.library(name)

        l = new_library(config, reset=True)

        return l


    def test_basic(self):

        l = self.get_library()

        print l.info


        f1 = l.files.new_file(merge=True, path='file1',ref='file1')
        f2 = l.files.new_file(merge=True, path='file2',ref='file2')

        f1.append_file(f2)

        l.commit()


        print f1.files
        print f2.files

        f2.remove_file(f1)

        print f1.files
        print f2.files

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())