"""
Created on Jun 30, 2012

@author: eric
"""

import unittest
import os.path
from test_base import  TestBase
from  testbundle.bundle import Bundle
from sqlalchemy import * #@UnusedWildImport
from ambry.run import  get_runconfig
from ambry.run import  RunConfig

from ambry.source.repository import new_repository

import logging
import ambry.util


logger = ambry.util.get_logger(__name__)
logger.setLevel(logging.DEBUG) 

class Test(TestBase):
 
    def setUp(self):
      pass

    def tearDown(self):
        pass


    def testBasic(self):
        from ambry.bundle import new_analysis_bundle

        ab = new_analysis_bundle('test', 'foo.com', 'dataset',  subset='subset', bspace=None, btime=None,
                                variation=None, revision=1)

        print ab

        print ab.register


        @ab.register.prepare
        def prepare(bundle):
            pass




def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())