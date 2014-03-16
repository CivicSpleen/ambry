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

        ab = new_analysis_bundle('test',
                                source='foo.com', dataset='dataset',
                                subset='subset', revision=1)

        print "Bundle Dir", ab.bundle_dir

        with ab.config.about as a:
            a.title = 'This is an Example Analysis Bundle?'
            a.tags = ['example','another']
            a.groups = ['examples']

        ab.config.build.dependencies =  {
                'random': 'example.com-random-example1'
            }

        p = ab.library.dep('random').partition

        df =  p.select("SELECT * FROM example1",index_col='id').pandas

        gt90 =  df[df.int > 90]

        print gt90.head(10)

        out = ab.partitions.new_db_from_pandas(gt90,table = 'gt90')

        ab.post_build()



def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())