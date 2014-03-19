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

        ab = new_analysis_bundle(source='foo.com', dataset='dataset',
                                 subset='subset', variation='test', revision=2)

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

        # Try attaching


        ab.post_build()

        print p._repr_html_()


    def test_attachment(self):
        from ambry.bundle import new_analysis_bundle

        ab = new_analysis_bundle(source='foo.com', dataset='crime',
                                 subset='attach', variation='test', revision=2)

        print "Bundle Dir", ab.bundle_dir

        with ab.config.about as a:
            a.title = 'This is an Example Analysis Bundle?'
            a.tags = ['example', 'another']
            a.groups = ['examples']

        ab.config.build.dependencies = {
            'incidents': 'clarinova.com-crime-incidents-casnd-incidents',
            'addresses': 'clarinova.com-crime-incidents-casnd-addresses'
        }


        incidents = ab.library.dep('incidents').partition
        addresses = ab.library.dep('addresses').partition

        incidents.attach(addresses, 'addr')

        df = incidents.select("SELECT * FROM incidents "
                              "LEFT JOIN addr.addresses as addresses ON addresses.id = incidents.address_id "
                              "LIMIT 1000", index_col='id').pandas

        print df.dtypes



        ab.post_build()

    def test_library(self):

        import ambry

        l = ambry.ilibrary()

        print l.info

        iset = l.list()

        print str(iset)

        print iset._repr_html_()




    def test_find(self):
        import ambry

        l = ambry.ilibrary()

        iset = l.find(name='random')

        print str(iset)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())