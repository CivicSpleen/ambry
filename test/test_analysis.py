"""
Created on Jun 30, 2012

@author: eric
"""

import unittest
from test_base import  TestBase

import logging
import ambry.util



class Test(TestBase):
 
    def setUp(self):
      pass

    def tearDown(self):
        pass


    def testBasic(self):
        from ambry.bundle import new_analysis_bundle

        ab = new_analysis_bundle(source='foo.com', dataset='dataseter',
                                 subset='subset', variation='test', revision=2,
                                 ns_key='fe78d179-8e61-4cc5-ba7b-263d8d3602b9')

        print "Bundle Dir", ab.bundle_dir

        a = ab.metadata.about

        a.title = 'This is an Example Analysis Bundle?'
        a.tags = ['example','another']
        a.groups = ['examples']

        ab.metadata.dependencies['random'] =  'example.com-random-example1'

        p = ab.library.dep('random').partition

        df =  p.select("SELECT * FROM example1",index_col='id').pandas

        gt90 =  df[df.int > 90]

        print gt90.head(10)

        out = ab.partitions.new_db_from_pandas(gt90,table = 'gt90')

        ab.post_build()

        print p._repr_html_()


    def test_attachment(self):
        from ambry.bundle import new_analysis_bundle

        ab = new_analysis_bundle(source='foo.com', dataset='crime',
                                 subset='attach', variation='test', revision=2,
                                 ns_key='fe78d179-8e61-4cc5-ba7b-263d8d3602b9')

        print "Bundle Dir", ab.bundle_dir

        a = ab.metadata.about

        a.title = 'This is an Example Analysis Bundle?'
        a.tags = ['example', 'another']
        a.groups = ['examples']

        ab.metadata.dependencies['incidents'] = 'clarinova.com-crime-incidents-casnd-incidents'
        ab.metadata.dependencies['addresses'] = 'clarinova.com-crime-incidents-casnd-addresses'


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
        l.find(name='place')

        b = l.get('example.com-random')
        print b.partitions.info


        b = l.get('example.com-random-example1')
        print b.partition.table.info

        p = b.partition
        print p.select("SELECT uuid FROM example1").pandas

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