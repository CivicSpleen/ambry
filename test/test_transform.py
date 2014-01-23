"""
Created on Aug 31, 2012

@author: eric
"""
import unittest
from  testbundle.bundle import Bundle
from ambry.run import  RunConfig
from test_base import  TestBase  # @UnresolvedImport

class Test(TestBase):

    def setUp(self):

        self.copy_or_build_bundle()

        self.bundle = Bundle()    
        self.bundle_dir = self.bundle.bundle_dir


        
    def tearDown(self):
        pass

    def test_transforms(self):
        from ambry.transform import  CensusTransform
           
        #        
        #all_id               INTEGER        
        #text1                TEXT       3    NONE
        #text2                TEXT       4    NONE
        #integer1             INTEGER    3    -1
        #integer2             INTEGER    4    -1
        #integer3             INTEGER    5    -1
        #float                REAL       5    -1
        
        processors = {}
        for table in self.bundle.schema.tables:
            source_cols = [c.name for c in table.columns]
            
            columns = [c for c in table.columns if c.name in source_cols  ]  
            prow = [CensusTransform(c, useIndex=True) for c in columns]
     
            processors[table.name] = prow
            
        
        
        rows = [
                #[None, 1,2,3,4,5,6],
                [101, 999,9999,9999,9999,9999,6.34],
                ['101', '999','9999','9999','9999','9999','6.34']
                ]
        
        for row in rows:
            values=[ f(row) for f in processors['all'] ]
            print values
        
        
    def test_caster(self):
        from ambry.transform import CasterTransformBuilder
        import datetime
        
        ctb = CasterTransformBuilder()
        
        ctb.append('int',int)
        ctb.append('float',float)
        ctb.append('str',str)
        
        row = ctb({'int':1,'float':2,'str':'3'})
        
        self.assertTrue(isinstance(row['int'],int))
        self.assertEquals(row['int'],1)
        self.assertTrue(isinstance(row['float'],float))
        self.assertEquals(row['float'],2.0)
        self.assertTrue(isinstance(row['str'],unicode))
        self.assertEquals(row['str'],'3')
        
        # Should be idempotent
        row = ctb(row)
        self.assertTrue(isinstance(row['int'],int))
        self.assertEquals(row['int'],1)
        self.assertTrue(isinstance(row['float'],float))
        self.assertEquals(row['float'],2.0)
        self.assertTrue(isinstance(row['str'],unicode))
        self.assertEquals(row['str'],'3')
                
        
        ctb = CasterTransformBuilder()
        
        ctb.append('date',datetime.date)
        ctb.append('time',datetime.time)
        ctb.append('datetime',datetime.datetime)        
        
        row = ctb({'int':1,'float':2,'str':'3'})
        
        self.assertIsNone(row['date'])
        self.assertIsNone(row['time'])
        self.assertIsNone(row['datetime'])
        
        row = ctb({'date':'1990-01-01','time':'10:52','datetime':'1990-01-01T12:30'})
        
        self.assertTrue(isinstance(row['date'],datetime.date))
        self.assertTrue(isinstance(row['time'],datetime.time))
        self.assertTrue(isinstance(row['datetime'],datetime.datetime))
        
        self.assertEquals(row['date'],datetime.date(1990, 1, 1))
        self.assertEquals(row['time'],datetime.time(10, 52))
        self.assertEquals(row['datetime'],datetime.datetime(1990, 1, 1, 12, 30))
        
        # Should be idempotent
        row = ctb(row)
        self.assertTrue(isinstance(row['date'],datetime.date))
        self.assertTrue(isinstance(row['time'],datetime.time))
        self.assertTrue(isinstance(row['datetime'],datetime.datetime))
        
        # Case insensitive
        row = ctb({'Date':'1990-01-01','Time':'10:52','Datetime':'1990-01-01T12:30'})

        self.assertEquals(row['date'],datetime.date(1990, 1, 1))
        self.assertEquals(row['time'],datetime.time(10, 52))
        self.assertEquals(row['datetime'],datetime.datetime(1990, 1, 1, 12, 30))
        
        
    def test_intuit(self):
        import pprint
                
        schema = self.bundle.schema
        
        
        data = [
             (1,2,3),
             (1,2.1,3),
             (1,2.1,"foobar"),
             (1,2,3)
             ]
                                
        
        memo = None
        
        for row in data:
            memo  = schema.intuit(row, memo)

        pprint.pprint(memo)
        
        memo = None
        for row in data:
            row = dict(zip(('one', 'two','three'), row))
            memo  = schema.intuit(row, memo)        
        
        pprint.pprint(memo)
        
        
def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())