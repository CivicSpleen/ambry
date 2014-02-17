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


        
        
    def test_caster(self):
        from ambry.transform import CasterTransformBuilder, NonNegativeInt, NaturalInt
        import datetime
        
        ctb = CasterTransformBuilder()
        
        ctb.append('int',int)
        ctb.append('float',float)
        ctb.append('str',str)

        row, errors =  ctb({'int':1,'float':2,'str':'3'})

        self.assertIsInstance(row['int'],int)
        self.assertEquals(row['int'],1)
        self.assertTrue(isinstance(row['float'],float))
        self.assertEquals(row['float'],2.0)
        self.assertTrue(isinstance(row['str'],unicode))
        self.assertEquals(row['str'],'3')
        
        # Should be idempotent
        row, errors = ctb(row)
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

        row, errors = ctb({'int':1,'float':2,'str':'3'})
        
        self.assertIsNone(row['date'])
        self.assertIsNone(row['time'])
        self.assertIsNone(row['datetime'])

        row, errors = ctb({'date':'1990-01-01','time':'10:52','datetime':'1990-01-01T12:30'})
        
        self.assertTrue(isinstance(row['date'],datetime.date))
        self.assertTrue(isinstance(row['time'],datetime.time))
        self.assertTrue(isinstance(row['datetime'],datetime.datetime))
        
        self.assertEquals(row['date'],datetime.date(1990, 1, 1))
        self.assertEquals(row['time'],datetime.time(10, 52))
        self.assertEquals(row['datetime'],datetime.datetime(1990, 1, 1, 12, 30))
        
        # Should be idempotent
        row, errors = ctb(row)
        self.assertTrue(isinstance(row['date'],datetime.date))
        self.assertTrue(isinstance(row['time'],datetime.time))
        self.assertTrue(isinstance(row['datetime'],datetime.datetime))
        
        # Case insensitive
        row, errors = ctb({'Date':'1990-01-01','Time':'10:52','Datetime':'1990-01-01T12:30'})

        self.assertEquals(row['date'],datetime.date(1990, 1, 1))
        self.assertEquals(row['time'],datetime.time(10, 52))
        self.assertEquals(row['datetime'],datetime.datetime(1990, 1, 1, 12, 30))


        #
        # Custom caster types
        #

        class UpperCaster(str):
            def __new__(cls, v):
                return str.__new__(cls, v.upper())

        ctb = CasterTransformBuilder()

        ctb.append('int', int)
        ctb.append('float', float)
        ctb.append('str', UpperCaster)
        ctb.add_type(UpperCaster)

        row, errors = ctb({'int': 1, 'float': 2, 'str': 'three'})

        self.assertEquals(row['str'], 'THREE')

        #
        # Handling Errors
        #


        ctb = CasterTransformBuilder()

        ctb.append('int', int)
        ctb.append('float', float)
        ctb.append('str', str)
        ctb.append('ni1', NaturalInt)
        ctb.append('ni2', NaturalInt)

        row, errors = ctb({'int': '.', 'float': 'a', 'str': '3', 'ni1': 0, 'ni2': 3 },
                          codify_cast_errors=True)

        
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