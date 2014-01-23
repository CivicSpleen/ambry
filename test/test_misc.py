"""
Created on Aug 31, 2012

@author: eric
"""
import unittest
from test_base import  TestBase  # @UnresolvedImport

class Test(TestBase):

    def setUp(self):

        pass

    def tearDown(self):
        pass


    def test_lru(self):
        from ambry.util import lru_cache
        from time import sleep

        @lru_cache(maxsize=3)
        def f(x):
            from  random import randint

            return (x,randint(0,1000))


        o =  f(1)
        self.assertEquals(o, f(1))
        self.assertEquals(o, f(1))
        f(2)
        self.assertEquals(o, f(1))
        f(3)
        f(4)
        f(5)
        self.assertNotEquals(o, f(1))


        #
        # Verify expiration based on time.
        #
        @lru_cache(maxtime=3)
        def g(x):
            from  random import randint

            return (x, randint(0, 1000))

        o = g(1)
        sleep(2)
        self.assertEquals(o, g(1))
        sleep(4)
        self.assertNotEquals(o, g(1))


        
def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())