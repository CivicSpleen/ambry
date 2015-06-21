

from test.test_base import TestBase

from ambry.bundle import Bundle

class Test(TestBase):


    def test_statemachine_states(self):
        from time import sleep

        db = self.new_database()

        b = self.new_bundle()

        b.state = 'first'
        b.state = 'second'

        db.commit()

        self.assertEquals('second', b.state)
        self.assertFalse(b.error_state)

        b.set_error_state()

        self.assertTrue(bool(b.error_state))

def suite():
    import unittest
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

