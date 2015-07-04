

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

    def test_edit(self):

        b = self.new_bundle()

        fs = b.source_fs

        print list(fs.listdir())

        print b.source_files.build_file.prepare_to_edit()

        print list(fs.listdir())

        print b.source_files.build_file.path

def suite():
    import unittest
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

