

from test.test_base import TestBundle

from ambry.bundle import Bundle

class Test(TestBase):

    def test_bundle_dataset_object_state(self):
        """Check that using the datsets property properly resets when in a detached object state"""
        from sqlalchemy import inspect

        db = self.new_database()

        b = Bundle(self.new_db_dataset(), None, None)

        self.assertFalse(inspect(b._dataset).detached)

        db.commit()

        self.assertTrue(inspect(b._dataset).detached) # Commit() detaches
        self.assertFalse(inspect(b.dataset).detached) # Dataset property re-attaches
        self.assertFalse(inspect(b._dataset).detached) # Internal is still attached.

    def test_statemachine_states(self):
        from time import sleep

        db = self.new_database()

        b = Bundle(self.new_db_dataset(), None, None)

        sm = b.builder

        sm.state = 'first'
        sm.state = 'second'

        db.commit()

        self.assertEquals('second', sm.state)
        self.assertFalse(sm.error_state)

        sm.set_error_state()

        self.assertTrue(bool(sm.error_state))

    def test_sync(self):
        db = self.new_database()
        from fs.opener import fsopendir

        mem_fs = fsopendir("/tmp/foobar/")  # fsopendir("mem://")
        b = Bundle(self.new_db_dataset(), None, mem_fs)

        b.sync()

    # TODO Setup a Mock to check that the b.clean() process run through all of its states
    def test_clean(self):
        from fs.opener import fsopendir

        db = self.new_database()

        b = Bundle(self.new_db_dataset(), None, fsopendir("mem://"))

        b.sync()

        b.clean()

        self.dump_database('config',db)
        print '---'

        from ambry.bundle.states import StateMachine
        class SMTest(StateMachine):

            def clean(self):
                super(SMTest, self).clean()
                return False

        b._state_machine_class = SMTest

        b.clean()

        self.dump_database('config', db)


    def test_prepare(self):
        from test import bundlefiles
        from os.path import dirname
        from fs.opener import fsopendir

        db = self.new_database()

        b = Bundle(self.new_db_dataset(), None, fsopendir("mem://"))

        source_fs = fsopendir(dirname(bundlefiles.__file__))

        b.builder.sync(source_fs)  # Loads the files from directory

        b.sync()  # This will sync the files back to the bundle's source dir

        b.prepare()

        self.dump_database('tables', db)
        self.dump_database('columns', db)


def suite():
    import unittest
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

