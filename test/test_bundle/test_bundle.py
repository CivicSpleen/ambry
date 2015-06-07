

from test.test_base import TestBase

from ambry.bundle import Bundle

class Test(TestBase):

    def test_bundle_dataset_object_state(self):
        """Check that using the datsets property properly resets when in a detached object state"""
        from sqlalchemy import inspect

        db = self.new_database()

        b = Bundle(self.new_db_dataset())

        self.assertFalse(inspect(b._dataset).detached)

        db.commit()

        self.assertTrue(inspect(b._dataset).detached) # Commit() detaches
        self.assertFalse(inspect(b.dataset).detached) # Dataset property re-attaches
        self.assertFalse(inspect(b._dataset).detached) # Internal is still attached.

    def test_statemachine_states(self):
        from time import sleep

        db = self.new_database()

        b = Bundle(self.new_db_dataset())

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
        b = Bundle(self.new_db_dataset())

        b.sync(mem_fs)

    # TODO Setup a Mock to check that the b.clean() process run through all of its states
    def test_clean(self):
        from fs.opener import fsopendir

        db = self.new_database()

        b = Bundle(self.new_db_dataset())

        mem_fs = fsopendir("/tmp/foobar/")  # fsopendir("mem://")

        b.source_files(mem_fs).sync()

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



def suite():
    import unittest
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

