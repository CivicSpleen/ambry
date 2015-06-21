
from test.test_base import TestBase

class Test(TestBase):



    def test_library(self):
        from ambry.library import new_library
        rc = self.get_rc()

        l = new_library(rc)

        b = l.new_bundle(**self.ds_params(1))

        b.clean()

        from fs.opener import fsopendir
        from test import bundlefiles
        from os.path import dirname
        source_fs = fsopendir(dirname(bundlefiles.__file__))

        b.builder.sync(source_fs) # Loads the files from directory

        b.sync() # This will sync the files back to the bundle's source dir

        b.prepare()

        self.dump_database('config', l._db)


def suite():
    import unittest
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

