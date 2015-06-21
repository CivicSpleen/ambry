
from test.test_base import TestBase

class Test(TestBase):



    def test_library(self):
        from ambry.library import new_library
        from fs.opener import fsopendir
        from test import bundlefiles
        from os.path import dirname

        source_fs = fsopendir(dirname(bundlefiles.__file__))

        rc = self.get_rc()

        l = new_library(rc)

        b = l.new_bundle(**self.ds_params(1))

        # TODO. DOesn't actually test anything yet.


def suite():
    import unittest
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

