

from test.test_base import TestBase

from ambry.bundle import Bundle

class Test(TestBase):

    def get_rc(self, name='ambry.yaml'):
        from ambry.run import get_runconfig
        import os
        from test import bundlefiles

        def bf_dir(fn):
            return os.path.join(os.path.dirname(bundlefiles.__file__), fn)

        return get_runconfig(bf_dir(name))

    def test_run_config_filesystem(self):
        from ambry.run import get_runconfig
        import os
        from test import bundlefiles

        self.rc = self.get_rc()

        self.assertEquals('/tmp/cache/downloads', self.rc.filesystem('downloads'))
        self.assertEquals('/tmp/cache/extracts', self.rc.filesystem('extracts'))

    def test_run_config_library(self):

        self.rc = self.get_rc()

        print self.rc.library()

def suite():
    import unittest
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

