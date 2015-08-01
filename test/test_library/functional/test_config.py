# -*- coding: utf-8 -*-
import os

from ambry.run import get_runconfig
from test.test_base import TestBase
from test import bundlefiles


class Test(TestBase):

    def get_rc(self, name='ambry.yaml'):

        def bf_dir(fn):
            return os.path.join(os.path.dirname(bundlefiles.__file__), fn)

        return get_runconfig(bf_dir(name))

    def test_run_config_filesystem(self):

        self.rc = self.get_rc()

        self.assertEquals('/tmp/test/downloads', self.rc.filesystem('downloads'))
        self.assertEquals('/tmp/test/extracts', self.rc.filesystem('extracts'))

    def test_run_config_library(self):

        self.rc = self.get_rc()

        print self.rc.library()
