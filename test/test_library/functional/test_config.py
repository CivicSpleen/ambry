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
        rc = self.get_rc()
        self.assertEqual('/tmp/test/downloads', rc.filesystem('downloads'))
        self.assertEqual('/tmp/test/extracts', rc.filesystem('extracts'))

    def test_run_config_library(self):
        rc = self.get_rc()
        self.assertIn('warehouse', rc.library())
        self.assertIn('database', rc.library())
