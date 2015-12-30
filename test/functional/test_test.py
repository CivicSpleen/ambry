"""

Test the test setup code.

"""
import os

import yaml

from ambry.run import get_runconfig

from test import bundlefiles
from test.test_base import TestBase

import os
os.environ['AMBRY_TEST_DB'] = 'sqlite'

class Test(TestBase):
    def test_basic(self):

        l = self.library()

        print l.database.dsn

        l = self.library()

        print l.database.dsn