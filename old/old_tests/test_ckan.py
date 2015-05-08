"""
Created on Jan 17, 2013

@author: eric
"""
import unittest
from testbundle.bundle import Bundle
from ambry.identity import *  # @UnusedWildImport
from test_base import TestBase


class Test(TestBase):
    def setUp(self):
        self.copy_or_build_bundle()

        self.bundle = Bundle()
        self.bundle_dir = self.bundle.bundle_dir

    def tearDown(self):
        pass

    def test_basic(self):
        repo = self.bundle.repository

        for extract in repo.generate_extracts():
            print extract


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()