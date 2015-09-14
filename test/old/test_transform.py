"""
Created on Aug 31, 2012

@author: eric
"""
import unittest

from test.old.bundles.testbundle import Bundle
from test_base import TestBase  # @UnresolvedImport


class Test(TestBase):
    def setUp(self):

        self.copy_or_build_bundle()

        self.bundle = Bundle()
        self.bundle_dir = self.bundle.bundle_dir

    def tearDown(self):
        pass

    def test_intuit(self):
        import pprint

        schema = self.bundle.schema

        data = [
            (1, 2, 3),
            (1, 2.1, 3),
            (1, 2.1, "foobar"),
            (1, 2, 3)
        ]

        memo = None

        for row in data:
            memo = schema.intuit(row, memo)

        pprint.pprint(memo)

        memo = None
        for row in data:
            row = dict(list(zip(('one', 'two', 'three'), row)))
            memo = schema.intuit(row, memo)

        pprint.pprint(memo)


def suite():
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(Test))
    return test_suite


if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())