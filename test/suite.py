"""
Created on Oct 15, 2012

@author: eric
"""
import unittest

import test_bundle
import test_library
from test.old import test_metadata, test_warehouse, test_identity
import test_warehouse

suite = unittest.TestSuite()
suite.addTests(test_identity.suite())
suite.addTests(test_bundle.suite())
suite.addTests(test_library.suite())
suite.addTests(test_metadata.suite())
# suite.addTests(test_cli.suite()) The cli tests are broken when run from the command line.

suite.addTests(test_warehouse.suite())

if __name__ == '__main__':
    unittest.TextTestRunner().run(suite)
