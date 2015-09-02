"""
Created on Oct 15, 2012

@author: eric
"""
import unittest

from . import test_bundle
from . import test_file
from . import test_etl
from . import test_simple_bundle

suite = unittest.TestSuite()
suite.addTests(test_bundle.suite())
suite.addTests(test_file.suite())
suite.addTests(test_etl.suite())
suite.addTests(test_simple_bundle.suite())

if __name__ == '__main__':
    unittest.TextTestRunner().run(suite)