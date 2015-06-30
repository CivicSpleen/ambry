"""
Created on Oct 15, 2012

@author: eric
"""
import unittest

import test_bundle
import test_file

suite = unittest.TestSuite()
suite.addTests(test_bundle.suite())
suite.addTests(test_file.suite())

if __name__ == '__main__':
    unittest.TextTestRunner().run(suite)