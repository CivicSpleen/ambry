"""
Created on Oct 15, 2012

@author: eric
"""
import unittest

import test_config
import test_library

suite = unittest.TestSuite()
suite.addTests(test_config.suite())
suite.addTests(test_library.suite())


if __name__ == '__main__':
    unittest.TextTestRunner().run(suite)
