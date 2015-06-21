"""
Created on Oct 15, 2012

@author: eric
"""
import unittest

import test_bundle
import test_library
import test_orm
import test_identity


suite = unittest.TestSuite()
suite.addTests(test_identity.suite())
suite.addTests(test_bundle.suite())
suite.addTests(test_library.suite())
suite.addTests(test_orm.suite())

if __name__ == '__main__':
    unittest.TextTestRunner().run(suite)
