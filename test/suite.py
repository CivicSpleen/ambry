"""
Created on Oct 15, 2012

@author: eric
"""
import unittest

import test_library
import test_orm
import test_identity


suite = unittest.TestSuite()
suite.addTests(test_identity.suite())
suite.addTests(test_library.suite())
suite.addTests(test_orm.suite())

test_loader = unittest.defaultTestLoader
test_bundle_suite = test_loader.discover('test_bundle', top_level_dir='test')
suite.addTests(test_bundle_suite)


if __name__ == '__main__':
    unittest.TextTestRunner().run(suite)
