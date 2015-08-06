"""
Created on Oct 15, 2012

@author: eric
"""
import unittest
import test_identity

suite = unittest.TestSuite()
suite.addTests(test_identity.suite())

test_loader = unittest.defaultTestLoader

test_bundle_suite = test_loader.discover('test_bundle', top_level_dir='test')
suite.addTests(test_bundle_suite)

test_library_suite = test_loader.discover('test_library', top_level_dir='test')
suite.addTests(test_library_suite)

test_orm_suite = test_loader.discover('test_orm', top_level_dir='test')
suite.addTests(test_orm_suite)

test_metadata_suite = test_loader.discover('test_metadata', top_level_dir='test')
suite.addTests(test_metadata_suite)

test_etl_suite = test_loader.discover('test_etl', top_level_dir='test')
suite.addTests(test_etl_suite)


if __name__ == '__main__':
    unittest.TextTestRunner().run(suite)
