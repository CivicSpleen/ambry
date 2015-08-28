"""
Created on Oct 15, 2012

@author: eric
"""
import unittest
import test_identity

suite = unittest.TestSuite()
suite.addTests(test_identity.suite())

test_loader = unittest.defaultTestLoader

modules_to_discover = [
    'test_bundle',
    'test_library',
    'test_orm',
    'test_metadata',
    'test_etl',
    # 'test_warehouse',  # FIXME: Uncomment after documenting postgres FDW setup.
]

for mod in modules_to_discover:
    suite.addTest(test_loader.discover(mod, top_level_dir='test'))

if __name__ == '__main__':
    unittest.TextTestRunner().run(suite)
