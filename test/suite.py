"""
Created on Oct 15, 2012

@author: eric
"""

import unittest
import test_bundle
import test_library
import test_server
import test_filesystem
import test_identity
import test_misc
import test_source

if __name__ == '__main__':
    suite = unittest.TestSuite()

    suite.addTests(test_identity.suite())
    suite.addTests(test_bundle.suite())
    suite.addTests(test_library.suite())
    suite.addTests(test_server.suite())
    suite.addTests(test_filesystem.suite())
    suite.addTests(test_misc.suite())
    suite.addTests(test_source.suite())
    unittest.TextTestRunner().run(suite)