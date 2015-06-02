"""
Created on Oct 15, 2012

@author: eric
"""
import unittest

import test_config
import test_dataset
import test_table


suite = unittest.TestSuite()
suite.addTest(test_config.suite())
suite.addTest(test_dataset.suite())
suite.addTest(test_table.suite())

if __name__ == '__main__':
    unittest.TextTestRunner().run(suite)
