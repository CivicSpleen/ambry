"""Base class for testing and test support functions

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import logging
import sys

import unittest
from unittest.util import safe_repr

# A baseclass for tests that uses the events.
class BundleTest(unittest.TestCase):

    bundle = None
    library = None

    def setUp(self):
        # noinspection PyUnresolvedReferences
        from ambry.build import bundle, library # From the codes loaded in the bundles test.py file

        self.bundle = bundle()
        self.library = library()

        self.logging_handler = logging.StreamHandler(sys.stdout)
        self.bundle.logger.addHandler(self.logging_handler)

    def tearDown(self):
        self.bundle.logger.removeHandler(self.logging_handler)

    def _assertInHeaders(self, headers, member, msg=None):
        from six import string_types

        def _assert_in(member, headers):
            if member not in headers:
                standardMsg = '%s not found in %s' % (safe_repr(member),
                                                      safe_repr(headers))

                self.fail(self._formatMessage(msg, standardMsg))

        if isinstance(member, string_types):
            _assert_in(member, headers)
        else:
            members = member
            for member in members:
                _assert_in(member, headers)


    def assertInSourceHeaders(self, table_name, member, msg = None):
        """
        Fail if the member, which may be a string type or a collection, is not in the headers
        of a source table.

        :param table_name:
        :param member:
        :param msg:
        :return:
        """
        headers = [c.source_header for c in self.bundle.source_table(table_name).columns]

        return self._assertInHeaders(headers, member)


    def assertInDestHeaders(self, table_name, member, msg = None):
        """
        Fail if the member, which may be a string type or a collection, is not in the headers
        of a destination table.

        :param table:
        :param value:
        :param msg:
        :return:
        """
        headers = [c.name for c in self.bundle.table(table_name).columns]

        return self._assertInHeaders(headers, member)


