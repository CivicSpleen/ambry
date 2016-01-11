# -*- coding: utf-8 -*-

import unittest

try:
    # py2, mock is external lib.
    from mock import Mock
except ImportError:
    # py3, mock is included
    from unittest.mock import Mock

from ambry.etl.pipeline import BundleSQLPipe
from ambry.bundle import Bundle


class BundleSQLPipeTest(unittest.TestCase):

    # .process_header tests
    def test_returns_unchanged_header(self):
        fake_bundle = Mock(spec=Bundle)
        fake_source = Mock()
        pipe = BundleSQLPipe(fake_bundle, fake_source)
        row = ['header1', 'header2']
        ret = pipe.process_header(row)
        self.assertEqual(ret, row)

    def test_executes_bundle_sql(self):
        fake_bundle = Mock(spec=Bundle)
        fake_source = Mock()
        pipe = BundleSQLPipe(fake_bundle, fake_source)
        pipe.process_header([])

    # .__str__ test
    def test_returns_string_representation(self):
        fake_bundle = Mock(spec=Bundle)
        fake_source = Mock()
        pipe = BundleSQLPipe(fake_bundle, fake_source)
        self.assertEqual(str(pipe), 'SQL ambry.etl.pipeline.BundleSQLPipe')
