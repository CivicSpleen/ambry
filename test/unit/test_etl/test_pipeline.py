# -*- coding: utf-8 -*-

import unittest

try:
    # py2, mock is external lib.
    from mock import Mock
except ImportError:
    # py3, mock is included
    from unittest.mock import Mock

from ambry.etl.pipeline import DatabaseRelationSourcePipe
from ambry.bundle import Bundle


class DatabaseRelationPipeTest(unittest.TestCase):

    # .__str__ test
    def test_returns_string_representation(self):
        fake_bundle = Mock(spec=Bundle)
        fake_source = Mock()
        fake_source.name = 'Test'
        pipe = DatabaseRelationSourcePipe(fake_source, fake_bundle)
        self.assertEqual(str(pipe), 'DatabaseRelation ambry.etl.pipeline.DatabaseRelationSourcePipe')
