# -*- coding: utf-8 -*-
import unittest

from test.test_base import TestBase


class Test(TestBase):

    # Order of tests is important here: basic ingests first, casters second.
    def test1_basic(self):
        b = self.import_single_bundle('ingest.example.com/basic')
        self.assertIsNotNone(b.import_tests())
        try:
            b.ingest()
        finally:
            b.clean_all()
            b.close()

    @unittest.expectedFailure
    def test2_casters(self):
        b = self.import_single_bundle('build.example.com/casters')
        self.assertIsNone(b.import_tests())
        try:
            b.ingest()
            b.source_schema()
            b.schema()
            b.build()
        finally:
            b.clean_all()
            b.close()
