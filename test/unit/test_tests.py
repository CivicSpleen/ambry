# -*- coding: utf-8 -*-
# Tests of the test structure

from test.test_base import TestBase


class Test(TestBase):

    @classmethod
    def setUpClass(cls):
        cls.import_bundles()

    def test_1(self):
        l = self.library()

        for bundle in l.bundles:
            print bundle.identity.name

    def test_2(self):
        l = self.library()

        for bundle in l.bundles:
            print bundle.identity.name
