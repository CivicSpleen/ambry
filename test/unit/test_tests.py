# -*- coding: utf-8 -*-
# Tests of the test structure
# FIXME: What exactly that tests are testing? Ask Eric.

import pytest

from test.test_base import TestBase


class Test(TestBase):

    def setUp(self):
        super(self.__class__, self).setUp()
        self.import_bundles()

    @pytest.mark.slow
    def test_1(self):
        l = self.library()

        for bundle in l.bundles:
            print bundle.identity.name

    @pytest.mark.slow
    def test_2(self):
        l = self.library()

        for bundle in l.bundles:
            print bundle.identity.name
