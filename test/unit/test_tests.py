# -*- coding: utf-8 -*-
# Tests of the test structure
# FIXME: What exactly that tests are testing? Ask Eric.

from test.test_base import ConfigDatabaseTestBase


class Test(ConfigDatabaseTestBase):

    def setUp(self):
        super(self.__class__, self).setUp()
        self.import_bundles()

    def test_1(self):
        l = self.library()

        for bundle in l.bundles:
            print bundle.identity.name

    def test_2(self):
        l = self.library()

        for bundle in l.bundles:
            print bundle.identity.name
