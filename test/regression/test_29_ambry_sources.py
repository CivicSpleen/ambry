# -*- coding: utf-8 -*-
from __future__ import print_function

from test.test_base import TestBase

class BundleWarehouse(TestBase):

    def test_bundle_warehouse(self):

        l = self.library()

        b = l.bundle('build.example.com-casters')

        wh = b.warehouse('test')

        wh.clean()

        print(wh.dsn)

        print(wh.materialize('build.example.com-casters-simple'))


