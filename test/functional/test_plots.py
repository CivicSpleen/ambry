# -*- coding: utf-8 -*-

from ambry.orm.column import Column

from test.proto import TestBase


class Test(TestBase):

    def test_basic(self):


        l = self.library()

        b = l.bundle('build.example.com-plot')

        p = b.partition(table='plotdata')

        from ambry.orm import Plot

