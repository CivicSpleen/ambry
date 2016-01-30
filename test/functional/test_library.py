# -*- coding: utf-8 -*-

from ambry.identity import DatasetNumber
from ambry.orm.exc import ConflictError

from test.test_base import TestBase


class Test(TestBase):

    def setUp(self):

        super(Test, self).setUp()

        # Make an array of dataset numbers, so we can refer to them with a single integer
        self.dn = [str(DatasetNumber(x, x)) for x in range(1, 10)]

    def test_context(self):
        import time

        l = self.library()

        t = time.time()

        context = l.context

        with context as l:
            l.root.config.library.test.time = t

        with context as l:
            print l.root.config.library.test.time



