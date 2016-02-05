# -*- coding: utf-8 -*-
import ambry.bundle


class Bundle(ambry.bundle.Bundle):
    pass

    def get_sql(self):

        s = self.source('int100')

        (_,itrs) = self._iterable_source(s)

        for row in itrs:
            print row
