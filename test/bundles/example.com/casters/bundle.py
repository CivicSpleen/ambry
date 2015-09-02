# -*- coding: utf-8 -*-
from ambry.bundle import Bundle
from ambry.etl import SourcePipe


class ExampleSourcePipe(SourcePipe):

    def __iter__(self):

        import uuid
        from datetime import date
        from collections import OrderedDict
        from itertools import cycle

        # For commas formatting
        import locale
        locale.setlocale(locale.LC_ALL, 'en_US')

        cat_cycle = cycle(['red', 'blue', 'green', 'yellow', 'black'])

        num_cycle = cycle([1, 2, 3, 4, 5, 6, '*'])

        for i in range(6000):
            row = OrderedDict()

            row['uuid'] = str(uuid.uuid4())
            row['index'] = i
            row['index2'] = i*2
            row['numcom'] = locale.format("%d", i, grouping=True)
            row['indexd3'] = float(i) / 3.0
            row['categorical'] = next(cat_cycle)
            row['codes'] = next(num_cycle)
            row['keptcodes'] = next(num_cycle)
            row['date'] = date(2000, i % 12 + 1, i % 28 + 1)

            if i == 0:
                yield list(row.keys())

            yield list(row.values())


# Casters can be in the class or in the module.


class Bundle(Bundle):
    @staticmethod
    def double(v):
        return v * 2

    @staticmethod
    def remove_comma(v):
        try:
            return int(v)
        except ValueError:
            return int(v.replace(',', ''))

    def remove_codes(self, v):
        try:
            return int(v)
        except ValueError:
            return -1
