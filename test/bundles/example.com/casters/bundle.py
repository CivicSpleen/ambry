# -*- coding: utf-8 -*-
from ambry.bundle import Bundle
from ambry.etl import DatafileSourcePipe


class ExampleSourcePipe(DatafileSourcePipe):

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


def caster_everything(v, i_s, header_s, i_d, header_d, row, errors, scratch, pipe):
    return v

def caster_all(v, i, header, row, exceptions, pipe):
    return v

def caster_v(v):
    return v

def caster_vih(v, i, header):
    return v

def caster_vrep(v, row, exceptions, pipe):
    return v

def cst_nullify(v):
    return v

def cst_initialize(v):
    return v

def cst_typecast(v):
    return v

def cst_transform(v):
    return v

def cst_exception(v, i, header, row, exceptions, pipe):
    return v

class Bundle(Bundle):

    @staticmethod
    def doubleit(v):
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

    @staticmethod
    def doubleit1(v):
        return int(v) * 2

    @staticmethod
    def doubleit2(pipe, row, v):
        return int(v) * 2

    def doubleit3(self, row, v):
        return int(v) * 2

