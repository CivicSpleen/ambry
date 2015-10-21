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

        num_cycle = cycle([1, 2, 3, 4, '*'])

        for i in range(6000):
            row = OrderedDict()

            row['uuid'] = str(uuid.uuid4())
            row['index'] = i
            row['index2'] = i*2
            row['numcom'] = locale.format("%d", i, grouping=True)
            row['indexd3'] = float(i) / 3.0
            row['categorical'] = next(cat_cycle)
            row['removecodes'] = next(num_cycle)
            row['keepcodes'] = row['removecodes']
            row['date'] = date(2000, i % 12 + 1, i % 28 + 1)

            if i == 0:
                yield list(row.keys())

            yield list(row.values())


def cst_init(v):
    return 1

def caster_everything(v, i_s, header_s, i_d, header_d, row, errors, scratch, pipe):
    return v+1

def caster_all(v, i_s, header_s, row, errors, pipe):
    return v+1

def caster_v(v):
    return v+1

def caster_vih(v, i_s, header_s):
    return v+1

def caster_vrep(v, row, errors, pipe):
    return v+1

def cst_double(v):
    return v*2 if v is not None  else None

def cst_exception(v, i_d, header_d, row, errors, pipe, exception):
    print "CST_EXCEPTION ",i_d, header_d, exception
    errors[header_d] = v
    return None


def cst_reraise_value(exception):
    raise ValueError(exception.value)


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
        except:
            return -1



    @staticmethod
    def doubleit1(v):
        return int(v) * 2

    @staticmethod
    def doubleit2(pipe, row, v):
        return int(v) * 2

    def doubleit3(self, row, v):
        return int(v) * 2

    def recode(self, v):
        from ambry.valuetype.types import IntOrCode

        try:
            return int(v)
        except:
            return None
