"""


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from core import *


class MeasureVT(FloatValue):
    role = ROLE.MEASURE
    vt_code = 'm'

    def __init__(self,v):
        pass


class CountVT(IntValue):
    role = ROLE.MEASURE
    vt_code = 'm/count'

    def __init__(self,v):
        pass


class ProportionVT(FloatValue):
    role = ROLE.MEASURE
    vt_code = 'm/pro'

    def __init__(self,v):
        pass

    @property
    def percent(self):
        return PercentageVT(self*100)


class PercentageVT(FloatValue):
    role = ROLE.MEASURE
    vt_code = 'm/pct'

    def __new__(cls, v):

        if '%' in  v:
            v = v.strip('%')

        return FloatValue.__new__(cls, v)


    def __init__(self,v):
        pass

    @property
    def rate(self):
        return ProportionVT(self / 100.0)


class IntervalVT(FloatValue):
    role = ROLE.MEASURE
    vt_code = 'm/I'

    def __init__(self,v):
        pass


class RatiometricVT(FloatValue):
    role = ROLE.MEASURE
    vt_code = 'm/R'

    def __init__(self,v):
        pass

measure_value_types = {
    "m/int": IntValue,
    "m/str": TextValue,
    "m/float": FloatValue,
    "m": MeasureVT,
    "m/count": CountVT,
    "m/pro": ProportionVT,
    "m/pct": PercentageVT,
    "m/I": IntervalVT,
    "m/R": RatiometricVT,
}