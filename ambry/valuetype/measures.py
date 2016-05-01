"""


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from core import *


class MeasureVT(FloatValue):
    role = ROLE.MEASURE
    vt_code = 'm'



class ZScoreVT(FloatValue):
    """A Z score"""
    desc = 'Z Score'
    role = ROLE.MEASURE
    vt_code = 'm/z'

class ArealDensityVT(FloatValue):
    """A general areal density"""
    role = ROLE.MEASURE
    desc = 'Areal Density'
    vt_code = 'm/den'


class CountVT(IntValue):
    role = ROLE.MEASURE
    vt_code = 'm/count'
    desc = 'Count'
    def __init__(self,v):
        pass

class RatioVT(FloatValue):
    """A general ratio, with values that may exceed 1"""
    role = ROLE.MEASURE
    vt_code = 'm/ratio'
    desc = 'Ratio'

class ProportionVT(RatioVT):
    """A general ratio of two other values, from 0 to 1"""
    role = ROLE.MEASURE
    vt_code = 'm/pro'
    desc = 'Proportion'

    def __new__(cls, v):

        o = RatioVT.__new__(cls, v)

        if bool(o) and float(o) > 1:
            return FailedValue(v, ValueError("Proportion values must be less than 1"))

        return o

    @property
    def rate(self):
        return self

    @property
    def percent(self):
        return PercentageVT(self*100)

class RateVT(ProportionVT):
    """A general ratio of two other values, from 0 to 1"""
    role = ROLE.MEASURE
    vt_code = 'm/rate'
    desc = 'Rate, 0->1'

class PercentageVT(FloatValue):
    """Percentage, expressed as 0 to 100. """
    role = ROLE.MEASURE
    vt_code = 'm/pct'
    desc = 'Percentage 0->100'

    def __new__(cls, v):

        if isinstance(v, text_type) and '%' in v:
            v = v.strip('%')

        return FloatValue.__new__(cls, v)

    def init(self):
        pass

    @property
    def rate(self):
        return ProportionVT(self / 100.0)

    @property
    def percent(self):
        return self

class PercentileVT(FloatValue):
    """Percentile ranking, 0 to 100 """
    role = ROLE.MEASURE
    vt_code = 'm/pctl'
    desc = 'Percentile Rank'

    def __new__(cls, v):

        if isinstance(v,text_type) and '%' in v:
            v = v.strip('%')

        return FloatValue.__new__(cls, v)


    @property
    def rate(self):
        return ProportionVT(self / 100.0)



measure_value_types = {
    "m/int": IntValue,
    "m/str": TextValue,
    "m/float": FloatValue,
    "m": MeasureVT,
    "m/z": ZScoreVT,
    "m/den": ArealDensityVT,
    "m/count": CountVT,
    "m/ratio": RatioVT,
    "m/rate": RateVT,
    "m/pro": ProportionVT,
    "m/pct": PercentageVT,
    "m/pctl": PercentileVT,
    "m/geo/wkt": StrValue # WKT Geometry String
}