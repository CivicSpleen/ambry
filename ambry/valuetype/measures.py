"""


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from core import *

class MeasureMixin(object):
    pass


class IntMeasure(IntValue, MeasureMixin):
    role = ROLE.MEASURE
    vt_code = 'measure/int'

class LongMeasure(LongValue, MeasureMixin):
    role = ROLE.MEASURE
    vt_code = 'mmeasure/long'

class FloatMeasure(FloatValue, MeasureMixin):
    role = ROLE.MEASURE
    vt_code = 'measure'


class ZScoreVT(FloatMeasure):
    """A Z score"""
    desc = 'Z Score'
    vt_code = 'z'

class ArealDensityVT(FloatMeasure):
    """A general areal density"""
    desc = 'Density'
    vt_code = 'density'


class CountVT(IntMeasure):
    vt_code = 'count'
    desc = 'Count'
    def __init__(self,v):
        pass

class RatioVT(FloatMeasure):
    """A general ratio, with values that may exceed 1"""
    vt_code = 'ratio'
    desc = 'Ratio'

class ProportionVT(FloatMeasure):
    """A general ratio of two other values, from 0 to 1"""
    vt_code = 'pro'
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
    vt_code = 'rate'
    desc = 'Rate, 0->1'

class PercentageVT(FloatMeasure):
    """Percentage, expressed as 0 to 100. """
    vt_code = 'pct'
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





measure_value_types = {
    "measure": FloatMeasure,
    "measure/int": IntMeasure,
    "measure/long": LongMeasure,
    "density": ArealDensityVT,
    "count": CountVT,
    "ratio": RatioVT,
    "rate": RateVT,
    "proportion": ProportionVT,
    "pct": PercentageVT,
    "percent": PercentageVT,
    "z": ZScoreVT,
}