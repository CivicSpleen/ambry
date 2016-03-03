"""


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from core import *


class MeasureVT(ValueType):
    role = ROLE.MEASURE
    vt_code = 'm'

    def __init__(self,v):
        pass


class CountVT(ValueType):
    role = ROLE.MEASURE
    vt_code = 'm/count'

    def __init__(self,v):
        pass


class ProportionVT(ValueType):
    role = ROLE.MEASURE
    vt_code = 'm/pro'

    def __init__(self,v):
        pass


class PercentageVT(ValueType):
    role = ROLE.MEASURE
    vt_code = 'm/pct'

    def __init__(self,v):
        pass


class IntervalVT(ValueType):
    role = ROLE.MEASURE
    vt_code = 'm/I'

    def __init__(self,v):
        pass


class RatiometricVT(ValueType):
    role = ROLE.MEASURE
    vt_code = 'm/R'

    def __init__(self,v):
        pass

