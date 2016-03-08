"""


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from core import *

class ErrorVT(ValueType):
    role = ROLE.ERROR
    vt_code = 'e'

    def __init__(self,v):
        pass

class MarginOfErrorVT(FloatValue):
    role = ROLE.ERROR
    vt_code = 'e/m'

    def __init__(self,v):
        pass

    @property
    def se(self):

        if '/90' in self.vt_code:
            return self / 1.645
        elif '/95' in self.vt_code:
            return self / 1.96
        elif '/99' in self.vt_code:
            return self / 2.575
        else:
            raise ValueError("Can't understand value_args: {}".format(self.type_args))

    @property
    def m90(self):
        return self.se * 1.645

    @property
    def m95(self):
        return self.se * 1.96

    @property
    def m99(self):
        return self.se * 2.575

    def ci90u(self, v):
        """Return the 90% confidence interval upper value"""
        return v + self.m90

    def ci90l(self, v):
        """Return the 90% confidence interval lower value"""
        return v - self.m90

    @property
    def m95(self):
        pass

    def ci90u(self, v):
        """Return the 95% confidence interval upper value"""
        return v + self.m90

    def ci90l(self, v):
        """Return the 95% confidence interval lower value"""
        return v - self.m90

    def ci95u(self, v):
        """Return the 95% confidence interval upper value"""
        return v + self.m95

    def ci95l(self, v):
        """Return the 95% confidence interval lower value"""
        return v - self.m95


    def rse(self, denom):
        """Return the relative standard error, relative to the given value"""
        return self.se / denom


class ConfidenceIntervalHalfVT(FloatValue):
    """An upper or lower half of a confidence interval"""
    role = ROLE.ERROR
    vt_code = 'e/ci'

    def __init__(self,v):
        pass

    @property
    def se(self):
        pass


class StandardErrorVT(IntValue):
    """A standard error"""
    role = ROLE.ERROR
    vt_code = 'e/se'

    def __init__(self,v):
        pass

    @property
    def m90(self):
        o =  MarginOfErrorVT(self * 1.645)
        o.vt_code += '/90'
        return o

    @property
    def m95(self):
        o = MarginOfErrorVT(self * 1.96)
        o.vt_code += '/95'
        return o

    @property
    def m99(self):
        o = MarginOfErrorVT(self * 2.575)
        o.vt_code += '/99'
        return o

    def ci90u(self, v):
        """Return the 95% confidence interval upper value"""
        return v + self.m90

    def ci90l(self, v):
        """Return the 95% confidence interval lower value"""
        return v - self.m90

    def ci95u(self, v):
        """Return the 95% confidence interval upper value"""
        return v + self.m95

    def ci95l(self, v):
        """Return the 95% confidence interval lower value"""
        return v - self.m95


class RelativeStandardErrorVT(ValueType):
    role = ROLE.ERROR
    vt_code = 'e/rse'

    def __init__(self,v):
        pass

error_value_types = {
    "e": ErrorVT,
    "e/m": MarginOfErrorVT,
    "e/ci": ConfidenceIntervalHalfVT,
    "e/se": StandardErrorVT,
    "e/rse": RelativeStandardErrorVT,
}