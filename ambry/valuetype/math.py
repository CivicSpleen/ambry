"""Math functions available for use in derivedfrom columns


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from .math import *
from random import *

def rate(num, denom, round_digits = None):
    """
    Convenience function to divide two numbers, possibly integers.
    :param num: Numerator
    :param denom: Denominator
    :param round_digits: Number of digits to round to
    :return: Float
    """

    if round_digits:
        return round(float(num)/float(denom), round_digits)
    else:
        return float(num)/float(denom)

def percent(num, denom, round_digits = 2):
    """Convenience function to turn two numbers, possibly integers, into a percentage.
    :param num: Numerator
    :param denom: Denominator
    :param round_digits: Number of digits to round to
    :return: Float
    """
    return round(float(num)/float(denom)*100, round_digits)