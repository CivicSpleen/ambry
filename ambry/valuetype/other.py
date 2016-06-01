"""

Other Types. These are not measures or dimensions, so they don't clog up charts.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from core import *
import re


class IntOther(IntValue):
    role = ROLE.OTHER
    lom = LOM.ORDINAL
    vt_code = 'other/int'


class FloatOther(FloatValue):
    role = ROLE.OTHER
    lom = LOM.ORDINAL
    vt_code = 'other/float'


class TextOther(TextValue):
    role = ROLE.OTHER
    lom = LOM.NOMINAL
    vt_code = 'other/text'


class StrOther(StrValue):
    role = ROLE.OTHER
    lom = LOM.NOMINAL
    vt_code = 'other'

other_value_types = {
    "other/int": IntOther,
    'other/float': FloatOther,
    'other/text': TextOther,
    'other': StrOther
}
