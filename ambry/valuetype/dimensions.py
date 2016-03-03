"""


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from core import *


class KeyVT(IntValue):
    role = ROLE.DIMENSION
    vt_code = 'k'

    def __init__(self, v):
        pass


class IdentifierVT(IntValue):
    role = ROLE.DIMENSION
    vt_code = 'i'

    def __init__(self, v):
        pass


class DimensionVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd'

    def __init__(self, v):
        pass


class NominalVT(IntValue):
    role = ROLE.DIMENSION
    vt_code = 'd/N'

    def __init__(self, v):
        pass


class CategoricalVT(TextValue):
    role = ROLE.DIMENSION
    vt_code = 'd/C'

    def __init__(self, v):
        pass


class RaceEthVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/raceth'

    # Civick Numeric, Civick, Census, HCI, Description
    re_codes = [
        [1, u'aian', u'C', u'AIAN', u'American Indian and Alaska Native Alone'],
        [2, u'asian', u'D', u'Asian', u'Asian Alone'],
        [3, u'black', u'B', u'AfricanAm', u'Black or African American Alone'],
        [4, u'hisp', u'I', u'Latino', u'Hispanic or Latino'],
        [5, u'nhopi', u'E', u'NHOPI', u'Native Hawaiian and Other Pacific Islander Alone'],
        [6, u'white', u'A', u'White', u'White alone'],
        [61, u'whitenh', u'H', None, u'White Alone, Not Hispanic or Latino'],
        [7, u'multiple', None, u'Multiple', u'Multiple'],
        [8, u'other', None, u'Other', u'Some Other Race Alone'],
        [9, u'all', None, u'Total', u'Total Population']]

    def __init__(self, v):
        pass

class RaceEthHCI(TextValue):
    role = ROLE.DIMENSION
    vt_code = 'd/raceth/hci'

    hci_map = {e[3].lower(): e for e in RaceEthVT.re_codes}

    def __init__(self, v):
        pass

    @property
    def civick(self):
        return self.hci_map[self.lower()][1]


class RaceEthCen00VT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/raceth/cen00'

    def __init__(self, v):
        pass


class RaceEthCen10VT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/raceth/cen10'

    def __init__(self, v):
        pass


class RaceEthOmbVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/raceth/omb'

    def __init__(self, v):
        pass


class RaceEthReidVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/raceth/reid'

    def __init__(self, v):
        pass


class RaceEthNameVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/raceth/name'

    def __init__(self, v):
        pass


class AgeVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/age'

    def __init__(self, v):
        pass


class AgeYearsVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/age/years'

    def __init__(self, v):
        pass


class AgeRangeVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/age/range'

    def __init__(self, v):
        pass


dimension_value_types = {
    "d/raceth": RaceEthVT,
    "d/raceth/hci": RaceEthHCI,
    "d/raceth/cen00": RaceEthCen00VT,
    "d/raceth/cen10": RaceEthCen10VT,
    "d/raceth/omb": RaceEthOmbVT,
    "d/raceth/reid": RaceEthReidVT,
    "d/raceth/name": RaceEthNameVT,
    "d/age": AgeVT,
    "d/age/years": AgeYearsVT,
    "d/age/range": AgeRangeVT}
