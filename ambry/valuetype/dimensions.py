"""


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from core import *
import re


class KeyVT(IntValue):
    role = ROLE.KEY
    vt_code = 'k'

class IdentifierVT(IntValue):
    role = ROLE.IDENTIFIER
    vt_code = 'i'

class DimensionVT(TextValue):
    role = ROLE.DIMENSION
    vt_code = 'd'

class RaceEthVT(IntValue):
    role = ROLE.DIMENSION
    vt_code = 'd/raceth'
    low = LOM.NOMINAL

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

class RaceEthCodeHCI(IntValue):
    role = ROLE.DIMENSION
    vt_code = 'd/raceth/hci/code'
    low = LOM.ORDINAL

    hci_map = {e[3].lower() if e[3] else None: e for e in RaceEthVT.re_codes}

    @property
    def civick(self):
        return self.hci_map[self.lower()][1]

class RaceEthNameHCI(TextValue):
    role = ROLE.DIMENSION
    vt_code = 'd/raceth/hci/name'
    low = LOM.NOMINAL

    hci_map = {e[3].lower() if e[3] else None: e for e in RaceEthVT.re_codes}

    @property
    def civick(self):
        return self.hci_map[self.lower()][1]

class RaceEthCen00VT(RaceEthVT):
    role = ROLE.DIMENSION
    vt_code = 'd/raceth/cen00'

class RaceEthCen10VT(RaceEthVT):
    role = ROLE.DIMENSION
    vt_code = 'd/raceth/cen10'

class RaceEthOmbVT(RaceEthVT):
    role = ROLE.DIMENSION
    vt_code = 'd/raceth/omb'

class RaceEthReidVT(TextValue):
    role = ROLE.DIMENSION
    vt_code = 'd/raceth/reid'
    low = LOM.NOMINAL

class RaceEthNameVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/raceth/name'
    low = LOM.NOMINAL

class AgeVT(IntValue):
    """A single-year age"""
    role = ROLE.DIMENSION
    vt_code = 'd/age'
    low = LOM.ORDINAL

class AgeRangeVT(TextValue):
    """An age range, between two years. The range is half-open. """
    role = ROLE.DIMENSION
    vt_code = 'd/age/range'
    low = LOM.ORDINAL

    # Standard age ranges
    ranges = [
        (0,6),
        (6,18),
        (0,18),
        (0,21),
        (18,35),
        (18,65),
        (35,65),
        (65,85),
        (65,100),
        (85,100)
    ]

    def __init__(self, v):
        parts = v.split('-')
        self.from_year, self.to_year = int(parts[0]), int(parts[1])

class AgeRangeCensus(TextValue):
    """Age ranges that appear in census column titles"""
    role = ROLE.DIMENSION
    vt_code = 'd/age/range/census'
    low = LOM.ORDINAL

    under = re.compile('[Uu]nder (?P<to>\d+)')
    over = re.compile('(?P<from>\d+) years and over')
    to = re.compile('(?P<from>\d+) to (?P<to>\d+) years')
    _and = re.compile('(?P<from>\d+) and (?P<and2>\d+) years')
    one = re.compile('(?P<one>\d+) years')

    from_year = None
    to_year = None

    def parse_age_group(self,v):
        for rx in (self.under, self.over, self.to, self._and, self.one):
            m = rx.search(v)
            if m:
                d = m.groupdict()

                if rx == self.under:
                    d['from'] = 0
                elif rx == self.over:
                    d['to'] = 120
                elif rx == self._and:
                    d['to'] = int(d['and2']) + 1
                    del d['and2']
                elif rx == self.to:
                    d['to'] = int(d['to']) + 1
                elif rx == self.one:
                    d['from'] = d['one']
                    d['to'] = d['one']
                    del d['one']

                return int(d['from']), int(d['to'])

        return None, None

    def __init__(self, v):
        self.from_year, self.to_year = self.parse_age_group(v)

    def __str__(self):
        return "{:02d}-{:02d}".format(self.from_year, self.to_year)

class DecileVT(IntValue):
    """A Decile Ranking, from 1 to 10"""
    role = ROLE.DIMENSION
    vt_code = 'd'
    desc = "Decile ranking"
    low = LOM.ORDINAL

dimension_value_types = {
    "d/raceth": RaceEthVT,
    "d/raceth/hci/name": RaceEthNameHCI,
    "d/raceth/hci/code": RaceEthCodeHCI,
    "d/raceth/cen00": RaceEthCen00VT,
    "d/raceth/cen10": RaceEthCen10VT,
    "d/raceth/omb": RaceEthOmbVT,
    "d/raceth/reid": RaceEthReidVT,
    "d/raceth/name": RaceEthNameVT,
    "d/age": AgeVT,
    "d/age/range": AgeRangeVT,
    "d/decile": DecileVT
}
