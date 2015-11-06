"""Value Types for the United States Postal Service

The value converters can recognize, parse, normalize and transform common codes, such as FIPS, ANSI and census codes.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import six

from .. import ValueType, StrValue

states = [('Alabama', 'AL', 1), ('Alaska', 'AK', 2), ('Arizona', 'AZ', 4), ('Arkansas', 'AR', 5),
          ('California', 'CA', 6), ('Colorado', 'CO', 8), ('Connecticut', 'CT', 9), ('Delaware', 'DE', 10),
          ('District of Columbia', 'DC', 11), ('Florida', 'FL', 12), ('Georgia', 'GA', 13), ('Hawaii', 'HI', 15),
          ('Idaho', 'ID', 16), ('Illinois', 'IL', 17), ('Indiana', 'IN', 18), ('Iowa', 'IA', 19), ('Kansas', 'KS', 20),
          ('Kentucky', 'KY', 21), ('Louisiana', 'LA', 22), ('Maine', 'ME', 23), ('Maryland', 'MD', 24),
          ('Massachusetts', 'MA', 25), ('Michigan', 'MI', 26), ('Minnesota', 'MN', 27), ('Mississippi', 'MS', 28),
          ('Missouri', 'MO', 29), ('Montana', 'MT', 30), ('Nebraska', 'NE', 31), ('Nevada', 'NV', 32),
          ('New Hampshire', 'NH', 33), ('New Jersey', 'NJ', 34), ('New Mexico', 'NM', 35), ('New York', 'NY', 36),
          ('North Carolina', 'NC', 37), ('North Dakota', 'ND', 38), ('Ohio', 'OH', 39), ('Oklahoma', 'OK', 40),
          ('Oregon', 'OR', 41), ('Pennsylvania', 'PA', 42), ('Puerto Rico', 'PR', 72), ('Rhode Island', 'RI', 44),
          ('South Carolina', 'SC', 45), ('South Dakota', 'SD', 46), ('Tennessee', 'TN', 47), ('Texas', 'TX', 48),
          ('Utah', 'UT', 49), ('Vermont', 'VT', 50), ('Virginia', 'VA', 51), ('Virgin Islands', 'VI', 78),
          ('Washington', 'WA', 53), ('West Virginia', 'WV', 54), ('Wisconsin', 'WI', 55), ('Wyoming', 'WY', 56)]


class StateAbr(StrValue):
    """Two letter state Abbreviation. May be uppercase or lower case. """

    __datatype__ = str
    _fips_map = None
    _abr_map = None
    _name_map = None

    def __new__(cls, v):
        o = super(StrValue, cls).__new__(cls, cls.parse(v))
        return o

    @classmethod
    def parse(cls,  v):
        """Parse a value of this type and return a list of parsed values"""

        if isinstance(v, int):
            v = cls.abr_map(v)

        if not isinstance(v, six.string_types):
            raise ValueError('Value must be a string')

        if len(v) != 2:
            raise ValueError('Value must be 2 char wide')

        return v

    def intuit_name(self, name):
        """Return a numeric value in the range [-1,1), indicating the likelyhood that the name is for a valuable of
        of this type. -1 indicates a strong non-match, 1 indicates a strong match, and 0 indicates uncertainty. """

        if name == 'stusab':
            return 1.
        elif name == 'state':
            return .5
        else:
            return 0

    @classmethod
    def fips_map(cls):
        if not cls._fips_map:
            cls._fips_map = {e[1]: int(e[2]) for e in states}

        return cls._fips_map

    @classmethod
    def abr_map(cls):
        if not cls._abr_map:
            cls._abr_map = {int(e[2]): e[1] for e in states}

        return cls._abr_map

    @classmethod
    def name_map(cls):
        if not cls._name_map:
            cls._name_map = {e[1]: e[0] for e in states}

        return cls._name_map

    @property
    def stusab(self):
        return self

    @property
    def name(self):
        return StateAbr.name_map()[self]

    @property
    def fips(self):
        """Convert the abbreviation to a FIPS code"""
        from ..fips import State
        return State(StateAbr.fips_map()[self])


class Zip(ValueType):
    """Zip codes, 5 digits or zip+4 """

    __datatype__ = str

    def __init__(self, bundle, library, group=None):
        super(Zip, self).__init__(bundle, library, group)
        import re

        self.z5p4re = re.compile(r'(\d\d\d\d\d)-?(\d\d\d\d)?')

    def intuit_name(self, name):
        """Return a numeric value in the range [-1,1), indicating the likelyhood that the name is for a valuable of
        of this type. -1 indicates a strong non-match, 1 indicates a strong match, and 0 indicates uncertainty. """

        if 'zip' in name.lower():
            return 1.
        else:
            return 0

    def parse(self, v):
        """Parse a value of this type and return a list of parsed values"""

        self._parsed = self.z5p4re.match(str(v)).groups()

        # TODO Could return state from 3 digit prefix

    @property
    def zip5(self):
        return self._parsed[0]

    @property
    def zip5p4(self):
        return '{}-{}'.format(*self._parsed)
