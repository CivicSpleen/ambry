"""Value Types for the United States Postal Service

The value converters can recognize, parse, normalize and transform common codes, such as FIPS, ANSI and census codes.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from .. import ValueType
from ambry.util import memoize

class StateAbr(ValueType):
    """Two letter state Abbreviation. May be uppercase or lower case. """

    __datatype__ = str

    def intuit_name(self, name):
        """Return a numeric value in the range [-1,1), indicating the likelyhood that the name is for a valuable of
        of this type. -1 indicates a strong non-match, 1 indicates a strong match, and 0 indicates uncertainty. """

        if name == 'stusab':
            return 1.
        elif name == 'state':
            return .5
        else:
            return 0

    def intuit_value(self, v):
        """Return true if the input value could be a value of this type. """
        return len(v) == 2

    def parse(self, v):
        """Parse a value of this type and return a list of parsed values"""

        self._parsed = str(v).lower()
        return self

    def stusab(self):
        return self._parsed


    def state_fips(self):
        return None

    def str(self):
        """Return the two0letter abbreviation as a string"""
        return self.stusab()

    def int(self):
        """Return the FIPS code, as an integer"""
        return self.stusab()


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

    def intuit_value(self, v):
        """Return true if the input value could be a value of this type. """
        import re

        return bool(self.z5p4re.match(v))

    def parse(self, v):
        """Parse a value of this type and return a list of parsed values"""

        self._parsed =  self.z5p4re.match(str(v)).groups()

        # TODO Could return state from 3 digit prefix

    @property
    def zip5(self):
        return self._parsed[0]

    @property
    def zip5p4(self):
        return "{}-{}".format(*self._parsed)