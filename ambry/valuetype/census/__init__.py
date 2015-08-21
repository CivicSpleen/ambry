"""Value Types for Census codes, primarily geoids.

The value converters can recognize, parse, normalize and transform common codes, such as FIPS, ANSI and census codes.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from .. import ValueType

import geoid

class Geoid(ValueType):
    """Two letter state Abbreviation. May be uppercase or lower case. """

    __datatype__ = str
    parser = None

    def intuit_name(self, name):
        """Return a numeric value in the range [-1,1), indicating the likelyhood that the name is for a valuable of
        of this type. -1 indicates a strong non-match, 1 indicates a strong match, and 0 indicates uncertainty. """

        if name == 'geoid':
            return 1.
        else:
            return 0

    def intuit_value(self, v):
        """Return true if the input value could be a value of this type. """
        return NotImplemented

    def parse(self, v):
        """Parse a value of this type and return a list of parsed values"""

        self._parsed = self.parser(v)
        return self

    def __getattr__(self, item):

        try:
            return getattr(self._parsed, item)
        except KeyError:
            return object.__getattribute__(item)

class AcsGeoid(Geoid):
    parser = geoid.acs.AcsGeoid.parse

class CensusGeoid(Geoid):
    parser = geoid.census.CensusGeoid.parse

class TigerGeoid(Geoid):
    parser = geoid.tiger.TigerGeoid.parse
