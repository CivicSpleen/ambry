"""Value Types for Census codes, primarily geoids.

The value converters can recognize, parse, normalize and transform common codes, such as FIPS, ANSI and census codes.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from .. import StrValue

import geoid.census, geoid.acs, geoid.civick, geoid.tiger

from sqlalchemy.engine import RowProxy

class Geoid(object):
    """Two letter state Abbreviation. May be uppercase or lower case. """

    __pythontype = str
    parser = None

    geoid = None

    def __init__(self, v):
        self.geoid = self.parser(v)

    @classmethod
    def parse(cls,  v):
        """Parse a value of this type and return a list of parsed values"""

        if not isinstance(v, basestring):
            raise ValueError("Value must be a string")

        return

    def intuit_name(self, name):
        """Return a numeric value in the range [-1,1), indicating the likelyhood that the name is for a valuable of
        of this type. -1 indicates a strong non-match, 1 indicates a strong match, and 0 indicates uncertainty. """

        if name == 'geoid':
            return 1.
        else:
            return 0

    @property
    def state(self):
        from ..fips import State
        return State(self.geoid.state)

    def __getattr__(self, item):

        try:
            return getattr(self.geoid, item)
        except KeyError:
            return object.__getattribute__(item)

    def render(self):
        return str(self.geoid)

    @property
    def acs(self):
        return self.geoid.convert(geoid.acs.AcsGeoid)

    @property
    def gvid(self):
        return self.geoid.convert(geoid.civick.Gvid)

    @property
    def census(self):
        return self.geoid.convert(geoid.census.CensusGeoid)

    @property
    def tiger(self):
        return self.geoid.convert(geoid.tiger.TigerGeoid)


class AcsGeoid(Geoid):
    parser = geoid.acs.AcsGeoid.parse

class CensusGeoid(Geoid):
    parser = geoid.census.CensusGeoid.parse

class TigerGeoid(Geoid):
    parser = geoid.tiger.TigerGeoid.parse

class GVid(Geoid):
    parser = geoid.civick.GVid.parse
