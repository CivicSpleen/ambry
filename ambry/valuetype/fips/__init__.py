"""Value Types for FIPS codes

The value converters can recognize, parse, normalize and transform common codes, such as FIPS, ANSI and census codes.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from .. import ValueType

class County(ValueType):
    """Up-to 3 digit integers, or three digit strings. """

    __datatype__ = str

    def intuit_name(self, name):
        """Return a numeric value in the range [-1,1), indicating the likelyhood that the name is for a valuable of
        of this type. -1 indicates a strong non-match, 1 indicates a strong match, and 0 indicates uncertainty. """

        if 'county' in name:
            return .5
        elif 'cnty' in name:
            return .4
        else:
            return 0

    def intuit_value(self, v):
        """Return true if the input value could be a value of this type. """

        try:
            return int(v) < 1000
        except:
            return False

    def fips(self):
        return self._parsed

    def fips_i(self):
        """Integer version of the fips"""
        return int(self._parsed)

    def geoid(self, state):
        """A county geoid"""
        from geoid.acs import County
        return str(County(int(state), int(self._parsed)))

    def tiger(self, state):
        """A county geoid, in tiger format"""
        from geoid.tiger import County
        return str(County(int(state), int(self._parsed)))

    def gvid(self, state):
        """A state geoid, in CivicKnowledge format"""
        from geoid.civick import County
        return str(County(int(state), int(self._parsed)))

    def name(self):
        """Return the name of the county"""
        raise NotImplementedError()

    def census_name(self):
        """Return the name of the county, including the state"""
        raise NotImplementedError()

    def parse(self, v):
        """Parse a value of this type and return a list of parsed values"""

        self._parsed = '{:03d}'.format(int(v))
        return self


class State(ValueType):
    """Up-to 2 digit integers """

    __datatype__ = str

    def intuit_name(self, name):
        """Return a numeric value in the range [-1,1), indicating the likelyhood that the name is for a valuable of
        of this type. -1 indicates a strong non-match, 1 indicates a strong match, and 0 indicates uncertainty. """

        if 'state' in name:
            return .5
        else:
            return 0

    def intuit_value(self, v):
        """Return true if the input value could be a value of this type. """

        try:
            return int(v) < 100
        except:
            return False

    def usps(self):
        """Return the USPS abbreviation"""
        raise NotImplemented()

    def uspsl(self):
        """Return the USPS abbreviation, lowercased"""
        return self.usps().lower()

    def fips(self):
        return self._parsed

    def fips_i(self):
        """Integer version of the fips"""
        return int(self._parsed)

    def geoid(self):
        """A state geoid"""
        from geoid.acs import State

        return str(int(self._parsed))

    def tiger(self):
        """A state geoid, in tiger format"""
        from geoid.tiger import State

        return str(int(self._parsed))

    def gvid(self):
        """A state geoid, in CivicKnowledge format"""
        from geoid.civick import State

        return str( int(self._parsed))

    def name(self):
        """Return the name of the county"""
        raise NotImplementedError()

    def census_name(self):
        """Return the name of the county, including the state"""
        raise NotImplementedError()

    def parse(self, v):
        """Parse a value of this type and return a list of parsed values"""

        self._parsed = '{:02d}'.format(int(v))
        return self