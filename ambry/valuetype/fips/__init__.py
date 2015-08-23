"""Value Types for FIPS codes

The value converters can recognize, parse, normalize and transform common codes, such as FIPS, ANSI and census codes.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from .. import ValueType, IntValue

from ..usps import states

class County(object):
    """Up-to 3 digit integers, or three digit strings. """

    __pythontype__ = str

    def __init__(self, state, v):
        pass


    def intuit_name(self, name):
        """Return a numeric value in the range [-1,1), indicating the likelyhood that the name is for a valuable of
        of this type. -1 indicates a strong non-match, 1 indicates a strong match, and 0 indicates uncertainty. """

        if 'county' in name:
            return .5
        elif 'cnty' in name:
            return .4
        else:
            return 0


    def county_map(self):
        """
        Map the FIPS codes of state and county to the county name
        :return:
        """
        if not County._county_map:
            counties = self.library.partition('census.gov-acs-geofile-2009-geofile50-20095-50')
            states = self.library.partition('census.gov-acs-geofile-2009-geofile40-20095-40')

            state_names = {}
            for row in states.stream(as_dict=True):
                if row['component'] == '00':
                    state_names[row['state']] = row['name'].strip()

                    County._county_map = {}

            for row in counties.stream(as_dict=True):
                name = row['name'].replace(', ' + state_names[row['state']], '')
                name, last = name[:name.rindex(' ')], name[name.rindex(' '):]
                County._county_map[(row['state'], row['county'])] = (name.strip(), last.strip())


        return County._county_map

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


class State(IntValue):
    """Up-to 2 digit integers """

    _fips_map = None
    _abr_map = None
    _name_map = None

    @classmethod
    def parse(cls, v):

        v = int(v)

        if isinstance(v, int):
            if not v in cls.abr_map():
                raise ValueError("Integer '{}' is not a valid FIPS state code".format(v))

        return v

    def intuit_name(self, name):
        """Return a numeric value in the range [-1,1), indicating the likelyhood that the name is for a valuable of
        of this type. -1 indicates a strong non-match, 1 indicates a strong match, and 0 indicates uncertainty. """

        if 'state' in name:
            return .5
        else:
            return 0

    @classmethod
    def fips_map(cls):
        if not cls._fips_map:
            cls._fips_map = {e[1]: e[2] for e in states}

        return cls._fips_map

    @classmethod
    def abr_map(cls):
        if not cls._abr_map:
            cls._abr_map = {e[2]: e[1] for e in states}

        return cls._abr_map

    @classmethod
    def name_map(cls):
        if not cls._name_map:
            cls._name_map = {e[2]: e[0] for e in states}

        return cls._name_map

    @property
    def usps(self):
        """Return the USPS abbreviation"""
        from ..usps import StateAbr
        return StateAbr(State.abr_map()[self])

    @property
    def str(self):
        """String version of the fips"""
        return '{:02d}'.format(self)

    @property
    def geoid(self):
        """A state geoid"""
        from geoid.acs import State
        return State(self)

    @property
    def tiger(self):
        """A state geoid, in tiger format"""
        from geoid.tiger import State
        return State(self)

        return str(int(self._parsed))

    @property
    def gvid(self):
        """A state geoid, in CivicKnowledge format"""
        from geoid.civick import State
        return State(self)

        return str( int(self._parsed))

    @property
    def name(self):
        """Return the name of the county"""
        return State.name_map()[self]

    def census_name(self):
        """Return the name of the county, including the state"""
        return State.name_map()[self]
