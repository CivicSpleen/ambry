"""Value Types for Census codes, primarily geoids.

The value converters can recognize, parse, normalize and transform common codes, such as FIPS, ANSI and census codes.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""
import re

import geoid.census
import geoid.acs
import geoid.civick
import geoid.tiger

import six

from .. import TextValue, ValueType


class Geoid(ValueType):
    """Two letter state Abbreviation. May be uppercase or lower case. """

    _pythontype = str
    parser = None

    geoid = None

    def __init__(self, v):
        from geoid import Geoid

        if isinstance(v, Geoid):
            self.geoid = v
        else:
            try:
                self.geoid = self.parser(v)
            except ValueError:
                self.geoid = self.parser('invalid')

    @classmethod
    def parse(cls,  v):
        """Parse a value of this type and return a list of parsed values"""

        if not isinstance(v, six.string_types):
            raise ValueError('Value must be a string')

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
        except AttributeError:
            return object.__getattribute__(self, item)

    def render(self):
        return str(self.geoid)

    @property
    def acs(self):
        return self.geoid.convert(geoid.acs.AcsGeoid)

    @property
    def gvid(self):
        return self.geoid.convert(geoid.civick.GVid)

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

class CensusStateGeoid(Geoid):
    @classmethod
    def parser(cls, v):
        """Ensure that the upstream parser gets two digits. """
        return geoid.census.State.parse(str(v).zfill(2))

class CensusCountyGeoid(Geoid):
    parser = geoid.census.County.parse

class CensusPlaceGeoid(Geoid):
    parser = geoid.census.Place.parse

class CensusBlockgroupGeoid(Geoid):
    parser = geoid.census.Blockgroup.parse

class CensusTractGeoid(Geoid):
    parser = geoid.census.Tract.parse

class TigerGeoid(Geoid):
    parser = geoid.tiger.TigerGeoid.parse


class GVid(Geoid):
    parser = geoid.civick.GVid.parse


class CountyName(TextValue):
    """A Census county name"""

    _pythontype = str

    # Strip the county and state name. THis doesn't work for some locations
    # where the county is actually called a parish or a bario.

    state_name_pattern = r', (.*)$'
    state_name_re = re.compile(state_name_pattern)

    def __init__(self, name):
        self.name = name

    def intuit_name(self, name):
        """Return a numeric value in the range [-1,1), indicating the likelyhood that the name is for a valuable of
        of this type. -1 indicates a strong non-match, 1 indicates a strong match, and 0 indicates uncertainty. """

        raise NotImplementedError

    @property
    def state(self):
        try:
            county, state = self.name.split(',')
            return state
        except ValueError:
            # The search will fail for 'District of Columbia'
            return ''

    @property
    def medium_name(self):
        """The census name without the state"""
        return self.state_name_re.sub('', self.name)

    type_names = (
        'County', 'Municipio', 'Parish', 'Census Area', 'Borough',
        'Municipality', 'city', 'City and Borough')
    type_name_pattern = '|'.join('({})'.format(e) for e in type_names)
    type_names_re = re.compile(type_name_pattern)

    @property
    def division_name(self):
        """The type designation for the county or county equivalent, such as 'County','Parish' or 'Borough'"""
        try:
            return next(e for e in self.type_names_re.search(self.name).groups() if e is not None)
        except AttributeError:
            # The search will fail for 'District of Columbia'
            return ''

    county_name_pattern = r'(.+) {}, (.+)'.format(type_name_pattern)
    county_name_re = re.compile(county_name_pattern)

    @property
    def short_name(self):
        try:
            county, state = self.name.split(',')
        except ValueError:
            return self.name  # 'District of Colombia'

        return self.type_names_re.sub('', county)
