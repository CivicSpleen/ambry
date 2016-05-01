"""


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from core import *
import six
import re

import geoid.census
import geoid.acs
import geoid.civick
import geoid.tiger

from ambry.valuetype import TextValue, FailedValue


class FailedGeoid(FailedValue):

    def __str__(self):
        return 'invalid'

class Geoid(TextValue):
    """General Geoid """
    desc = 'Census Geoid'
    parser = None
    geoid = None

    def __new__(cls, *args, **kwargs):

        v = args[0]

        if v is None or (isinstance(v, string_types) and v.strip() == ''):
            return None

        if isinstance(v, geoid.Geoid):
            o = TextValue.__new__(cls, str(v))
            o.geoid = v
            return o

        try:
            o = TextValue.__new__(cls, *args, **kwargs)
            o.geoid = cls.parser(args[0])
            return o
        except ValueError as e:
            return FailedValue(args[0], e)

    def __getattr__(self, item):
        """Allow getting attributes from the internal geoid"""
        try:
            return getattr(self.geoid, item)
        except AttributeError:
            return object.__getattribute__(self, item)


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


class GeoVT(TextValue):
    role = ROLE.DIMENSION
    vt_code = 'd/geo'
    desc = 'Geographic Identifier'

class GeoLabelVT(TextValue):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/label'
    desc = 'Geographic Identifier Label'



class GeoAcsVT(Geoid):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/acs'
    desc = 'ACS Geoid'
    parser = geoid.acs.AcsGeoid.parse

class GeoTigerVT(Geoid):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/tiger'
    desc = 'Tigerline Geoid'
    parser = geoid.tiger.TigerGeoid.parse


class GeoCensusVT(Geoid):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/census'
    desc = 'Census Geoid'
    parser = geoid.census.CensusGeoid.parse

    @classmethod
    def subclass(cls, vt_code, vt_args):
        """Return a dynamic subclass that has the extra parameters built in"""
        from geoid import get_class
        import geoid.census

        parser = get_class(geoid.census, vt_args.strip('/')).parse

        cls = type(vt_code.replace('/', '_'), (cls,), {'vt_code': vt_code, 'parser': parser})
        globals()[cls.__name__] = cls
        assert cls.parser

        return cls

class GeoCensusTractVT(GeoCensusVT):

    vt_code = 'd/geo/census/tract'
    desc = 'Census Tract Geoid'
    parser = geoid.census.Tract.parse

    @property
    def dotted(self):
        """Return just the tract number, excluding the state and county, in the dotted format"""
        v = str(self.geoid.tract).zfill(6)
        return v[0:4]+'.'+v[4:]


class GeoGvidVT(Geoid):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/gvid'
    desc = 'CK Geoid'
    parser = geoid.civick.GVid.parse


class GeoNameVT(TextValue):
    role = ROLE.DIMENSION
    desc = 'Geographic Name'
    vt_code = 'd/geo/name'

class GeoZipVT(IntValue):
    """A ZIP code"""
    role = ROLE.DIMENSION
    desc = 'ZIP Code'
    vt_code = 'd/geo/usps/zip'


class GeoStusabVT(TextValue):
    """A 2 character state abbreviation"""
    role = ROLE.DIMENSION
    desc = 'USPS State Code'
    vt_code = 'd/geo/usps/state'

    def __new__(cls, v):

        if v is None:
            return NoneValue

        try:
            return text_type.__new__(cls, str(v).lower())
        except Exception as e:
            return FailedValue(v, e)

geo_value_types = {
    "d/geo": GeoVT,
    "d/geo/label": GeoLabelVT,
    "d/geo/geoid": GeoAcsVT,  # acs_geoid
    "d/geo/acs": GeoAcsVT, # acs_geoid
    "d/geo/tiger": GeoTigerVT, # tiger_geoid
    "d/geo/census": GeoCensusVT, # census_geoid
    'd/geo/census/tract': GeoCensusTractVT,
    "d/geo/gvid": GeoGvidVT,
    "d/geo/int": IntValue,
    "d/geo/fips": IntValue,
    "d/geo/fips/state": IntValue,  # fips_state
    "d/geo/fips/county": IntValue, #fips_
    "d/geo/ansi": IntValue,
    "d/geo/ce": IntValue, # Census specific int code, like FIPS and ANSI, but for tracts, blockgroups and blocks
    "d/geo/name": GeoNameVT,
    "d/geo/usps/zip": GeoZipVT, # zip
    "d/geo/usps/state": GeoStusabVT, # stusab
    "d/geo/lat/ddeg": FloatValue, # Decimal degrees
    "d/geo/lon/ddeg": FloatValue,  # Decimal degrees
}