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
from ambry.valuetype.dimensions import StrDimension

class FailedGeoid(FailedValue):
    def __str__(self):
        return 'invalid'


class Geoid(StrDimension):
    """General Geoid """
    desc = 'Census Geoid'
    parser = None
    geoid = None

    def __new__(cls, *args, **kwargs):

        v = args[0]

        if v is None or (isinstance(v, string_types) and v.strip() == ''):
            return None

        if isinstance(v, geoid.Geoid):
            o = StrDimension.__new__(cls, str(v))
            o.geoid = v
            return o

        try:
            o = StrDimension.__new__(cls, *args, **kwargs)
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


class GeoLabel(LabelValue):
    role = ROLE.DIMENSION
    vt_code = 'geo/label'
    desc = 'Geographic Identifier Label'


class GeoAcsVT(Geoid):
    role = ROLE.DIMENSION
    vt_code = 'geo/acs'
    desc = 'ACS Geoid'
    parser = geoid.acs.AcsGeoid.parse


class GeoTigerVT(Geoid):
    role = ROLE.DIMENSION
    vt_code = 'geo/tiger'
    desc = 'Tigerline Geoid'
    parser = geoid.tiger.TigerGeoid.parse


class GeoCensusVT(Geoid):
    role = ROLE.DIMENSION
    vt_code = 'geo/census'
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
    vt_code = 'geo/census/tract'
    desc = 'Census Tract Geoid'
    parser = geoid.census.Tract.parse

    @property
    def dotted(self):
        """Return just the tract number, excluding the state and county, in the dotted format"""
        v = str(self.geoid.tract).zfill(6)
        return v[0:4] + '.' + v[4:]


class GeoGvidVT(Geoid):
    role = ROLE.DIMENSION
    vt_code = 'geo/gvid'
    desc = 'CK Geoid'
    parser = geoid.civick.GVid.parse


class GeoZipVT(IntDimension):
    """A ZIP code"""

    desc = 'ZIP Code'
    vt_code = 'geo/usps/zip'


class GeoStusabVT(StrDimension):
    """A 2 character state abbreviation"""
    desc = 'USPS State Code'
    vt_code = 'geo/usps/state'

    def __new__(cls, v):

        if v is None:
            return NoneValue

        try:
            return str.__new__(cls, str(v).lower())
        except Exception as e:
            return FailedValue(v, e)

class FipsValue(IntDimension):
    """A FIPS Code"""
    role = ROLE.DIMENSION
    desc = 'Fips Code'
    vt_code = 'fips'

class GnisValue(IntDimension):
    """An ANSI geographic code"""
    role = ROLE.DIMENSION
    desc = 'US Geographic Names Information System  Code'
    vt_code = 'gnis'

class CensusValue(IntDimension):
    """An geographic code defined by the census"""
    role = ROLE.DIMENSION
    desc = 'Census Geographic Code'
    vt_code = 'geo/census'


class WellKnownTextValue(IntDimension):
    """Geographic shape in Well Known Text format"""
    role = ROLE.DIMENSION
    desc = 'Well Known Text'
    vt_code = 'wkt'


class DecimalDegreesValue(FloatDimension):
    """An geographic code defined by the census"""
    role = ROLE.DIMENSION
    desc = 'Geographic coordinate in decimal degrees'


geo_value_types = {
    "label/geo": GeoLabel,
    "geoid": GeoAcsVT,  # acs_geoid
    "geoid/tiger": GeoAcsVT,  # acs_geoid
    "geoid/census": GeoAcsVT,  # acs_geoid
    "gvid": GeoGvidVT,
    "fips": FipsValue,
    "fips/state": FipsValue,  # fips_state
    "fips/county": FipsValue,  # fips_
    "gnis": GnisValue,
    "census": CensusValue,  # Census specific int code, like FIPS and ANSI, but for tracts, blockgroups and blocks
    "zip": GeoZipVT,  # zip
    "zcta": GeoZipVT,  # zip
    "stusab": GeoStusabVT,  # stusab
    "lat": DecimalDegreesValue,  # Decimal degrees
    "lon": DecimalDegreesValue,  # Decimal degrees
    "wkt": WellKnownTextValue  # WKT Geometry String
}
