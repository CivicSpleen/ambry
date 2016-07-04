"""


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from core import *
import six
import re
import geoid
import geoid.census
import geoid.acs
import geoid.civick
import geoid.tiger
from ambry.valuetype import TextValue, FailedValue
from ambry.valuetype.dimensions import StrDimension


class FailedGeoid(FailedValue):
    def __str__(self):
        return 'invalid'


class Geoid(StrDimension, GeoMixin):
    """General Geoid """
    desc = 'Census Geoid'
    geoid_cls = None
    geoid = None

    def __new__(cls, *args, **kwargs):
        import geoid

        v = args[0]

        if v is None or (isinstance(v, string_types) and v.strip() == ''):
            return NoneValue

        if isinstance(v, geoid.Geoid):
            o = StrDimension.__new__(cls, str(v))
            o.geoid = v
            return o

        try:

            if len(args) < 2:  # Parse a string
                _geoid = cls.geoid_cls.parse(v)
            else:  # construct from individual state, county, etc, values
                _geoid = cls.geoid_cls(*args, **kwargs)

            o = StrDimension.__new__(cls, str(_geoid))
            o.geoid = _geoid

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


class GeoLabel(LabelValue, GeoMixin):
    role = ROLE.DIMENSION
    vt_code = 'geo/label'
    desc = 'Geographic Identifier Label'


class GeoAcsVT(Geoid):
    role = ROLE.DIMENSION
    vt_code = 'geoid'
    desc = 'ACS Geoid'
    geoid_cls = geoid.acs.AcsGeoid


class CountyGeoid(GeoAcsVT):
    """An ACS Geoid for Counties """
    desc = 'County ACS geoid'
    geoid_cls = geoid.acs.County


class TractGeoid(GeoAcsVT):
    """An ACS Geoid for Counties """
    desc = 'Tract ACS geoid'
    geoid_cls = geoid.acs.Tract


class CensusTractGeoid(Geoid):
    """A Census Geoid for Counties """
    desc = 'Census Tract  geoid'
    geoid_cls = geoid.census.Tract

    @property
    def dotted(self):
        """Return just the tract number, excluding the state and county, in the dotted format"""
        v = str(self.geoid.tract).zfill(6)
        return v[0:4] + '.' + v[4:]


def county(state, county):
    return CountyGeoid(state, county)


class GeoTigerVT(Geoid):
    role = ROLE.DIMENSION
    vt_code = 'geo/tiger'
    desc = 'Tigerline Geoid'
    geoid_cls = geoid.tiger.TigerGeoid


class GeoCensusVT(Geoid):
    role = ROLE.DIMENSION
    vt_code = 'geoid/census'
    desc = 'Census Geoid'
    geoid_cls = geoid.census.CensusGeoid

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


class GeoGvidVT(Geoid):
    role = ROLE.DIMENSION
    vt_code = 'geo/gvid'
    desc = 'CK Geoid'
    geoid_cls = geoid.civick.GVid


class ZipCode(IntDimension, GeoMixin):
    """A ZIP code"""

    desc = 'ZIP Code'
    vt_code = 'zip'


class ZipCodePlusFour(StrDimension, GeoMixin):
    """A ZIP code"""

    desc = 'ZIP Code with 4 digit extension'
    vt_code = 'zipp4'

class GeoStusabVT(StrDimension, GeoMixin):
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


class Fips(IntDimension, GeoMixin):
    """A FIPS Code"""
    role = ROLE.DIMENSION
    desc = 'Fips Code'
    vt_code = 'fips'


class FipsState(IntDimension, GeoMixin):
    """A FIPS Code"""
    role = ROLE.DIMENSION
    desc = 'Fips State Code'
    vt_code = 'fips/state'

    @property
    def geoid(self):
        import geoid.census
        v = geoid.census.State(int(self))
        if not v:
            return NoneValue
        else:
            return v

class GeoInt(IntDimension, GeoMixin):
    """General integer Geo identifier"""
    role = ROLE.DIMENSION
    desc = 'General integer Geo identifier'
    vt_code = 'geo/int'


class GnisValue(IntDimension, GeoMixin):
    """An ANSI geographic code"""
    role = ROLE.DIMENSION
    desc = 'US Geographic Names Information System  Code'
    vt_code = 'gnis'


class CensusValue(IntDimension, GeoMixin):
    """An geographic code defined by the census"""
    role = ROLE.DIMENSION
    desc = 'Census Geographic Code'
    vt_code = 'geo/census'


class WellKnownTextValue(IntDimension, GeoMixin):
    """Geographic shape in Well Known Text format"""
    role = ROLE.DIMENSION
    desc = 'Well Known Text'
    vt_code = 'wkt'


class DecimalDegreesValue(FloatDimension, GeoMixin):
    """An geographic code defined by the census"""
    role = ROLE.DIMENSION
    desc = 'Geographic coordinate in decimal degrees'


geo_value_types = {
    "label/geo": GeoLabel,
    "geoid": GeoAcsVT,  # acs_geoid
    "geoid/tiger": GeoAcsVT,  # acs_geoid
    "geoid/census": GeoAcsVT,  # acs_geoid
    "geoid/county": CountyGeoid,  # acs_geoid
    "geoid/census/tract": CensusTractGeoid,
    "geoid/tract": TractGeoid,
    "gvid": GeoGvidVT,
    "fips": Fips,
    "fips/state": FipsState,  # fips_state
    "fips/county": Fips,  # fips_
    "geo/int": GeoInt,
    "gnis": GnisValue,
    "census": CensusValue,  # Census specific int code, like FIPS and ANSI, but for tracts, blockgroups and blocks
    "zip": ZipCode,  # zip
    "zipp4": ZipCodePlusFour,  # zip
    "zcta": ZipCode,  # zip
    "stusab": GeoStusabVT,  # stusab
    "lat": DecimalDegreesValue,  # Decimal degrees
    "lon": DecimalDegreesValue,  # Decimal degrees
    "wkt": WellKnownTextValue  # WKT Geometry String
}
