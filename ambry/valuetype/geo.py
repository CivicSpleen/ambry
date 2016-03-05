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


from ambry.valuetype import TextValue, ValueType, FailedValue


class FailedGeoid(FailedValue):

    def __str__(self):
        return 'invalid'

class Geoid(StrValue):
    """General Geoid """

    _pythontype = str
    parser = None
    geoid = None

    def __new__(cls, *args, **kwargs):

        v = args[0]

        if v is None or (isinstance(v, string_types) and v.strip() == ''):
            return None

        try:
            o = StrValue.__new__(cls, *args, **kwargs)
            o.geoid = cls.parser(args[0])
            return o
        except ValueError:
            return FailedValue(args[0])


    def __init__(self, v):
        pass


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

class GeoVT(StrValue):
    role = ROLE.DIMENSION
    vt_code = 'd/geo'


class GeoAcsVT(Geoid):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/acs'
    parser = geoid.acs.AcsGeoid.parse

class GeoTigerVT(Geoid):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/tiger'
    parser = geoid.tiger.TigerGeoid.parse


class GeoCensusVT(Geoid):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/census'
    parser = geoid.census.CensusGeoid.parse

class GeoGvidVT(Geoid):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/gvid'
    parser = geoid.census.CensusGeoid.parse


class GeoNameVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/name'

class GeoZipVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/usps/zip'


class GeoStusabVT(TextValue):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/usps/state'

geo_value_types = {
    "d/geo": GeoVT,
    "d/geo/acs": GeoAcsVT,
    "d/geo/tiger": GeoTigerVT,
    "d/geo/census": GeoCensusVT,
    "d/geo/gvid": GeoGvidVT,
    "d/geo/fips": GeoCensusVT,
    "d/geo/fips/state": GeoCensusVT,
    "d/geo/fips/county": GeoCensusVT,
    "d/geo/name": GeoNameVT,
    "d/geo/usps/zip": GeoZipVT,
    "d/geo/usps/state": GeoStusabVT,
}