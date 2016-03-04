"""


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from core import *


class GeoVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/geo'


class GeoAcsVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/acs'


class GeoTigerVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/tiger'


class GeoCensusVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/census'


class GeoGvidVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/gvid'


class GeoFipsVT(IntValue):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/fips'


class GeoFipsStateVT(GeoFipsVT):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/fips/state'


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
    "d/geo/fips": GeoFipsVT,
    "d/geo/fips/state": GeoFipsVT,
    "d/geo/name": GeoNameVT,
    "d/geo/usps/zip": GeoZipVT,
    "d/geo/usps/state": GeoStusabVT,
}