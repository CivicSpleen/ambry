"""


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from core import *



class GeoVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/geo'

    def __init__(self,v):
        pass


class GeoAcsVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/acs'

    def __init__(self,v):
        pass


class GeoTigerVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/tiger'

    def __init__(self,v):
        pass


class GeoCensusVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/census'

    def __init__(self,v):
        pass


class GeoGvidVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/gvid'

    def __init__(self,v):
        pass


class GeoFipsVT(IntValue):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/fips'

    def __init__(self,v):
        pass

class GeoFipsStateVT(GeoFipsVT):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/fips/state'

    def __init__(self,v):
        pass


class GeoNameVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/name'

    def __init__(self,v):
        pass


class GeoZipVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/usps/zip'

    def __init__(self,v):
        pass


class GeoStusabVT(TextValue):
    role = ROLE.DIMENSION
    vt_code = 'd/geo/usps/state'

    def __init__(self,v):
        pass

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