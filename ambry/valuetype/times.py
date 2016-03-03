"""


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from core import *

class GeneralTimeVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/dt'

    def __init__(self,v):
        pass


class TimeVT(TimeValue):
    role = ROLE.DIMENSION
    vt_code = 'd/dt/time'

    def __init__(self,v):
        pass

class DateVT(DateValue):
    role = ROLE.DIMENSION
    vt_code = 'd/dt/date'

    def __init__(self,v):
        pass


class DateTimeVT(DateTimeValue):
    role = ROLE.DIMENSION
    vt_code = 'd/dt/datetime'

    def __init__(self,v):
        pass


class DurationVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/duration'

    def __init__(self,v):
        pass


class DurationYearRangeVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/duration/yrange'

    def __init__(self,v):
        pass


class DurationIsoVT(ValueType):
    role = ROLE.DIMENSION
    vt_code = 'd/duration/iso'

    def __init__(self,v):
        pass


times_value_types = {
    'date': DateVT,
    'datetime': DateTimeVT,
    'time': TimeVT,
    "d/dt": GeneralTimeVT,
    "d/dt/date": TimeVT,
    "d/dt/date": DateVT,
    "d/dt/datetime": DateTimeVT,
    "d/duration": DurationVT,
    "d/duration/yrange": DurationYearRangeVT,
    "d/duration/iso": DurationIsoVT,
}

