"""


Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from core import *

from datetime import date, time, datetime

def cast_date(v, header_d, clear_errors):
    if v is None or v == '':
        return None
    elif isinstance(v, date):
        return v
    elif isinstance(v, ValueType):
        return v.__date__()

    raise ValueError("cast_date: value '{}' for column '{}'  is not a date, and full casting is not implemented yet"
                     .format(v,header_d))

def cast_datetime(v, header_d, clear_errors):
    if v is None or v == '':
        return None
    elif isinstance(v, datetime):
        return v
    elif isinstance(v, ValueType):
        return v.__datetime__()

    raise ValueError("cast_datetime: value '{}' for column '{}'  is not a datetime, and full casting is not implemented yet"
                     .format(v,header_d))

def cast_time(v, header_d, clear_errors):

    if v is None or v == '':
        return None
    elif isinstance(v, time):
        return v
    elif isinstance(v, ValueType):
        return v.__time__()

    raise ValueError("cast_time: value '{}' for column '{}' is not a time, and full casting is not implemented yet"
                     .format(v,header_d))




class TimeVT(TimeValue):
    role = ROLE.DIMENSION
    vt_code = 'd/dt/time'


class DateVT(DateValue):
    role = ROLE.DIMENSION
    vt_code = 'd/dt/date'



class DateTimeVT(DateTimeValue):
    role = ROLE.DIMENSION
    vt_code = 'd/dt/datetime'



class DurationVT(StrValue):
    role = ROLE.DIMENSION
    vt_code = 'd/duration'



class DurationYearRangeVT(StrValue):
    role = ROLE.DIMENSION
    vt_code = 'd/duration/yrange'


class DurationIsoVT(StrValue):
    role = ROLE.DIMENSION
    vt_code = 'd/duration/iso'



times_value_types = {
    'date': DateVT,
    'datetime': DateTimeVT,
    'time': TimeVT,
    "d/duration": DurationVT,
    "d/duration/yrange": DurationYearRangeVT,
    "d/duration/iso": DurationIsoVT,
}

