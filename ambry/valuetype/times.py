"""


Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from core import *
import re

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

class IntervalVT(StrValue):
    """A generic time interval"""
    role = ROLE.DIMENSION
    vt_code = 'd/interval'

    year_re = re.compile(r'(\d{4})')
    year_range_re = re.compile(r'(\d{4})(?:\/|-|--)(\d{4})') # / and -- make it also an ISO interval

    def __new__(cls, v):

        if v is None or (isinstance(v, string_types) and v.strip() == ''):
            return None

        v = v.strip()

        m = cls.year_re.match(v)

        if m:
            return IntervalYearVT(v)

        m = cls.year_range_re.match(v)

        if m:
            return IntervalYearRangeVT(m.group(1), m.group(2))

        return IntervalIsoVT(v)

class IntervalYearVT(IntValue):
    """Time interval of a single year"""
    role = ROLE.DIMENSION
    vt_code = 'd/interval/year'


    def __new__(cls, v):

        if v is None or (isinstance(v, string_types) and v.strip() == ''):
            return None

        return IntValue.__new__(cls, v)

    @property
    def start(self):
        return int(self)

    @property
    def end(self):
        return int(self)

class IntervalYearRangeVT(ValueType):
    """A half-open time interval between two years"""
    role = ROLE.DIMENSION
    vt_code = 'd/interval/yrange'

    y1 = None
    y2 = None

    def __new__(cls, *args, **kwargs):

        v = args[0]

        if v is None or (isinstance(v, string_types) and v.strip() == ''):
            return None

        if len(args) in (1,2):
            return ValueType.__new__(cls, *args, **kwargs)

        return FailedValue(args[0])

    def __init__(self, v, y2=None):

        if y2 is not None:
            self.y1 = v
            self.y2 = y2
        else:
            m = IntervalVT.year_range_re.match(v)
            self.y1 = int(m.group(1))
            self.y2 = int(m.group(2))

    @property
    def start(self):
        return int(self.y1)

    @property
    def end(self):
        return int(self.y2)

    def __str__(self):
        return str(self.y1)+'/'+str(self.y2)

class IntervalIsoVT(StrValue):
    role = ROLE.DIMENSION
    vt_code = 'd/duration/iso'

    interval = None

    def __new__(cls, *args, **kwargs):

        v = args[0]

        if v is None or (isinstance(v, string_types) and v.strip() == ''):
            return None

        return StrValue.__new__(cls, *args, **kwargs)

    def __init__(self, v):
        import aniso8601

        self.interval = aniso8601.parse_interval(v)

    def __str__(self):

        return str(self.interval[0])+'/'+str(self.interval[1])

    @property
    def start(self):
        return self.start

    @property
    def end(self):
        return self.end


times_value_types = {
    'date': DateVT,
    'datetime': DateTimeVT,
    'time': TimeVT,
    "d/interval": IntervalVT,
    "d/interval/year": IntervalYearRangeVT,
    "d/interval/yrange": IntervalYearRangeVT,
    "d/interval/iso": IntervalIsoVT,
}

