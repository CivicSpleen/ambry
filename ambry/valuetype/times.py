"""


Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from core import *
import re

from datetime import date, time, datetime

def cast_date(v, header_d, clear_errors, errors):
    if v is None or v == '':
        return None
    elif isinstance(v, date):
        return v
    elif isinstance(v, ValueType):
        return v.__date__()

    raise ValueError("cast_date: value '{}' for column '{}'  is not a date, and full casting is not implemented yet"
                     .format(v,header_d))

def cast_datetime(v, header_d, clear_errors, errors):
    if v is None or v == '':
        return None
    elif isinstance(v, datetime):
        return v
    elif isinstance(v, ValueType):
        return v.__datetime__()

    raise ValueError("cast_datetime: value '{}' for column '{}'  is not a datetime, and full casting is not implemented yet"
                     .format(v,header_d))

def cast_time(v, header_d, clear_errors, errors):

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
    desc = 'Time'
    lom = LOM.ORDINAL

class DateVT(DateValue):
    role = ROLE.DIMENSION
    vt_code = 'd/dt/date'
    desc = 'Date'
    lom = LOM.ORDINAL

    def __init__(self, v):
        super(DateVT, self).__init__(v)


class DateTimeVT(DateTimeValue):
    role = ROLE.DIMENSION
    vt_code = 'd/dt/datetime'
    desc = 'Date and time'
    lom = LOM.ORDINAL

class IntervalVT(StrValue):
    """A generic time interval"""
    role = ROLE.DIMENSION
    vt_code = 'd/interval'
    desc = 'Time interval'
    lom = LOM.ORDINAL

    year_re = re.compile(r'^(\d{4})$')
    year_range_re = re.compile(r'(\d{4})(?:\/|-|--)(\d{4})') # / and -- make it also an ISO interval

    def __new__(cls, v):

        if v is None or (isinstance(v, string_types) and v.strip() == ''):
            return NoneValue

        if isinstance(v, (int, float)):
            return IntervalYearVT(int(v))

        if isinstance(v, string_types):
            v = v.strip()

        m = cls.year_re.match(v)

        if m:
            return IntervalYearVT.__new__(IntervalYearVT,v)

        m = cls.year_range_re.match(v)

        if m:
            return IntervalYearRangeVT.__new__(IntervalYearRangeVT, v)

        return IntervalIsoVT(v)

class IntervalYearVT(IntValue):
    """Time interval of a single year"""
    role = ROLE.DIMENSION
    vt_code = 'd/interval/year'
    desc = 'Single year Interval'
    lom = LOM.ORDINAL

    def __new__(cls, v):

        if v is None or (isinstance(v, string_types) and v.strip() == ''):
            return NoneValue

        return IntValue.__new__(cls, v)

    @property
    def start(self):
        return int(self)

    @property
    def end(self):
        return int(self)

class IntervalYearRangeVT(StrValue):
    """A half-open time interval between two years"""
    role = ROLE.DIMENSION
    vt_code = 'd/interval/yrange'
    desc = 'Year range interval'
    lom = LOM.ORDINAL

    y1 = None
    y2 = None

    def __new__(cls, v):
        if v is None or (isinstance(v, string_types) and v.strip() == ''):
            return NoneValue

        m = IntervalVT.year_range_re.match(v)

        if not m:
            return FailedValue(v,ValueError("IntervalYearRangeVT failed to match year range"))

        o = StrValue.__new__(cls, v)

        o.y1 = int(m.group(1))
        o.y2 = int(m.group(2))

        return o

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
    desc = 'ISO FOrmat Interval'
    lom = LOM.ORDINAL

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
    'd/date': DateVT,
    'd/datetime': DateTimeVT,
    'd/time': TimeVT,
    "d/interval": IntervalVT,
    "d/interval/year": IntervalYearRangeVT,
    "d/interval/yrange": IntervalYearRangeVT,
    "d/interval/iso": IntervalIsoVT,
}

