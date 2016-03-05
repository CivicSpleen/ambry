"""


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from six import text_type
from six import string_types
from datetime import date, time, datetime
from decorator import decorator
from ambry.util import Constant

ROLE = Constant()
ROLE.DIMENSION = 'd'
ROLE.MEASURE = 'm'
ROLE.ERROR = 'e'

@decorator
def valuetype(func, *args, **kw):
    return func(*args, **kw)


def cast_int(v, header_d, clear_errors):

    if isinstance(v, FailedValue):
        if clear_errors:
            return None
        else:
            raise ValueError("Failed to cast '{}'  to int for column {}".format(v, header_d))

    if v != 0 and not bool(v):
        return None
    else:
        try:
            return int(v)
        except Exception as e:
            raise ValueError("Failed to cast '{}' ( {} ) to int for column {}".format(v,type(v),header_d))

def cast_float(v, header_d, clear_errors):


    if isinstance(v, FailedValue):
        if clear_errors:
            return None
        else:
            raise ValueError("Failed to cast '{}'  to float for column {}".format(v, header_d))

    if v != 0 and not bool(v):
        return None
    else:
        try:
            return float(v)
        except Exception as e:
            raise ValueError("Failed to cast '{}' ( {} )  to float for column {}".format(v,type(v),header_d))

def cast_str(v, header_d, clear_errors):

    if isinstance(v, FailedValue):
        if clear_errors:
            return None
        else:
            raise ValueError("Failed to cast '{}'  to str for column {}".format(v, header_d))

    if v != 0 and not bool(v):
        return None
    else:
        try:
            return str(v)
        except Exception as e:
            raise ValueError("Failed to cast '{}' ( {} )  to str for column {}".format(v,type(v),header_d))

def cast_unicode(v, header_d, clear_errors):

    if isinstance(v, FailedValue):
        if clear_errors:
            return None
        else:
            raise ValueError("Failed to cast '{}'  to unicode for column {}".format(v, header_d))

    if v != 0 and not bool(v):
        return None
    else:
        try:
            return unicode(v)
        except Exception as e:
            raise ValueError("Failed to cast '{}' ( {} )  to unicode for column {}".format(v,type(v),header_d))

class ValueType(object):

    _pythontype = text_type

    @classmethod
    def python_type(self):
        return self._pythontype

    @classmethod
    def intuit_name(self, name):
        """Return a numeric value in the range [-1,1), indicating the likelihood that the name is for a variable of
        of this type. -1 indicates a strong non-match, 1 indicates a strong match, and 0 indicates uncertainty. """
        raise NotImplementedError()

    @property
    def failed_value(self):
        return None

class FailedValue(str, ValueType):
    """When ValueTypes fail to convert, the __new__ returns an object of this type,
    which resolves as a string containing the value that failed """

    _pythontype = str

    def __new__(cls, v):
        o = str.__new__(cls,v)
        return o

    @property
    def failed_value(self):
        return str(self)

    def __bool__(self):
        return False

    def __nonzero__(self):
        return False

class StrValue(str, ValueType):
    _pythontype = str

    def __new__(cls, v):
        try:
            return str.__new__(cls, v)
        except:
            return FailedValue(v)

class TextValue(text_type, ValueType):
    _pythontype = text_type

    def __new__(cls, v):
        try:
            return text_type.__new__(cls, v)
        except:
            return FailedValue(v)

class IntValue(int, ValueType):
    _pythontype = int

    def __new__(cls, v):
        try:
            return int.__new__(cls, v)
        except:
            return FailedValue(v)

class FloatValue(float, ValueType):
    _pythontype = float

    def __new__(cls, v):

        try:
            return float.__new__(cls, v)
        except:
            return FailedValue(v)



class DateValue(date, ValueType):
    _pythontype = date

    def __new__(cls, v):
        from dateutil import parser

        if v is None or (isinstance(v, string_types) and v.strip() == ''):
            return None

        if isinstance(v, (datetime, date)):
            d = v
        else:
            d = parser.parse(v)

        return super(DateValue, cls).__new__(cls, d.year, d.month, d.day)

class TimeValue(time, ValueType):
    _pythontype = time

    def __new__(cls, v):
        from dateutil import parser

        if v is None or (isinstance(v, string_types) and v.strip() == ''):
            return None

        d = parser.parse(v)

        return super(TimeValue, cls).__new__(cls, d.hour, d.minute, d.second)




class DateTimeValue(datetime, ValueType):
    _pythontype = datetime

    def __new__(cls, v):
        from dateutil import parser

        if v is None or (isinstance(v, string_types) and v.strip() == ''):
            return None

        d = parser.parse(v)
        return super(DateTimeValue, cls).__new__(cls, d.year, d.month, d.day, d.hour, d.minute, d.second)

