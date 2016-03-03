"""


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from six import text_type
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


# ValueType second-level subclasses, StrValue, IntValue, etc, are described from fundamental types so they
# can be inserted directly into the database.

def MaybeFail(value_class, v):
    """Try to parse v with the value_class, and return a FailedValue on failures, or None if the
    input is none"""

    if v is None:
        return None

    try:
        return value_class(v)
    except ValueError as e:
        return FailedValue(value_class, v, e)


class ValueType(object):

    _pythontype = str
    failed_value = None

    def __new__(cls, v):
        o = cls._pythontype.__new__(cls, cls.parse(v))
        return o

    @classmethod
    def python_type(self):
        return self._pythontype

    @classmethod
    def intuit_name(self, name):
        """Return a numeric value in the range [-1,1), indicating the likelihood that the name is for a variable of
        of this type. -1 indicates a strong non-match, 1 indicates a strong match, and 0 indicates uncertainty. """
        raise NotImplementedError()

    @classmethod
    def new(cls, v):
        """Return a new instance of the class, raising an Exception on parse failures"""
        return v

    @classmethod
    def new_or_none(cls, v):
        """Return a new instance of the class, returning a non on parse failures."""
        try:
            return v
        except Exception as e:
            return FailedValue(cls, v, e)

class FailedValue(object):
    """Represents a value that could not be parsed by the original type class"""

    def __init__(self, type_class, failed_value, exc):
        self.type_class = type_class
        self.failed_value = failed_value
        self.exc = exc

    def __nonzero__(self):
        return False

    def __int__(self):
        return None

    def __float__(self):
        return float('nan')

    def __str__(self):
        return ''

    def __unicode__(self):
        return ''



class StrValue(str, ValueType):
    _pythontype = str


class TextValue(text_type, ValueType):
    _pythontype = text_type


class IntValue(int, ValueType):
    _pythontype = int


class FloatValue(float, ValueType):
    _pythontype = float


class DateValue(date, ValueType):
    _pythontype = date

    def __new__(cls, v):
        from dateutil import parser
        if isinstance(v, (datetime, date)):
            d = v
        else:
            d = parser.parse(v)

        return super(DateValue, cls).__new__(cls, d.year, d.month, d.day)

class TimeValue(time, ValueType):
    _pythontype = time

    def __new__(cls, v):
        from dateutil import parser
        d = parser.parse(v)
        return super(TimeValue, cls).__new__(cls, d.hour, d.minute, d.second)


class DateTimeValue(datetime, ValueType):
    _pythontype = datetime

    def __new__(cls, v):
        from dateutil import parser

        d = parser.parse(v)
        return super(DateTimeValue, cls).__new__(cls, d.year, d.month, d.day, d.hour, d.minute, d.second)

