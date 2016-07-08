"""


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""
from __future__ import print_function
from six import text_type
from six import string_types
from datetime import date, time, datetime
from decorator import decorator
from ambry.util import Constant
from ambry.valuetype.exceptions import TooManyCastingErrors

ROLE = Constant()
ROLE.DIMENSION = 'd'
ROLE.CATEGORY = 'c'
ROLE.MEASURE = 'm'
ROLE.ERROR = 'e'
ROLE.KEY = 'k'
ROLE.IDENTIFIER = 'i'
ROLE.UNKNOWN = 'u'
ROLE.OTHER = 'u'

role_descriptions = {
    ROLE.DIMENSION: 'dimension',
    ROLE.CATEGORY: 'category',
    ROLE.MEASURE: 'measure',
    ROLE.ERROR: 'error',
    ROLE.KEY: 'key',
    ROLE.IDENTIFIER: 'identifier',
    ROLE.UNKNOWN: 'unknown',
    ROLE.OTHER: 'other'
}

# Levels of Measurement
LOM = Constant()
LOM.UNKNOWN = 'u'
LOM.NOMINAL = 'n'
LOM.ORDINAL = 'o'
LOM.INTERVAL = 'i'
LOM.RATIO = 'r'


@decorator
def valuetype(func, *args, **kw):
    return func(*args, **kw)


def count_errors(errors):
    if sum(len(e) for e in errors.values()) > 50:
        raise TooManyCastingErrors("Too many casting errors")


class TimeMixin(object):
    pass


class GeoMixin(object):
    pass


class DimensionMixin(object):
    pass


class LabelMixin(object):
    pass


class ValueType(object):
    role = ROLE.UNKNOWN
    low = LOM.UNKNOWN
    _pythontype = text_type
    desc = ''

    def raise_for_error(self):
        # Not an error, so nothing to raise
        pass

    @property
    def is_none(self):
        return False

    @classmethod
    def python_type(self):
        return self._pythontype

    @property
    def failed_value(self):
        return None

    @classmethod
    def description(cls):
        if cls.desc:
            return cls.desc
        elif cls.__doc__:
            return cls.desc
        else:
            return cls.vt

    @classmethod
    def subclass(cls, vt_code, vt_args):
        """
        Return a dynamic subclass that has the extra parameters built in
        :param vt_code: The full VT code, privided to resolve_type
        :param vt_args: The portion of the VT code to the right of the part that matched a ValueType
        :return:
        """
        return type(vt_code.replace('/', '_'), (cls,), {'vt_code': vt_code, 'vt_args': vt_args})

    @classmethod
    def is_time(cls):
        return issubclass(cls, TimeMixin)

    @classmethod
    def is_geo(cls):
        return issubclass(cls, GeoMixin)

    @classmethod
    def is_geoid(cls):
        from ambry.valuetype import Geoid
        return issubclass(cls, Geoid)


    @classmethod
    def is_label(cls):
        return issubclass(cls, LabelMixin)

class _NoneValue(object):
    """Represent None as a ValueType"""

    _pythontype = None

    @classmethod
    def python_type(self):
        return self._pythontype

    def raise_for_error(self):
        pass

    @property
    def is_none(self):
        return True

    @property
    def failed_value(self):
        return None

    def __bool__(self):
        return False

    def __nonzero__(self):
        return False

    def __len__(self):
        return False

    def __getattr__(self, item):
        """All properties of NoneValue are also NoneValues"""
        return self

    def __repr__(self):
        return "NoneValue"


NoneValue = _NoneValue()


class FailedValue(str, ValueType):
    """When ValueTypes fail to convert, the __new__ returns an object of this type,
    which resolves as a string containing the value that failed """

    _pythontype = str
    exc = None

    def __new__(cls, *args, **kwargs):
        o = str.__new__(cls, args[0])
        return o

    def __init__(self, v, exc=None):
        self.exc = exc

    def raise_for_error(self):
        if self.exc:
            raise self.exc

    @property
    def is_none(self):
        return False

    @property
    def failed_value(self):
        return str(self)

    def __bool__(self):
        return False

    __nonzero__ = __bool__

    def __getattr__(self, item):
        return self


def cast_int(v, header_d, errors):
    if isinstance(v, FailedValue):
        errors[header_d].add(u"Failed to cast '{}' ({}) to int in '{}': {}".format(v, type(v), header_d, v.exc))
        count_errors(errors)
        return None

    if v != 0 and not bool(v):
        return None
    else:
        try:
            return int(v)
        except Exception as e:
            errors[header_d].add(u"Failed to cast '{}' ( {} ) to int in '{}': {}".format(v, type(v), header_d, e))
            count_errors(errors)


cast_id = cast_int


def cast_long(v, header_d, errors):
    if isinstance(v, FailedValue):
        errors[header_d].add(u"Failed to cast '{}' ({}) to long in '{}': {}".format(v, type(v), header_d, v.exc))
        count_errors(errors)
        return None

    if v != 0 and not bool(v):
        return None
    else:
        try:
            return long(v)
        except Exception as e:
            errors[header_d].add(u"Failed to cast '{}' ( {} ) to long in '{}': {}".format(v, type(v), header_d, e))
            count_errors(errors)


def cast_float(v, header_d, errors):
    if isinstance(v, FailedValue):
        errors[header_d].add(u"Failed to cast '{}' ({}) to float in '{}': {}".format(v, type(v), header_d, v.exc))
        count_errors(errors)
        return None

    if v != 0 and not bool(v):
        return None
    else:
        try:
            return float(v)
        except Exception as e:
            errors[header_d].add(u"Failed to cast '{}' ( {} )  to float in '{}': {}".format(v, type(v), header_d, e))
            count_errors(errors)


def cast_str(v, header_d, errors):
    if isinstance(v, FailedValue):
        errors[header_d].add(u"Uncleared errors on value '{}' in '{}': {}".format(v, header_d, str(v.exc)))
        count_errors(errors)

    if v != 0 and not bool(v):
        return None
    else:
        try:
            return str(v)
        except Exception as e:
            errors[header_d].add(u"Failed to cast '{}' ( {} )  to str in '{}': {}".format(v, type(v), header_d, e))
            count_errors(errors)


def cast_text(v, header_d, errors):
    if isinstance(v, FailedValue):
        errors[header_d].add(u"Failed to cast '{}' ({}) to unicode in '{}': {}".format(v, type(v), header_d, v.exc))
        count_errors(errors)

    if v != 0 and not bool(v):
        return None
    else:
        try:
            return unicode(v)
        except Exception as e:
            errors[header_d].add(u"Failed to cast '{}' ( {} )  to unicode in '{}': {}".format(v, type(v), header_d, e))
            count_errors(errors)


cast_unicode = cast_text


class StrValue(str, ValueType):
    _pythontype = str
    desc = 'Character String'
    vt_code = 'str'
    lom = LOM.NOMINAL

    def __new__(cls, v):

        if v is None or v is NoneValue or v == '':
            return NoneValue

        try:
            return str.__new__(cls, v)
        except Exception as e:
            return FailedValue(v, e)


class TextValue(text_type, ValueType):
    _pythontype = text_type
    desc = 'Text String'
    vt_code = 'text'
    low = LOM.NOMINAL

    def __new__(cls, v):

        if v is None or v is NoneValue or v == '':
            return NoneValue

        try:
            return text_type.__new__(cls, v)
        except Exception as e:
            return FailedValue(v, e)


class IntValue(int, ValueType):
    _pythontype = int
    desc = 'Integer'
    vt_code = 'int'

    def __new__(cls, v):

        try:
            return int.__new__(cls, v)
        except Exception as e:

            if v is None or v is NoneValue or v == '':
                return NoneValue

            return FailedValue(v, e)


class LongValue(long, ValueType):
    _pythontype = long
    desc = 'Long'
    vt_code = 'long'

    def __new__(cls, v):
        try:
            return long.__new__(cls, v)
        except Exception as e:

            if v is None or v is NoneValue or v == '':
                return NoneValue

            return FailedValue(v, e)


class FloatValue(float, ValueType):
    _pythontype = float
    desc = 'General Floating Point'
    vt_code = 'float'

    def __new__(cls, v):

        try:
            return float.__new__(cls, v)
        except Exception as e:

            if v is None or v is NoneValue or v == '':
                return NoneValue

            return FailedValue(v, e)


class DateValue(date, ValueType):
    _pythontype = date
    role = ROLE.DIMENSION
    desc = 'Date'
    vt_code = 'date'
    low = LOM.ORDINAL

    def __new__(cls, v):
        from dateutil import parser

        if v is None or (isinstance(v, string_types) and v.strip() == ''):
            return NoneValue

        try:
            if isinstance(v, (datetime, date)):
                d = v
            else:
                d = parser.parse(str(v))

            return super(DateValue, cls).__new__(cls, d.year, d.month, d.day)
        except TypeError:
            if str(v) == 'NaT':  # THe Pandas "Not A Time" value
                return NoneValue
            raise

    def __init__(self, v):
        pass


class TimeValue(time, ValueType):
    _pythontype = time
    desc = 'time'
    low = LOM.ORDINAL

    def __new__(cls, v):
        from dateutil import parser

        if v is None or (isinstance(v, string_types) and v.strip() == ''):
            return NoneValue

        try:
            d = parser.parse(v)

            return super(TimeValue, cls).__new__(cls, d.hour, d.minute, d.second)
        except TypeError:
            if str(v) == 'NaT':  # THe Pandas "Not A Time" value
                return NoneValue
            raise


class DateTimeValue(datetime, ValueType):
    _pythontype = datetime
    desc = 'Date/Time'
    vt_code = 'datetime'
    low = LOM.ORDINAL

    def __new__(cls, v):
        from dateutil import parser

        if v is None or (isinstance(v, string_types) and v.strip() == ''):
            return NoneValue

        try:
            d = parser.parse(v)
            return super(DateTimeValue, cls).__new__(cls, d.year, d.month, d.day, d.hour, d.minute, d.second)

        except TypeError:
            if str(v) == 'NaT':  # THe Pandas "Not A Time" value
                return NoneValue
            raise


class KeyVT(IntValue):
    role = ROLE.KEY
    lom = LOM.ORDINAL
    vt_code = 'k'


class IdentifierVT(IntValue):
    role = ROLE.IDENTIFIER
    lom = LOM.ORDINAL
    vt_code = 'i'


class IntDimension(IntValue, DimensionMixin):
    role = ROLE.DIMENSION
    lom = LOM.ORDINAL
    vt_code = 'dimension/int'


class FloatDimension(FloatValue, DimensionMixin):
    role = ROLE.DIMENSION
    lom = LOM.ORDINAL
    vt_code = 'dimension/float'


class TextDimension(TextValue, DimensionMixin):
    role = ROLE.DIMENSION
    lom = LOM.NOMINAL
    vt_code = 'dimension/text'


class StrDimension(StrValue, DimensionMixin):
    role = ROLE.DIMENSION
    lom = LOM.NOMINAL
    vt_code = 'dimension'


class LabelValue(TextDimension, LabelMixin):
    desc = 'Value Label'
    vt_code = 'label'


def upper(v):
    if isinstance(v, (FailedValue, _NoneValue)):
        return v

    return v.upper()


def lower(v):
    if isinstance(v, (FailedValue, _NoneValue)):
        return v

    return v.lower()


def title(v):
    if isinstance(v, (FailedValue, _NoneValue)):
        return v

    return v.title()


def robust_int(v):
    """Parse an int robustly, ignoring commas and other cruft. """

    if isinstance(v, int):
        return v

    if isinstance(v, float):
        return int(v)

    v = str(v).replace(',', '')

    if not v:
        return None

    return int(v)


def print_value(row_n, header_d, v):
    print("print_value {}: {} = {}".format(row_n, header_d, v))
