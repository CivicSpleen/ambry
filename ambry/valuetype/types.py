"""Math functions available for use in derivedfrom columns


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

class NullValue(Exception):
    """Raised from a caster to indicate that the returned value should be None"""

import dateutil.parser as dp
import datetime
import textwrap

from six import string_types, iteritems

class CodeValue(object):

    def __init__(self, code):
        self.code = code

    code = None

    def __str__(self):
        return None

    def __unicode__(self):
        return None


def row_number(row_n):
    return row_n

def nullify(v):
    """Convert empty strings and strings with only spaces to None values. """

    if isinstance(v, string_types):
        v = v.strip()

    if v is None or v == '':
        return None
    else:
        return v


#
# Casters that retiurn a default valur
#
def int_d(v, default=None):
    """Cast to int, or on failure, return a default Value"""

    try:
        return int(v)
    except:
        return default

def float_d(v, default=None):
    """Cast to int, or on failure, return a default Value"""

    try:
        return float(v)
    except:
        return default

#
# Casters that return a null on failure
#

def int_n(v):
    """Cast to int, or on failure, return a default Value"""

    try:
        return int(v) # Just to be sure the code property exists
    except:
        return None

def float_n(v):
    """Cast to int, or on failure, return None"""

    try:
        return float(v)  # Just to be sure the code property exists
    except:
        return None


def int_e(v):
    """Cast to int, or on failure raise a NullValue exception"""

    try:
        return int(v)
    except:
        raise NullValue(v)


class CasterError(Exception):
    pass


class CastingError(TypeError):

    def __init__(self, type_target, field_header, value, message, *args, **kwargs):

        self.type_target = type_target
        self.field_header = field_header
        self.value = value

        message = "Failed to cast column '{}' value='{}' to '{}': {} "\
            .format(field_header, value, type_target, message)

        # Call the base class constructor with the parameters it needs
        Exception.__init__(self, textwrap.fill(message, 120), *args, **kwargs)


def parse_int(v, header_d):
    """Parse as an integer, or a subclass of Int."""

    if v is None:
        return None

    try:
        # The converson to float allows converting float strings to ints.
        # The conversion int('2.134') will fail.
        return int(round(float(v), 0))
    except (TypeError, ValueError) as e:
        raise CastingError(int, header_d, v, str(e))

def parse_float(v,  header_d):

    if v is None:
        return None

    try:
        return float(v)
    except (TypeError, ValueError) as e:
        raise CastingError(float, header_d, v, str(e))


def parse_str(v, i_d, header_d, errors):

    # This is often a no-op, but it ocassionally converts numbers into strings

    if v is None: return None

    try:
        return str(v).strip()
    except UnicodeEncodeError:
        return unicode(v).strip()

def parse_unicode(v,  header_d):

    if v is None: return None

    try:
        return unicode(v).strip()
    except Exception as e:
        raise CastingError(unicode, header_d, v, str(e))

def parse_type(type_, v,  header_d):

    if v is None: return None

    try:
        return type_(v)
    except (TypeError, ValueError) as e:
        raise CastingError(type_, header_d, v, str(e))


def parse_date(v, header_d):

    if v is None: return None

    if isinstance(v, string_types):
        try:
            return dp.parse(v).date()
        except (ValueError,  TypeError) as e:
            raise CastingError(datetime.date, header_d, v, str(e))

    elif isinstance(v, datetime.date):
        return v
    else:
        raise CastingError(int, header_d, v, "Expected datetime.date or basestring, got '{}'".format(type(v)))

def parse_time(v,  header_d):

    if v is None: return None

    if isinstance(v, string_types):
        try:
            return dp.parse(v).time()
        except ValueError as e:
            raise CastingError(datetime.time, header_d, v, str(e))

    elif isinstance(v, datetime.time):
        return v
    else:
        raise CastingError(int, header_d, v, "Expected datetime.time or basestring, got '{}'".format(type(v)))

def parse_datetime(v,  header_d):

    if v is None: return None

    if isinstance(v, string_types):
        try:
            return dp.parse(v)
        except (ValueError, TypeError) as e:
            raise CastingError(datetime.datetim, header_d, v, str(e))

    elif isinstance(v, datetime.datetime):
        return v
    else:
        raise CastingError(int, header_d, v, "Expected datetime.time or basestring, got '{}'".format(type(v)))

