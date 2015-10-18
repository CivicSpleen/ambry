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

    def __init__(self, field_header, value, message, *args, **kwargs):

        # Call the base class constructor with the parameters it needs
        Exception.__init__(self, textwrap.fill(message, 120), *args, **kwargs)

        self.field_name = field_name
        self.value = value


def parse_int(v, i_d, header_d, errors):
    """Parse as an integer, or a subclass of Int."""

    try:
        return int(round(float(v), 0))
    except (ValueError, TypeError) as e:
        errors[i_d] = (i_d, header_d, e)
        return None
    except OverflowError as e:
        raise OverflowError("Failed to convert int in pipe, for column {}, value '{}' ".format(header, v))


def parse_float(pipe, header, v, row, scratch):

    try:
        return float(v)
    except (TypeError, ValueError) as e:
        caster.cast_error(errors, float, header, v, e)
        return None


def parse_str(pipe, header, v, row, scratch):

    # This is often a no-op, but it ocassionally convertes numbers into strings

    try:
        return str(v).strip()
    except UnicodeEncodeError:
        return unicode(v).strip()


def parse_unicode(pipe, header, v, row, scratch):
    return unicode(v).strip()


def parse_type(type_, pipe,  header, v, row, scratch):

    try:
        return type_(v)
    except (TypeError, ValueError) as e:
        caster.cast_error(errors, type_, header, v, e)
        return None


def parse_date(pipe, header, v, row, scratch):

    if isinstance(v, string_types):
        try:
            return dp.parse(v).date()
        except (ValueError,  TypeError) as e:
            caster.cast_error(errors, datetime.date, header, v, e)
            return None

    elif isinstance(v, datetime.date):
        return v
    else:
        caster.cast_error(errors,
            datetime.date, header, v, "Expected datetime.date or basestring, got '{}'".format(type(v)))
        return None


def parse_time(pipe, header, v, row, scratch):

    if isinstance(v, string_types):
        try:
            return dp.parse(v).time()
        except ValueError as e:
            caster.cast_error(errors, datetime.date, header, v, e)
            return None

    elif isinstance(v, datetime.time):
        return v
    else:
        caster.cast_error(errors,
            datetime.date, header, v, "Expected datetime.time or basestring, got '{}'".format(type(v)))
        return None


def parse_datetime(pipe, header, v, row, errors):

    if isinstance(v, string_types):
        try:
            return dp.parse(v)
        except (ValueError, TypeError) as e:
            caster.cast_error(errors, datetime.date, header, v, e)
            return None

    elif isinstance(v, datetime.datetime):
        return v
    else:
        caster.cast_error(errors,
            datetime.date, header, v, "Expected datetime.time or basestring, got '{}'".format(type(v)))
        return None
