"""Support functions for transforming rows read from input files before writing
to databases.

Copyright (c) 2015 Civic Knowlege. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""
import textwrap
from functools import partial

class CasterError(Exception):
    pass


class CastingError(TypeError):

    def __init__(self, field_name, value, message, *args, **kwargs):

        # Call the base class constructor with the parameters it needs
        Exception.__init__(self, textwrap.fill(message, 120), *args, **kwargs)

        self.field_name = field_name
        self.value = value

def coerce_int(v):
    """Convert to an int, or return if isn't an int."""
    try:
        return int(v)
    except:
        return v

def coerce_int_except(v, msg):
    """Convert to an int, throw an exception if it isn't."""

    try:
        return int(v)
    except:
        raise ValueError("Bad value: '{}'; {} ".format(v, msg))

def coerce_float(v):
    """Convert to an float, or return if isn't an int."""
    try:
        return float(v)
    except:
        return v

def coerce_float_except(v, msg):
    """Convert to an float, throw an exception if it isn't."""
    try:
        return float(v)
    except:
        raise ValueError("Bad value: '{}'; {} ".format(v, msg))

class PassthroughTransform(object):

    """Pasthorugh the value unaltered."""

    def __init__(self, column, useIndex=False):
        """"""
        # Extract the value from a position in the row
        if useIndex:
            f = lambda row, column=column: row[column.sequence_id - 1]
        else:
            f = lambda row, column=column: row[column.name]

        self.f = f

    def __call__(self, row):
        return self.f(row)

class BasicTransform(object):

    """A Callable class that will take a row and return a value, cleaned
    according to the classes cleaning rules."""

    @staticmethod
    def basic_defaults(v, column, default, f):
        """Basic defaults method, using only the column default and
        illegal_value parameters.

        WIll also convert blanks and None to the default

        """
        if v is None:
            return default
        elif v == '':
            return default
        elif str(v) == column.illegal_value:
            return default
        else:
            return f(v)

    def __init__(self, column, useIndex=False):
        """"""
        self.column = column

        # for numbers try to coerce to an integer. We'd have to use a helper func
        # with a try/catch, except in this case, integers are always all digits
        # here
        if str(column.datatype) == 'integer' or str(column.datatype) == 'integer64':
            # f = lambda v: int(v)
            msg = column.name
            f = lambda v, msg = msg: coerce_int_except(v, msg)
        elif column.datatype == 'real':
            # f = lambda v: int(v)
            msg = column.name
            f = lambda v, msg = msg: coerce_float_except(v, msg)
        else:
            f = lambda v: v

        if column.default is not None:
            if column.datatype == 'text':
                default = column.default
            else:
                default = int(column.default)
        else:
            default = None

        if default:
            f = (
                lambda v,
                column=column,
                f=f,
                default=default,
                defaults_f=self.basic_defaults: defaults_f(
                    v,
                    column,
                    default,
                    f))

        # Strip test values, but not numbers
        f = lambda v, f=f: f(v.strip()) if isinstance(v, basestring) else f(v)

        if useIndex:
            f = lambda row, column=column, f=f: f(row[column.sequence_id - 1])
        else:
            f = lambda row, column=column, f=f: f(row[column.name])

        self.f = f

    def __call__(self, row):
        return self.f(row)


#
# Functions for CasterTransformBUilder
#

class Int(int):

    """An Integer."""

    def __init__(self, v):
        int.__init__(self, v)
        if self < 0:
            raise ValueError("Must be a non negative integer")

class NonNegativeInt(int):

    '''An Integer that is >=0
    '''

    def __init__(self, v):
        int.__init__(self, v)
        if self < 0:
            raise ValueError("Must be a non negative integer")

class NaturalInt(int):

    """An Integer that is > 0."""

    def __init__(self, v):
        int.__init__(self, v)
        if self <= 0:
            raise ValueError("Must be a greater than zero")

def is_nothing(v):

    if isinstance(v, basestring):
        v = v.strip()

    if v is None or v == '' or v == '-':
        return True
    else:
        return False

def parse_int(caster, name, v, type_=int):
    """Parse as an integer, or a subclass of Int."""

    try:
        if is_nothing(v):
            return None
        else:
            return int(round(float(v), 0))
    except ValueError as e:
        caster.cast_error(int, name, v, e)
        return None

def parse_float(caster, name, v):

    try:
        if is_nothing(v):
            return None
        else:
            return float(v)
    except (TypeError, ValueError) as e:
        caster.cast_error(float, name, v, e)
        return None

def parse_str(caster, name, v):
    return str(v)

def parse_unicode(caster, name, v):
    return unicode(v)

def parse_type(type_,caster,  name, v):

    try:
        if is_nothing(v):
            return None
        else:
            return type_(v)
    except (TypeError, ValueError) as e:
        caster.cast_error(type_, name, v, e)
        return None

def parse_date(caster, name, v):
    import dateutil.parser as dp
    import datetime

    if is_nothing(v):
        return None
    elif isinstance(v, basestring):
        try:
            return dp.parse(v).date()
        except (ValueError,  TypeError) as e:
            caster.cast_error(datetime.date, name, v, e)
            return None

    elif isinstance(v, datetime.date):
        return v
    else:
        caster.cast_error(datetime.date, name, v,"Expected datetime.date or basestring, got '{}'".format(type(v)))
        return None

def parse_time(caster, name, v):
    import dateutil.parser as dp
    import datetime
    if is_nothing(v):
        return None
    elif isinstance(v, basestring):
        try:
            return dp.parse(v).time()
        except ValueError as e:
            caster.cast_error(datetime.date, name, v, e)
            return None

    elif isinstance(v, datetime.time):
        return v
    else:
        caster.cast_error(datetime.date, name, v, "Expected datetime.time or basestring, got '{}'".format(type(v)))
        return None

def parse_datetime(caster, name, v):
    import dateutil.parser as dp
    import datetime
    if is_nothing(v):
        return None
    elif isinstance(v, basestring):
        try:
            return dp.parse(v)
        except (ValueError, TypeError) as e:
            caster.cast_error(datetime.date, name, v, e)
            return None

    elif isinstance(v, datetime.datetime):
        return v
    else:
        caster.cast_error(datetime.date, name, v, "Expected datetime.time or basestring, got '{}'".format(type(v)))
        return None

class Transform(object):

    def __init__(self, error_handler = None):
        self.types = []
        self._compiled = None
        self.custom_types = {}
        self.dict_code = None

        self.dict_transform = None
        self.row_transform = None

        self.error_handler = error_handler

        self.error_accumulator = None

    def append(self, name, type_):
        self.types.append((name, type_))

        # Doing this every time is more expensive, but not much compared to row processing
        self.compile()

    def add_type(self, t):
        self.custom_types[t.__name__] = t

        self.compile()

    def make_transform(self):
        import datetime

        o = []

        for i, (name, type_) in enumerate(self.types):

            if type_ == str:
                type_ = unicode

            if type_ in [datetime.date, datetime.time, datetime.datetime, int, float, str, unicode ]:
                o.append((i, name, 'parse_{}'.format(type_.__name__)))

            else:
                o.append((i, name, 'partial(parse_type,{})'.format(type_.__name__)))

            c = []

        dict_transform = """def dict_transform(caster, row):

        import dateutil.parser as dp
        import datetime
        from functools import partial
        from ambry.bundle.etl.transform import parse_date, parse_time, parse_datetime

        return {{{}}}""".format(','.join("'{name}':{func}(caster, '{name}', row.get('{name}'))".format(i=i, name=name,func=v)
                                         for i,name,v in o))

        row_transform = """def row_transform(caster, row):

        import dateutil.parser as dp
        import datetime
        from functools import partial
        from ambry.bundle.etl.transform import parse_date, parse_time, parse_datetime

        return [{}]""".format(
            ','.join("{func}(caster, {i}, row[{i}])".format(i=i, name=name,func=v) for i, name, v in o))

        return dict_transform,  row_transform

    def compile(self):
        # import uuid

        # Get the code in string form.
        dtc,  rtc  = self.make_transform()

        for k, v in self.custom_types.items():
            globals()[k] = v

        exec dtc
        self.dict_transform = locals()['dict_transform']

        exec rtc
        self.row_transform = locals()['row_transform']

    def cast_error(self, type_, name, v, e):
        self.error_accumulator[name] = {'type':type_, 'value':v, 'exception':str(e)}


class DictTransform(Transform):

    def __call__(self, row):
        """Call the caster to cast all of the values in a row.
        """
        self.error_accumulator = {}
        row = self.dict_transform(self, row)
        if self.error_handler:
            row, self.error_accumulator = self.error_handler(row, self.error_accumulator)

        return row, self.error_accumulator

class ListTransform(Transform):

    def __call__(self, row):
        """Call the caster to cast all of the values in a row.
        """
        self.error_accumulator = {}
        row = self.row_transform(self, row)
        if self.error_handler:
            row,self.error_accumulator  = self.error_handler(row, self.error_accumulator)

        return row, self.error_accumulator