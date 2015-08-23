"""Support functions for transforming rows read from input files before writing
to databases.

Copyright (c) 2015 Civic Knowlege. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""
import dateutil.parser as dp
import datetime
import sys
import textwrap

from six import string_types, iteritems

from .pipeline import Pipe, MissingHeaderError


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

    if isinstance(v, string_types):
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
    except OverflowError as e:
        raise OverflowError("Failed to convert int in caster, for column {}, value '{}' ".format(name, v))


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


def parse_type(type_, caster,  name, v):

    try:
        if is_nothing(v):
            return None
        else:
            return type_(v)
    except (TypeError, ValueError) as e:
        caster.cast_error(type_, name, v, e)
        return None


def parse_date(caster, name, v):
    if is_nothing(v):
        return None
    elif isinstance(v, string_types):
        try:
            return dp.parse(v).date()
        except (ValueError,  TypeError) as e:
            caster.cast_error(datetime.date, name, v, e)
            return None

    elif isinstance(v, datetime.date):
        return v
    else:
        caster.cast_error(
            datetime.date, name, v, "Expected datetime.date or basestring, got '{}'".format(type(v)))
        return None


def parse_time(caster, name, v):
    if is_nothing(v):
        return None
    elif isinstance(v, string_types):
        try:
            return dp.parse(v).time()
        except ValueError as e:
            caster.cast_error(datetime.date, name, v, e)
            return None

    elif isinstance(v, datetime.time):
        return v
    else:
        caster.cast_error(
            datetime.date, name, v, "Expected datetime.time or basestring, got '{}'".format(type(v)))
        return None


def parse_datetime(caster, name, v):
    if is_nothing(v):
        return None
    elif isinstance(v, string_types):
        try:
            return dp.parse(v)
        except (ValueError, TypeError) as e:
            caster.cast_error(datetime.date, name, v, e)
            return None

    elif isinstance(v, datetime.datetime):
        return v
    else:
        caster.cast_error(
            datetime.date, name, v, "Expected datetime.time or basestring, got '{}'".format(type(v)))
        return None

def derive_val(f,  caster, row, i, name, v  ):

    print '!!!', code

    return None


class Transform(object):

    def __init__(self, error_handler=None):
        self.types = []
        self._compiled = None
        self.custom_types = {}
        self.dict_code = None

        self.dict_transform = None
        self.row_transform = None

        self.error_handler = error_handler

        self.error_accumulator = None

        self.errors = None

        self.dict_transform_code = None
        self.row_transform_code = None

    def append(self, name, type_):

        self.types.append((name, type_))

    def add_to_env(self, t, name=None):
        if not name:
            name = t.__name__
        self.custom_types[name] = t


    def make_transform(self):

        o = []

        for i, (name, type_) in enumerate(self.types):

            if type_ == str:
                type_ = unicode

            if type_ in [datetime.date, datetime.time, datetime.datetime, int, float, str, unicode]:
                o.append((i, name, 'parse_{}'.format(type_.__name__)))
            elif isinstance(type_, basestring):
                # It is actually a dervivedfrom reference
                o.append((i, name, "partial(derive_val,row,'{}')".format(type_)))
            else:
                o.append((i, name, 'partial(parse_type,{})'.format(type_.__name__)))

        dict_transform = "lambda caster, row:{{{}}}".format(
                        ','.join("'{name}':{func}(caster, '{name}', row.get('{name}'))".format(i=i, name=name, func=v)
                        for i, name, v in o))

        row_transform = "lambda caster, row: [{}]".format(
                        ','.join("{func}(caster, {i}, row[{i}])".format(i=i, name=name, func=v)
                        for i, name, v in o))

        return dict_transform,  row_transform

    def compile(self):

        import dateutil.parser as dp
        import datetime
        from functools import partial
        from ambry.etl.transform import parse_date, parse_time, parse_datetime
        import sys

        # Get the code in string form.
        self.dict_transform_code, self.row_transform_code = self.make_transform()

        localvars = dict(locals().items())

        localvars.update(sys.modules[__name__].__dict__.items())

        for k, v in iteritems(self.custom_types):
            localvars[k] = v

        self.dict_transform = eval(self.dict_transform_code,  localvars)

        self.row_transform = eval(self.row_transform_code,  localvars)

    def cast_error(self, type_, name, v, e):
        self.error_accumulator[name] = {'type': type_, 'value': v, 'exception': str(e)}


class CasterPipe(Transform, Pipe, ):

    def __init__(self, table=None, error_handler=None):
        super(CasterPipe, self).__init__(error_handler)
        self.errors = []
        self.table = table

        self.row_transform_code = self.dict_transform_code = ''

    def get_caster_f(self, name):

        try:
            caster_f = getattr(self.bundle, name)
        except AttributeError:
            pass

        try:
            caster_f = getattr(sys.modules['ambry.build'], name)
        except AttributeError:
            pass

        if not caster_f:
            raise AttributeError("Could not find caster '{}' in bundle class or bundle module ".format(name))

        return caster_f

    def process_header(self, row):
        from functools import partial
        from ambry.valuetype import import_valuetype

        if self.table:
            table = self.table
        else:
            self.table = table = self.source.dest_table

        env = {}
        row_parts = []

        for i,c in enumerate(self.table.columns):

            f_name = "f_"+str(i)

            # New columns, created from extracting data from another column, using the code described in the
            # 'derivedfrom" column of the schema
            # Derived columns are new; there is no source column comminf from the upstream soruce
            if c.derivedfrom:
                inner_f = eval("lambda row, v, caster=self, i=i, header=c.name: {}".format(c.derivedfrom))
                env[f_name] = lambda  row, v, caster=self, i=i, header=c.name: derive_val(inner_f, caster, row, i, header, v)

            else:
                type_f = c.valuetype_class

                if c.name not in row:
                    # There is no source column, so insert a None. Maybe this should be an error.
                    env[f_name] = lambda row, v, caster=self, i=i, header=c.name: None

                elif c.caster:
                    # Regular casters, from the "caster" column of the schema
                    caster_f = self.get_caster_f(c.caster)
                    self.add_to_env(caster_f)
                    env[f_name] = lambda row, v, caster=self, i=i, header=c.name: caster_f(v)

                elif type_f.__name__ == c.datatype:
                    # Plain python type

                    env[f_name] = eval("lambda row, v, caster=self, i=i, header=c.name: parse_type(caster, header, v)"
                                       .format(type_f.__name__))

                else:
                    # Special valuetype, not a normal python type
                    env[f_name] = eval("lambda row, v, caster=self, i=i, header=c.name: parse_type({},caster, header, v)"
                            .format(c.datatype))
                    self.add_to_env(import_valuetype(c.datatype), c.datatype)

            self.add_to_env(env[f_name], f_name)

            row_parts.append(f_name)

        print row_parts
        print env

        #self.compile()

        print 'HERE'

        raise StopIteration()

        # Return the table header, rather than the original row header.
        return [ c.name for c in self.table.columns ]

    def process_body(self, row):

        self.error_accumulator = {}  # Clear the accumulator
        try:
            row = self.row_transform(self, row)
        except IndexError:
            raise IndexError('Header has {} items, Row has {} items, caster has {}\nheaders= {}\ncaster = {}\nrow    = {}'
                             .format(len(self.headers), len(row), len(self.types),
                                     self.headers, [t[0] for t in self.types], row))
        except Exception as e:
            m = str(e)

            print self.pipeline

            raise Exception("Failed to process row '{}'\n{}".format(row, e))

        if self.error_handler:
            row, self.error_accumulator = self.error_handler(row, self.error_accumulator)
        else:
            self.errors.append(self.error_accumulator)

        return row

    def __str__(self):
        from ambry.util import qualified_class_name

        return (qualified_class_name(self) + "\n" +
                self.indent + "Row: "+self.row_transform_code + "\n" +
                self.indent + "Dict: "+ self.dict_transform_code + "\n" )
