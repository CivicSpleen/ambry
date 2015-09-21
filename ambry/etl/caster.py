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

    # This is often a no-op, but it ocassionally convertes numbers into strings

    try:
        return str(v).strip()
    except UnicodeEncodeError:
        return unicode(v).strip()


def parse_unicode(caster, name, v):
    return unicode(v).strip()


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

class CasterPipe(Pipe):

    def __init__(self, table=None, error_handler=None):

        self.errors = []
        self.table = table

        self.row_processor = None

        self.types = []
        self._compiled = None
        self.custom_types = {}

        self.error_handler = error_handler

        self.error_accumulator = None

        self.transform_code = ''
        self.col_code = []

    def add_to_env(self, t, name=None):
        if not name:
            name = t.__name__
        self.custom_types[name] = t

    def cast_error(self, type_, name, v, e):
        self.error_accumulator[name] = {'type': type_, 'value': v, 'exception': str(e)}

    def get_caster_f(self, name):

        caster_f = None

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

    def env(self, **kwargs):
        import dateutil.parser as dp
        import datetime
        from functools import partial
        from ambry.etl import parse_date, parse_time, parse_datetime
        import sys
        localvars = dict(locals().items())

        localvars.update(sys.modules[__name__].__dict__.items())

        for k, v in iteritems(self.custom_types):
            localvars[k] = v

        for k, v in kwargs.items():
            localvars[k] = v

        return localvars


    def process_header(self, header):

        from ambry.etl import RowProxy
        from ambry.valuetype import import_valuetype

        if self.table:
            table = self.table
        else:
            self.table = table = self.source.dest_table

        env = {}
        row_parts = []

        col_code = [None]*len(self.table.columns)

        # Create an entry in the row processor function for each output column in the schema.
        for i,c in enumerate(self.table.columns):

            f_name = "f_"+str(c.name)

            type_f = c.valuetype_class

            if c.name not in header:
                # There is no source column, so insert a None. Maybe this should be an error.
                env[f_name] = lambda row, v, caster=self, i=i, header=c.name: None

                col_code[i]  = (c.name,"None")

            elif c.caster:
                # Regular casters, from the "caster" column of the schema
                caster_f = self.get_caster_f(c.caster)
                self.add_to_env(caster_f)

                env[f_name] = eval("lambda row, v, caster=self, i=i, header=header: {}(v)".format(c.caster),
                                   self.env(i=i, header=c.name))

                col_code[i] = (c.name,c.caster)

            elif type_f.__name__ == c.datatype:
                # Plain python type

                env[f_name] = eval("lambda row, v, caster=self, i=i, header=header: parse_{}(caster, header, v)"
                                   .format(type_f.__name__), self.env(i=i, header=c.name))

                col_code[i] = (c.name, "parse_{}(caster, header, v)".format(type_f.__name__) )

            else:
                # Special valuetype, not a normal python type
                vt_name = c.datatype.replace('.','_')
                self.add_to_env(import_valuetype(c.datatype), vt_name)
                env[f_name] = eval("lambda row, v, caster=self, i=i, header=header: parse_type({},caster, header, v)"
                        .format(vt_name), self.env(i=i, header=c.name))

                col_code[i] = (c.name, "parse_type({},caster, header, v)".format(c.datatype.replace('.','_')))

            self.add_to_env(env[f_name], f_name)

            try:
                header_index = header.index(c.name)
            except ValueError:
                header_index = None

            row_parts.append((f_name, header_index))

        self.col_code = col_code

        inner_code = ','.join(["{}(row, row[{}])".format(f_name, index) if index != None else "None"
                               for (f_name, index) in row_parts])

        self.transform_code = "lambda row: [{}] ".format(inner_code)

        self.row_processor = eval(self.transform_code, self.env())

        # Return the table header, rather than the original row header.
        self.new_header =  [ c.name for c in self.table.columns ]

        self.row_proxy = RowProxy(self.new_header)

        return self.new_header

    def process_body(self, row):

        self.error_accumulator = {}  # Clear the accumulator
        try:
            row = self.row_processor(self.row_proxy.set_row(row))

            if len(row) != len(self.headers):

                raise CasterError("Row length does not match header length")

        except IndexError:
            raise IndexError('Header has {} items, Row has {} items, caster has {}\nheaders= {}\ncaster = {}\nrow    = {}'
                             .format(len(self.headers), len(row), len(self.types),
                                     self.headers, [t[0] for t in self.types], row))
        except Exception as e:
            m = str(e)

            print self.pipeline

            raise

            raise Exception("Failed to process row '{}'\n{}".format(row, e))

        if self.error_handler:
            row, self.error_accumulator = self.error_handler(row, self.error_accumulator)
        else:
            self.errors.append(self.error_accumulator)

        return row

    def __str__(self):
        from ambry.util import qualified_class_name

        col_codes = '\n'.join( '  {:2d} {:15s}: {}'.format(i,col, e) for i, (col,e) in enumerate(self.col_code))

        return (qualified_class_name(self) + "\n" +
                self.indent + "Code: "+ self.transform_code + "\n" +col_codes)
