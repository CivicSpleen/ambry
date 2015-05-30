"""Support functions for transforming rows read from input files before writing
to databases.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""
import textwrap


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


class CensusTransform(BasicTransform):

    """Transformation that condsiders the special codes that the Census data
    may have in integer fields."""

    @staticmethod
    def census_defaults(v, column, default, f):
        """Basic defaults method, using only the column default and
        illegal_value parameters.

        WIll also convert blanks and None to the default

        """
        if v is None:
            return default
        elif v == '':
            return default
        elif column.illegal_value and str(v) == str(column.illegal_value):
            return default
        elif isinstance(v, basestring) and v.startswith('!'):
            return -2
        elif isinstance(v, basestring) and v.startswith('#'):
            return -3
        else:
            return f(v)

    def __init__(self, column, useIndex=False):
        """A Transform that is designed for the US Census, converting codes
        that apear in Integer fields. The geography data dictionary in.

            http://www.census.gov/prod/cen2000/doc/sf1.pdf


        Assignment of codes of nine (9) indicates a balance record or that
        the entity or attribute does not exist for this record.

        Assignment of pound signs (#) indicates that more than one value exists for
        this field and, thus, no specific value can be assigned.

        Assignment of exclamation marks (!) indicates that this value has not yet
        been determined or this file.

        This transform makes these conversions:

            The Column's illegal_value becomes -1
            '!' becomes -2
            #* becomes -3

        Args:
            column an orm.Column
            useIndex if True, access the column value in the row by index, not name

        """
        self.column = column

        # for numbers try to coerce to an integer. We'd have to use a helper func
        # with a try/catch, except in this case, integers are always all digits
        # here
        if column.datatype == 'integer' or str(column.datatype) == 'integer64':
            msg = column.name
            f = lambda v, msg = msg: coerce_int_except(v, msg)
        elif column.datatype == 'real' or column.datatype == 'float':
            msg = column.name
            f = lambda v, msg = msg: coerce_float_except(v, msg)
        else:

            # This answer claims that the files are encoded in IBM850, but for the 2000
            # census, latin1 seems to work correctly.
            # http://stackoverflow.com/questions/2477360/character-encoding-for-us-census-cartographic-boundary-files

            # Unicode, et al, is $#^#% horrible, so we're punting and using XML encoding,
            # which we will claim is to make the name appear correctly in web
            # pages.
            f = lambda v: v.strip().decode('latin1').encode('ascii','xmlcharrefreplace')

        if column.default and column.default.strip():
            if column.datatype == 'text' or column.datatype == 'varchar':
                default = column.default
            elif column.datatype == 'real' or column.datatype == 'float':
                default = float(column.default)
            elif column.datatype == 'integer' or str(column.datatype) == 'integer64':
                default = int(column.default)

            else:
                raise ValueError('Unknown column datatype: ' + column.datatype)
        else:
            default = None

        f = (
            lambda v,column=column,f=f,default=default,
            defaults_f=self.census_defaults: defaults_f(v,column,default,f))

        # Strip test values, but not numbers
        f = lambda v, f=f: f(v.strip()) if isinstance(v, basestring) else f(v)

        # Extract the value from a position in the row
        if useIndex:
            f = lambda row, column=column, f=f: f(row[column.sequence_id - 1])
        else:
            f = lambda row, column=column, f=f: f(row[column.name])

        self.f = f

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


def parse_int(name, v, type_=int):
    """Parse as an integer, or a subclass of Int."""
    try:
        if is_nothing(v):
            return None
        else:
            return int(round(float(v), 0))
    except ValueError:
        raise CastingError(
            name,
            v,
            "Can't cast '{}' to {} in field '{}' ".format(
                v,
                type_,
                name))


def parse_type(type_, name, v):

    try:
        if is_nothing(v):
            return None
        else:

            return type_(v)
    except TypeError:
        raise CastingError(
            name,
            v,
            "Can't cast '{}' to {} in field '{}' ".format(
                v,
                type_,
                name))
    except ValueError:
        raise CastingError(
            name,
            v,
            "Can't cast '{}' to {} in field '{}' ".format(
                v,
                type_,
                name))


def parse_date(name, v):
    import dateutil.parser as dp
    import datetime

    if is_nothing(v):
        return None
    elif isinstance(v, basestring):
        try:
            return dp.parse(v).date()
        except ValueError as e:
            raise CastingError(
                name,
                v,
                "Failed to parse time for value '{}': {}".format(
                    v,
                    e.message))
        except TypeError as e:
            raise CastingError(
                name,
                v,
                "Failed to parse time for value '{}': {}".format(
                    v,
                    e.message))
    elif isinstance(v, datetime.date):
        return v
    else:
        raise CastingError(
            name,
            v,
            "Expected datetime.date or basestring, got '{}'".format(
                type(v)))


def parse_time(name, v):
    import dateutil.parser as dp
    import datetime
    if is_nothing(v):
        return None
    elif isinstance(v, basestring):
        try:
            return dp.parse(v).time()
        except ValueError as e:
            raise CastingError(
                name,
                v,
                "Failed to parse time for value '{}': {}".format(
                    v,
                    e.message))
    elif isinstance(v, datetime.time):
        return v
    else:
        raise CastingError(name, v,"Expected datetime.time or basestring, got '{}'".format(type(v)))


def parse_datetime(name, v):
    import dateutil.parser as dp
    import datetime
    if is_nothing(v):
        return None
    elif isinstance(v, basestring):
        try:
            return dp.parse(v)
        except ValueError as e:
            raise CastingError(name, v,"Failed to parse time for value '{}': {}".format(
                    v,e.message))
        except TypeError as e:
            raise CastingError(name,v,"Failed to parse time for value '{}': {}".format(
                    v,e.message))
    elif isinstance(v, datetime.datetime):
        return v
    else:
        raise CastingError(
            name, v, "Expected datetime.datetime or basestring, got " + str(type(v)))


class CasterTransformBuilder(object):

    def __init__(self, env=None):
        self.types = []
        self._compiled = None
        self.custom_types = {}

        self.dict_code = None

    def append(self, name, type_):
        self.types.append((name, type_))

    def add_type(self, t):
        self.custom_types[t.__name__] = t

    def makeDictTransform(self):
        import uuid
        import datetime

        f_name = "dict_transform_" + str(uuid.uuid4()).replace('-', '')
        # f_name_inner = "dict_transform_" + str(uuid.uuid4()).replace('-', '')

        c = []

        o = """def {}(row):
    
    import dateutil.parser as dp
    import datetime
    from ambry.transform import parse_date, parse_time, parse_datetime

    return {{""".format(f_name)

        for i, (name, type_) in enumerate(self.types):
            if i != 0:
                o += ',\n'

            if type_ == str:
                type_ = unicode

            if type_ == datetime.date:
                o += "'{name}':parse_date('{name}', row.get('{name}'))".format(name=name)
                c.append(
                    "'{name}':lambda v: parse_date('{name}', v)".format(
                        name=name))
            elif type_ == datetime.time:
                o += "'{name}':parse_time('{name}', row.get('{name}'))".format(name=name)
                c.append(
                    "'{name}':lambda v: parse_time('{name}', v)".format(
                        name=name))
            elif type_ == datetime.datetime:
                o += "'{name}':parse_datetime('{name}', row.get('{name}'))".format(
                    name=name)
                c.append(
                    "'{name}':lambda v:parse_datetime('{name}', v)".format(
                        name=name))
            elif type_ == int:
                o += "'{name}':parse_int('{name}', row.get('{name}'))".format(name=name)
                c.append(
                    "'{name}':lambda v:parse_int('{name}', v)".format(
                        name=name))
            else:
                o += "'{name}':parse_type({type},'{name}', row.get('{name}'))".format(
                    type=type_.__name__,
                    name=name)
                c.append(
                    "'{name}':lambda v:parse_type({type},'{name}', v)".format(
                        type=type_.__name__,
                        name=name))

        o += """}"""

        cf = "caster_funcs={" + ','.join(c) + "}"

        return f_name, o, cf

    def compile(self):
        # import uuid

        if not self._compiled:

            # lfn, lf = self.makeListTransform()
            # exec(lf)
            # lf = locals()[lfn]

            lf = None

            # Get the code in string form.
            dfn, df, cf = self.makeDictTransform()

            exec df

            df = locals()[dfn]

            exec cf
            cf = locals()['caster_funcs']

            self._compiled = (lf, df, cf)

        return self._compiled

    def _call_dict(self, f, row, codify_cast_errors):
        """Call the caster to cast all of the values in a row.

        If there are casting errors, through an exception, unless
        codify_cast_errors, in which case move the value with the
        casting error to a field that is suffixed with '_code'

        """

        for k, v in self.custom_types.items():
            globals()[k] = v

        if codify_cast_errors:

            d = {k.lower(): v for k, v in row.items() if k}

            try:
                return f[1](d), {}
            except CastingError as e:
                print e
                do = {}
                cast_errors = {}

                for k, v in d.items():
                    try:
                        do[k] = f[2][k](v)
                    except KeyError:
                        cast_errors[k] = v
                    except CastingError:

                        do[k + '_code'] = v
                        cast_errors[k] = v
                        do[k] = None
                return do, cast_errors
            except TypeError:
                raise
        else:
            return f[1]({k.lower(): v for k, v in row.items()}), {}

    def __call__(self, row, codify_cast_errors=True):
        from sqlalchemy.engine.result import RowProxy  # @UnresolvedImport

        f = self.compile()

        if isinstance(row, (dict, RowProxy)):
            return self._call_dict(f, row, codify_cast_errors)

        else:
            raise Exception("Unknown row type: {} ".format(type(row)))
