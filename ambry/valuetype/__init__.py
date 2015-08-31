"""Value Types

The value converters can recognize, parse, normalize and transform common codes, such as FIPS, ANSI and census codes.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

def import_valuetype(name):
    import importlib
    full_qual = 'ambry.valuetype.' + name
    path, cls_name = full_qual.rsplit('.', 1)
    mod = importlib.import_module(path)
    cls = getattr(mod, cls_name)

    return cls

def python_type(name):
    return import_valuetype(name).__pythontype__


class ValueType(object):


    @classmethod
    def intuit_name(self, name):
        """Return a numeric value in the range [-1,1), indicating the likelihood that the name is for a valuable of
        of this type. -1 indicates a strong non-match, 1 indicates a strong match, and 0 indicates uncertainty. """
        raise NotImplementedError()

    @classmethod
    def parse(cls, v):
        """Parse a value of this type and return a list of parsed values"""

        return v


class StrValue(str,ValueType):

    __pythontype__ = str

    def __new__(cls, v):
        o = super(StrValue, cls).__new__(cls, cls.parse(v))
        return o

class IntValue(int,ValueType):
    __pythontype__ = int

    def __new__(cls, v):
        o = super(IntValue, cls).__new__(cls, cls.parse(v))
        return o

class FloatValue(float,ValueType):
    __pythontype__ = float

    def __new__(cls, v):
        o = super(FloatValue, cls).__new__(cls, cls.parse(v))
        return o


class RegEx(StrValue):
    pass