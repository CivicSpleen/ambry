"""Value Types

The value converters can recognize, parse, normalize and transform common codes, such as FIPS, ANSI and census codes.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

class ValueType(object):

    __datatype__ = int # Preferrerd datatype

    def __init__(self, bundle, library, group = None):

        self._bundle = bundle
        self._library = library
        self._group = group

        self._parsed = None # Parsed representation of the value

    def intuit_name(self, name):
        """Return a numeric value in the range [-1,1), indicating the likelyhood that the name is for a valuable of
        of this type. -1 indicates a strong non-match, 1 indicates a strong match, and 0 indicates uncertainty. """
        raise NotImplementedError()

    def intuit_value(self, v):
        """Return true if the input value could be a value of this type. """
        raise NotImplementedError()

    def parse(self, v):
        """Parse a value of this type and return a list of parsed values"""

        self._parsed = v
        return self



class RegEx(ValueType):
    __datatype__ = str  # Preferrerd datatype