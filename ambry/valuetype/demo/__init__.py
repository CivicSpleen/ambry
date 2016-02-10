"""Value Types for demographic values, like race / ethnicity

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from .. import ValueType, StrValue

class RECode(StrValue):
    """Race Ethnicity Code """

    __datatype__ = str

    code_map = None

    def __new__(cls, bundle, v):
        o = super(StrValue, cls).__new__(cls, cls.parse(v))
        return o

    def __init__(self, bundle, v):
        """

        :param bundle: Passed in so the object can get to the library
        :param v:
        :return:
        """
        pass






