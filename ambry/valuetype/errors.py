"""


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from core import *

class ErrorVT(ValueType):
    role = ROLE.ERROR
    vt_code = 'e'

    def __init__(self,v):
        pass


class MarginOfErrorVT(ValueType):
    role = ROLE.ERROR
    vt_code = 'e/m'

    def __init__(self,v):
        pass


class ConfidenceIntervalVT(ValueType):
    role = ROLE.ERROR
    vt_code = 'e/ci'

    def __init__(self,v):
        pass


class StandardErrorVT(ValueType):
    role = ROLE.ERROR
    vt_code = 'e/se'

    def __init__(self,v):
        pass


class RelativeStandardErrorVT(ValueType):
    role = ROLE.ERROR
    vt_code = 'e/rse'

    def __init__(self,v):
        pass
