"""Value Types

The value converters can recognize, parse, normalize and transform common codes, such as FIPS, ANSI and census codes.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""
from six import text_type
from datetime import date, time, datetime

from ambry.util import Constant

from core import *
from times import *
from geo import *
from dimensions import *
from measures import *
from errors import *

value_types = {
    "int": IntValue,
    "str": TextValue,
    "float": FloatValue,
    "d/int": IntValue,
    "d/str": TextValue,
    "d/float": FloatValue,
    "k": KeyVT,
    "i": IdentifierVT,
    "d": DimensionVT,
    "d/N": NominalVT,
    "d/C": CategoricalVT,
    "m/int": IntValue,
    "m/str": TextValue,
    "m/float": FloatValue,
    "m": MeasureVT,
    "m/count": CountVT,
    "m/pro": ProportionVT,
    "m/pct": PercentageVT,
    "m/I": IntervalVT,
    "m/R": RatiometricVT,

}

value_types.update(geo_value_types)
value_types.update(times_value_types)
value_types.update(dimension_value_types)
value_types.update(error_value_types)

def resolve_value_type(vt_code):

    vt_code = vt_code.strip('?')

    try:
        return value_types[vt_code]
    except KeyError:

        parts = vt_code.split('/')
        args = []
        while len(parts):
            args.append(parts.pop())

            try:
                o = value_types['/'.join(parts)]
                # Return a dynamic subclass that has the extra parameters built in
                return type(vt_code.replace('/','_'), (o,),{'vt_code':vt_code})
            except KeyError:
                pass

