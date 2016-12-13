""" Value Type Exceptions

Functions for handling exceptions

"""

import textwrap
from ambry.dbexceptions import BuildError

def clear_error(v):
    from ambry.valuetype import FailedValue

    if isinstance(v, FailedValue):
        return None
    return v


class CastingError(TypeError):

    def __init__(self, type_target, field_header, value, message, *args, **kwargs):

        self.type_target = type_target
        self.field_header = field_header
        self.value = value

        message = "Failed to cast column '{}' value='{}' to '{}': {} "\
            .format(field_header, value, type_target, message)

        # Call the base class constructor with the parameters it needs
        Exception.__init__(self, textwrap.fill(message, 120), *args, **kwargs)

class TooManyCastingErrors(BuildError):
    pass


def try_except( try_f, except_f):
    """Takes 2 closures and executes them in a try / except block """
    try:
        return try_f()
    except Exception as exception:
        return except_f(exception)

nan_value = float('nan')

def nan_is_none(v):
    import math

    try:
        if math.isnan(v):
            return None
        else:
            return v
    except (ValueError, TypeError):
        return v

def ignore(v):
    return None

def log_exception(exception, bundle):
    bundle.error(exception)
    return None

