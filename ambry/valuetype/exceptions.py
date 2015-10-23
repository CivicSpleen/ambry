""" Value Type Exceptions

Functions for handling exceptions

"""

import textwrap

class CastingError(TypeError):

    def __init__(self, type_target, field_header, value, message, *args, **kwargs):

        self.type_target = type_target
        self.field_header = field_header
        self.value = value

        message = "Failed to cast column '{}' value='{}' to '{}': {} "\
            .format(field_header, value, type_target, message)

        # Call the base class constructor with the parameters it needs
        Exception.__init__(self, textwrap.fill(message, 120), *args, **kwargs)


def try_except( try_f, except_f):
    """Takes 2 closures and executes them in a try / except block """
    try:
        return try_f()
    except Exception as exception:
        return except_f(exception)


def capture_code(v):
    v.code = v
    return None

def capture_error(v,  header_d,  errors):
    errors[header_d] = v
    return None

def error_from(header, errors):
    return errors.get(header)

def ignore_exception(v):
    return None

def log_exception(exception, bundle):
    bundle.error(exception)
    return None

