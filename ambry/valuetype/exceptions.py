""" Value Type Exceptions

Functions for handling exceptions

"""

def try_except( try_f, except_f):
    """Takes 2 closures and executes them in a try / except block """
    try:
        return try_f()
    except Exception as exception:
        return except_f(exception)


def capture_error(v,  header_d,  errors):
    errors[header_d] = v
    return None

def error_from(errors, header):
    return errors.get(header)

def ignore(v):
    return  None

def log_exception(exception, bundle):
    bundle.error(exception)
    return None

