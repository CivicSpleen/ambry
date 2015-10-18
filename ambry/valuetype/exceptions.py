""" Value Type Exceptions

Functions for handling exceptions

"""

def try_except( v, row, try_f, except_f):
    """Takes 2 closures and executes them in a try / except block """
    try:
        return try_f()
    except:
        return except_f()


def ignore(v, row, bundle):

    return  v


def print_errors(errors):
    print 'XXX', errors

