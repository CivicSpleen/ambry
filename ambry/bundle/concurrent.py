"""
Functions to support multiprocessing

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

library = None # GLobal library object


def _MPBundleMethod(f, args):
    """
    Decorator implementation for capturing exceptions.


    :param f:
    :param args:
    :return:
    """
    import traceback, os
    import signal

    # Have the child processes ignore the keyboard interrupt, and other signals. Instead, the parent will
    # catch these, and clean up the children.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    args = list(args)
    bundle_vid = args[0]

    try:

        b = library.bundle(bundle_vid)
        b = b.cast_to_subclass()
        b.multi = True  # In parent it is a number, in child, just needs to be true to get the right logger template
        b.is_subprocess = True
        args[0] = b
    except:
        print "Exception in feching bundle in multiprocessing run."
        traceback.print_exc()
        raise

    try:
        return f(args)
    except Exception as e:
        import sys

        tb = traceback.format_exc()
        b.error("Subprocess raised an exception: {}".format(e), False)
        b.error(tb, False)

        raise

def MPBundleMethod(f, *args, **kwargs):
    """Decorator to capture exceptions. and logging the error"""
    from decorator import decorator

    return decorator(_MPBundleMethod, f)  # Preserves signature

def alt_init(l):
    global library
    library = l

def init_library(rc_path, database_dsn=None):
    """Child initializer, setup in Library.process_pool"""
    global library
    from ambry.run import get_runconfig
    from ..library import new_library

    rc = get_runconfig(rc_path)
    library = new_library(rc, database_dsn)

@MPBundleMethod
def build_mp(args):
    """Ingest a source, using only arguments that can be pickled, for multiprocessing access"""
    b, source_name, force = args

    source = b.source(source_name)

    return b.build_source(source, force)

@MPBundleMethod
def ingest_mp(args):
    """Ingest a source, using only arguments that can be pickled, for multiprocessing access"""

    b, source_name, clean_files = args

    source = b.source(source_name)

    return b.ingest_source(source, clean_files)
