"""
Functions to support multiprocessing

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

library = None  # Global library object
import atexit
import os

def _MPBundleMethod(f, args):
    """
    Decorator implementation for capturing exceptions.


    :param f:
    :param args:
    :return:
    """
    import traceback

    args = list(args)
    bundle_vid = args[0]

    try:
        library.database.close()
        b = library.bundle(bundle_vid)
        b = b.cast_to_subclass()
        b.multi = True  # In parent it is a number, in child, just needs to be true to get the right logger template
        b.is_subprocess = True
        b.limited_run = bool(int(os.getenv('AMBRY_LIMITED_RUN', 0)))
        assert b._progress == None # Don't want to share connections across processes
        args[0] = b
    except:
        print('Exception in fetching bundle in multiprocessing run.')
        traceback.print_exc()
        raise

    try:
        return f(args)

    except Exception as e:

        tb = traceback.format_exc()
        print tb
        b.error('Subprocess {} raised an exception: {}'.format(os.getpid(), e), False)
        b.error(tb, False)

        raise
    finally:
        library.database.close()


def MPBundleMethod(f, *args, **kwargs):
    """Decorator to capture exceptions. and logging the error"""
    from decorator import decorator

    return decorator(_MPBundleMethod, f)  # Preserves signature

def alt_init(l):
    global library
    library = l

def init_library(database_dsn, accounts_password, limited_run = False):
    """Child initializer, setup in Library.process_pool"""
    global library
    from ambry.library import new_library
    from ambry.run import get_runconfig

    import os
    import signal

    #atexit.register(concurrent_at_exit)

    # Have the child processes ignore the keyboard interrupt, and other signals. Instead, the parent will
    # catch these, and clean up the children.
    #signal.signal(signal.SIGINT, signal.SIG_IGN)

    #signal.signal(signal.SIGTERM, sigterm_handler)

    os.environ['AMBRY_DB'] = database_dsn
    os.environ['AMBRY_PASSWORD'] = accounts_password
    os.environ['AMBRY_LIMITED_RUN'] = '1' if limited_run else '0'

    library = new_library(get_runconfig())

@MPBundleMethod
def build_mp(args):
    """Ingest a source, using only arguments that can be pickled, for multiprocessing access"""

    b, stage, source_name, force = args

    source = b.source(source_name)

    with b.progress.start('build_mp',stage,message="MP build", source=source) as ps:
        r = b.build_source(stage, source, ps, force)

    b.close()

    return r

@MPBundleMethod
def unify_mp(args):
    """Unify all of the segment partitions for a parent partition, then run stats on the MPR file"""

    b, partition_name = args

    with b.progress.start('coalesce_mp',0,message="MP coalesce {}".format(partition_name)) as ps:
        r = b.unify_partition(partition_name, None, ps)

    b.close()

    return r



@MPBundleMethod
def ingest_mp(args):
    """Ingest a source, using only arguments that can be pickled, for multiprocessing access"""

    b, stage, source_name, clean_files = args

    source = b.source(source_name)

    with b.progress.start('ingest_mp',0,message="MP ingestion", source=source) as ps:
        r =  b._ingest_source(source, ps, clean_files)

    b.close()

    return r
