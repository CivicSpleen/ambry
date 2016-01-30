"""
Functions to support multiprocessing

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import os
from multiprocessing.pool import debug, MaybeEncodingError, Pool as MPPool, MapResult

#import multiprocessing, logging
#multiprocessing.DEFAULT_LOGGING_FORMAT = '%(process)d [%(levelname)s/%(processName)s] %(message)s'
#logger = multiprocessing.log_to_stderr()
#logger.setLevel(multiprocessing.SUBDEBUG)



def worker(inqueue, outqueue, initializer=None, initargs=(), maxtasks=None):
    """ Custom worker for bundle operations

    :param inqueue:
    :param outqueue:
    :param initializer:
    :param initargs:
    :param maxtasks:
    :return:
    """
    from ambry.library import new_library
    from ambry.run import get_runconfig
    import traceback

    assert maxtasks is None or (type(maxtasks) == int and maxtasks > 0)

    put = outqueue.put
    get = inqueue.get

    if hasattr(inqueue, '_writer'):
        inqueue._writer.close()
        outqueue._reader.close()

    if initializer is not None:
        initializer(*initargs)

    try:
        task = get()
    except (EOFError, IOError):
        debug('worker got EOFError or IOError -- exiting')
        return

    if task is None:
        debug('worker got sentinel -- exiting')
        return

    job, i, func, args, kwds = task

    # func = mapstar = map(*args)

    # Since there is only one source build per process, we know the structure
    # of the args beforehand.
    mp_func = args[0][0]
    mp_args = list(args[0][1][0])

    library = new_library(get_runconfig())
    library.database.close()  # Maybe it is still open after the fork.
    library.init_debug()

    bundle_vid = mp_args[0]

    try:

        b = library.bundle(bundle_vid)
        library.logger = b.logger # So library logs to the same file as the bundle.

        b = b.cast_to_subclass()
        b.multi = True  # In parent it is a number, in child, just needs to be true to get the right logger template
        b.is_subprocess = True
        b.limited_run = bool(int(os.getenv('AMBRY_LIMITED_RUN', 0)))

        assert b._progress == None  # Don't want to share connections across processes

        mp_args[0] = b
        result = (True, [mp_func(*mp_args)])

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        b.error('Subprocess {} raised an exception: {}'.format(os.getpid(), e.message), False)
        b.error(tb, False)
        result = (False, e)

    assert result

    b.progress.close()
    library.close()

    try:
        put((job, i, result))
    except Exception as e:
        wrapped = MaybeEncodingError(e, result[1])
        debug("Possible encoding error while sending result: %s" % (wrapped))
        put((job, i, (False, wrapped)))


def add_bundle_to_args(library, mp_args):
    return mp_args


class Pool(MPPool):
    """A version of Pool that allows defining the worker function"""

    def __init__(self, library, processes=None, initializer=None, initargs=(), maxtasksperchild=None, worker_f=worker):
        self._worker_f = worker_f
        self._library = library # Parent library, not the same instance as the one in worker()
        super(Pool, self).__init__(processes, initializer, initargs, maxtasksperchild)


    def _repopulate_pool(self):
        """Bring the number of pool processes up to the specified number,
        for use after reaping workers which have exited.
        """
        for i in range(self._processes - len(self._pool)):
            w = self.Process(target=self._worker_f,
                             args=(self._inqueue, self._outqueue,
                                   self._initializer,
                                   self._initargs, self._maxtasksperchild))
            self._pool.append(w)
            w.name = w.name.replace('Process', 'PoolWorker')
            w.daemon = True
            w.start()
            debug('added worker')

def alt_init(l): # For testing
    global library
    library = l

def init_library(database_dsn, accounts_password, limited_run = False):
    """Child initializer, setup in Library.process_pool"""

    import os
    import signal

    # Have the child processes ignore the keyboard interrupt, and other signals. Instead, the parent will
    # catch these, and clean up the children.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    #signal.signal(signal.SIGTERM, sigterm_handler)

    os.environ['AMBRY_DB'] = database_dsn
    os.environ['AMBRY_PASSWORD'] = accounts_password
    os.environ['AMBRY_LIMITED_RUN'] = '1' if limited_run else '0'


def build_mp(b, stage, source_name, force):
    """Ingest a source, using only arguments that can be pickled, for multiprocessing access"""

    source = b.source(source_name)

    with b.progress.start('build_mp',stage,message="MP build", source=source) as ps:
        ps.add(message='Running source {}'.format(source.name), source=source, state='running')
        r = b.build_source(stage, source, ps, force)

    return r


def unify_mp(b, partition_name):
    """Unify all of the segment partitions for a parent partition, then run stats on the MPR file"""

    with b.progress.start('coalesce_mp',0,message="MP coalesce {}".format(partition_name)) as ps:
        r = b.unify_partition(partition_name, None, ps)

    return r


def ingest_mp(b, stage, source_name, clean_files):
    """Ingest a source, using only arguments that can be pickled, for multiprocessing access"""

    source = b.source(source_name)

    with b.progress.start('ingest_mp',0,message="MP ingestion", source=source) as ps:
        r =  b._ingest_source(source, ps, clean_files)

    return r
