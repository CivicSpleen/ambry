""" Accessor for creating progress records, which show the history of the building of
a bundle.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import os
import platform
from ambry.orm import Process
from six import string_types

class ProgressLoggingError(Exception):
    pass

class ProgressSection(object):
    """A handler of the records for a single routine or phase"""

    def __init__(self, parent, session, phase, stage, logger,  **kwargs):
        import signal

        self._parent = parent
        self._pid = os.getpid()
        self._hostname = platform.node()

        self._session = session
        self._logger = logger

        self._phase = phase
        self._stage = stage

        self._orig_alarm_handler = signal.SIG_DFL  # For start_progress_loggin

        self.rec = None

        self._ai_rec_id = None # record for add_update

        self._group = None
        self._group = self.add(log_action='start',state='running', **kwargs)

        assert self._session

    def __enter__(self):
        assert self._session
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        from ambry.util import qualified_name
        assert self._session
        if exc_val:
            self.add(
                message = str(exc_val),
                exception_class = qualified_name(exc_type),
                exception_trace = str(exc_tb),

            )
            self.done("Failed in context with exception")
            return False
        else:
            self.done("Successful context exit")
            return True

    def augment_args(self,args, kwargs):

        kwargs['pid'] = self._pid
        kwargs['d_vid'] = self._parent._d_vid
        kwargs['hostname'] = self._hostname
        kwargs['phase'] = self._phase
        kwargs['stage'] = self._stage
        kwargs['group'] = self._group

        if self._session is None:
            raise ProgressLoggingError("Progress logging section is already closed")

        for arg in args:
            if isinstance(arg, string_types):
                kwargs['message'] = arg

        # The sqlite driver has a seperate database, it can't deal with the objects
        # from a different database, so we convert them to vids. For postgres,
        # the pojects are in the same database, but they would have been attached to another
        # session, so we'd have to either detach them, or do the following anyway
        for table,vid in (('source','s_vid'), ('table','t_vid'),('partition','p_vid')):
            if table in kwargs:
                kwargs[vid] = kwargs[table].vid
                del kwargs[table]

    def add(self, *args, **kwargs):
        """Add a new record to the section"""

        self.augment_args(args, kwargs)

        kwargs['log_action'] = kwargs.get('log_action', 'add')

        rec = Process(**kwargs)

        self._session.add(rec)

        self.rec = rec

        if self._logger:
            self._logger.info(self.rec.log_str)
        self._session.commit()
        self._ai_rec_id = None

        return self.rec.id

    def update(self, *args, **kwargs):
        """Update the last section record"""

        self.augment_args(args, kwargs)

        kwargs['log_action'] = kwargs.get('log_action', 'update')

        if not self.rec:
            return self.add(**kwargs)
        else:
            for k, v in kwargs.items():

                # Don't update object; use whatever was set in the original record
                if k not in ('source','s_vid','table','t_vid','partition','p_vid'):
                    setattr(self.rec, k, v)

            self._session.merge(self.rec)
            if self._logger:
                self._logger.info(self.rec.log_str)
            self._session.commit()

            self._ai_rec_id = None
            return self.rec.id

    def add_update(self, *args, **kwargs):
        """A records is added, then on subsequent calls, updated"""

        if not self._ai_rec_id:
            self._ai_rec_id = self.add(*args, **kwargs)
        else:
            au_save = self._ai_rec_id
            self.update(*args,**kwargs)
            self._ai_rec_id = au_save

        return self._ai_rec_id

    def done(self, *args, **kwargs):

        start = self._session.query(Process).filter(Process.id == self._group).one()
        start.state = 'Done'

        pr_id = self.add(*args, log_action='done', **kwargs)

        return pr_id

class ProcessIntervals(object):

    def __init__(self, f, interval=2):
        """Context manager to start and stop context logging.

        :param f: A function to call. Returns either a string, or a tuple (format_string, format_args)
        :param interval: Frequency to call the function, in seconds.
        :return:

        """
        self._interval = interval
        self._interval_f = f
        self._orig_alarm_handler = None

    def __enter__(self):
        self.start_progress_logging(self._interval_f, self._interval)

    def __exit__(self, exc_type, exc_val, exc_tb):

        self.stop_progress_logging()

        if exc_val:
            return False
        else:
            return True

    def start_progress_logging(self, f, interval=2):
        """
        Call the function ``f`` every ``interval`` seconds to produce a logging message to be passed
        to self.log().

        NOTE: This may cause problems with IO operations:

            When a signal arrives during an I/O operation, it is possible that the I/O operation raises an exception
            after the signal handler returns. This is dependent on the underlying Unix system's
            semantics regarding interrupted system calls.

        :param f: A function to call. Returns either a string, or a tuple (format_string, format_args).
                  The function takes no args
        :param interval: Frequence to call the function, in seconds.
        :return:
        """

        import signal

        def handler(signum, frame):

            f()

            # Or, use signal.itimer()? Maybe, but this way, the handler will stop if there is
            # an exception, rather than getting regular exceptions.
            signal.alarm(interval)

        old_handler = signal.signal(signal.SIGALRM, handler)

        if not self._orig_alarm_handler:  # Only want the handlers from other outside sources
            self._orig_alarm_handler = old_handler

        signal.alarm(interval)

    def stop_progress_logging(self):
        """
        Stop progress logging by removing the Alarm signal handler and canceling the alarm.
        :return:
        """

        import signal

        if self._orig_alarm_handler:
            signal.signal(signal.SIGALRM, self._orig_alarm_handler)
            self._orig_alarm_handler = None

            signal.alarm(0)  # Cancel any currently active alarm.


class ProcessLogger(object):

    def __init__(self, dataset, logger = None):
        import signal
        import os.path

        self._vid = dataset.vid
        self._d_vid = dataset.vid
        self._logger = logger

        db = dataset._database
        schema = db._schema

        if db.driver == 'sqlite':
            # Create an entirely new database. Sqlite does not like concurrent access,
            # even from multiple connections in the same process.
            from ambry.orm import Database
            parts = os.path.split(db.dsn)
            dsn = '/'.join(parts[:-1]+('progress.db',))

            self._db = Database(dsn, foreign_keys=False)
            self._db.create() # falls through if already exists
            self._engine = self._db.engine
            self._connection = self._db.connection
            self._session = self._db.session

        else:
            # Make a new connection to the existing database
            self._db = db
            self._connection = self._db.engine.connect()
            self._session = self._db.Session(bind=self._connection)

        if schema:
            self._session.execute('SET search_path TO {}'.format(schema))

    def __del__(self):
        if self._db.driver == 'sqlite':
            self._db.close()
        else:
            if self._connection:
                self._connection.close()

    @property
    def dataset(self):
        from ambry.orm import Dataset
        return self._session.query(Dataset).filter(Dataset.vid == self._d_vid ).one()

    def start(self, phase, stage, **kwargs):
        """Start a new routine, stage or phase"""
        return ProgressSection(self, self._session, phase, stage, self._logger, **kwargs)

    def interval(self, func, interval=2):
        return ProcessIntervals(func, interval)

    @property
    def records(self):
        """Return all start records for this the dataset, grouped by the start record"""

        return (self._session.query(Process)
                .filter(Process.d_vid==self._d_vid)
                .filter(Process.log_action == 'start')
                ).all()
