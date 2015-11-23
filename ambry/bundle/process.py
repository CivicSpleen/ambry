""" Accessor for creating progress records, which show the history of the building of
a bundle.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import os
import platform
from ambry.orm import Process
from six import string_types

class ProgressSection(object):
    """A handler of the records for a single routine or phase"""

    def __init__(self, parent, session, **kwargs):
        self._parent = parent
        self._pid = os.getpid()
        self._hostname = platform.node()

        self._session = session

        self.add(log_action='start',**kwargs)

    def __del__(self):
        self._session.close()

    def augment_args(self,args, kwargs):
        for arg in args:
            if isinstance(arg, string_types):
                kwargs['message'] = arg


    def add(self, *args, **kwargs):
        """Add a new record to the section"""

        self.augment_args(args, kwargs)

        kwargs['pid'] = self._pid
        kwargs['d_vid'] = self._parent._d_vid
        kwargs['hostname'] = self._hostname
        kwargs['log_action'] = kwargs.get('log_action', 'add')
        self.rec = Process(**kwargs)
        self._session.add(self.rec)
        self._session.commit()

    def update(self, *args, **kwargs):
        """Update the last section record"""

        self.augment_args(args, kwargs)

        kwargs['pid'] = self._pid
        kwargs['d_vid'] = self._parent._d_vid
        kwargs['hostname'] = self._hostname
        kwargs['log_action'] = kwargs.get('log_action', 'update')

        if not self.rec:
            return self.add(**kwargs)
        else:
            for k, v in kwargs.items():
                setattr(self.rec, k, v)

            self._session.merge(self.rec)
            self._session.commit()


class ProcessLogger(object):

    def __init__(self, dataset):

        self._vid = dataset.vid
        self._db = dataset._database
        self._d_vid = dataset.vid

        self._connection = self._db.engine.connect()

        self.Session = self._db.Session

    def __del__(self):
        if self._connection:
            self._connection.close()

    def dataset(self):
        from ambry.orm import Dataset
        return self._session.query(Dataset).filter(Dataset.vid == self._d_vid ).one()

    def _new_session(self):
        return self._db.alt_session()

    def start(self, **kwargs):
        """Start a new routine, stage or phase"""
        return ProgressSection(self, self.Session(bind=self._connection), **kwargs)

    def context(self, f, interval=2):
        """Context manager to start and stop context logging.

        :param f: A function to call. Returns either a string, or a tuple (format_string, format_args)
        :param interval: Frequency to call the function, in seconds.
        :return:

        """

        bundle = self

        class _ProgressLogger(object):

            def __enter__(self):
                bundle.start_progress_logging(f, interval)

            def __exit__(self, exc_type, exc_val, exc_tb):

                bundle.stop_progress_logging()

                if exc_val:
                    return False
                else:
                    return True

        return _ProgressLogger()

    def start_progress_logging(self, f, interval=2):
        """
        Call the function ``f`` every ``interval`` seconds to produce a logging message to be passed
        to self.log().

        NOTE: This may cause problems with IO operations:

            When a signal arrives during an I/O operation, it is possible that the I/O operation raises an exception
            after the signal handler returns. This is dependent on the underlying Unix system's
            semantics regarding interrupted system calls.

        :param f: A function to call. Returns either a string, or a tuple (format_string, format_args)
        :param interval: Frequence to call the function, in seconds.
        :return:
        """

        import signal

        def handler(signum, frame):

            r = f()

            if isinstance(r, (tuple, list)):
                try:
                    self.log(r[0].format(*r[1]))
                except IndexError:
                    self.log(str(r) + ' (Bad log format)')  # Well, at least log something
            else:
                self.log(str(r) + ' ' + str(type(r)))

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
