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

        self.rec = None

        self._ai_rec_id = None # record for add_update

        self._group = None
        self._start = None
        self._group = self.add(log_action='start',state='running', **kwargs)



        assert self._session

    def __enter__(self):
        assert self._session
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        from ambry.util import qualified_name
        import traceback
        assert self._session

        if exc_val:
            self.add(
                message = str(exc_val),
                exception_class = qualified_name(exc_type),
                exception_trace = str(traceback.format_exc(exc_tb)),

            )
            self.done("Failed in context with exception")
            return False
        else:
            self.done("Successful context exit")
            return True

    @property
    def start(self):
        from sqlalchemy.orm.exc import NoResultFound

        if not self._start:
            try:
                self._start =  self._session.query(Process).filter(Process.id == self._group).one()
            except NoResultFound:
                self._start = None

        return self._start

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

        if self.start and self.start.state == 'done':
            raise ProgressLoggingError("Can't add -- process section is done")

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

    def update_done(self, *args, **kwargs):
        """Clear out the previous update"""
        kwargs['state'] = 'done'
        self.update(*args, **kwargs)
        self.rec = None


    def done(self, *args, **kwargs):


        kwargs['state'] = 'done'
        pr_id = self.add(*args, log_action='done', **kwargs)

        self._session.query(Process).filter(Process.group == self._group).update({Process.state: 'done'})
        self.start.state = 'done'

        return pr_id


class ProcessLogger(object):
    """Database connection and access object for recording build progress and build state"""

    def __init__(self, dataset, logger=None, new_connection=True):
        import os.path

        self._vid = dataset.vid
        self._d_vid = dataset.vid
        self._logger = logger
        self._buildstate = None
        self._new_connection = new_connection

        db = dataset._database
        schema = db._schema

        if db.driver == 'sqlite':
            # Create an entirely new database. Sqlite does not like concurrent access,
            # even from multiple connections in the same process.
            from ambry.orm import Database
            if db.dsn == 'sqlite://':
                # in memory database
                dsn = 'sqlite://'
            else:
                # create progress db near library db.
                parts = os.path.split(db.dsn)
                dsn = '/'.join(parts[:-1] + ('progress.db',))

            self._db = Database(dsn, foreign_keys=False)
            self._db.create()  # falls through if already exists
            self._engine = self._db.engine
            self._connection = self._db.connection
            self._session = self._db.session
            self._session.merge(dataset)
            self._session.commit()

        elif new_connection:  # For postgres, by default, create a new db connection
            # Make a new connection to the existing database
            self._db = db
            self._connection = self._db.engine.connect()
            self._session = self._db.Session(bind=self._connection, expire_on_commit=False)
        else:  # When not building, ok to use existing connection
            self._db = db
            self._connection = db.connection
            self._session = db.session

        if schema:
            self._session.execute('SET search_path TO {}'.format(schema))

    def __del__(self):
        if self._db.driver == 'sqlite':
            self._db.close()
        else:
            self.close()

    def close(self):

        if self._connection and self._new_connection:
            self._connection.close()

    @property
    def dataset(self):
        from ambry.orm import Dataset
        return self._session.query(Dataset).filter(Dataset.vid == self._d_vid).one()

    def start(self, phase, stage, **kwargs):
        """Start a new routine, stage or phase"""
        return ProgressSection(self, self._session, phase, stage, self._logger, **kwargs)

    @property
    def records(self):
        """Return all start records for this the dataset, grouped by the start record"""

        return (self._session.query(Process)
                .filter(Process.d_vid == self._d_vid)).all()

    @property
    def starts(self):
        """Return all start records for this the dataset, grouped by the start record"""

        return (self._session.query(Process)
                .filter(Process.d_vid == self._d_vid)
                .filter(Process.log_action == 'start')
                ).all()

    @property
    def query(self):
        """Return all start records for this the dataset, grouped by the start record"""

        return (self._session.query(Process).filter(Process.d_vid == self._d_vid))

    @property
    def exceptions(self):
        """Return all start records for this the dataset, grouped by the start record"""

        return (self._session.query(Process)
                .filter(Process.d_vid == self._d_vid)
                .filter(Process.exception_class != None)
                .order_by(Process.modified)).all()

    def clean(self):
        """Delete all of the records"""

        # Deleteing seems to be really weird and unrelable.
        (self._session.query(Process).filter(Process.d_vid == self._d_vid)
         ).delete(synchronize_session='fetch')

        for r in self.records:
            self._session.delete(r)

        self._session.commit()

    def commit(self):
        assert self._new_connection
        self._session.commit()

    @property
    def build(self):
        """Access build configuration values as attributes. See self.process
            for a usage example"""
        from ambry.orm.config import BuildConfigGroupAccessor

        # It is a lightweight object, so no need to cache
        return BuildConfigGroupAccessor(self.dataset, 'buildstate', self._session)


class CallInterval(object):
    """Call the inner callback at a limited frequency"""

    def __init__(self, f, freq,  **kwargs):
        import time
        self._f = f
        self._freq = freq
        self._next = time.time() + self._freq

        self._kwargs = kwargs

    def __call__(self, *args, **kwargs):
        import time

        if time.time() > self._next:
            kwargs.update(self._kwargs)
            self._f(*args, **kwargs)
            self._next = time.time() + self._freq


def call_interval(freq,**kwargs):
    """Decorator for the CallInterval wrapper"""
    def wrapper(f):
        return CallInterval(f, freq, **kwargs)

    return wrapper



