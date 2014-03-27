"""Dedicated interface to the Files table of the library.
"""

# Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt


from ..orm import File
from sqlalchemy.sql import or_
from ..util import Constant
from ..identity import LocationRef
import os

class Files(object):


    TYPE = Constant()
    TYPE.BUNDLE = LocationRef.LOCATION.LIBRARY
    TYPE.PARTITION = LocationRef.LOCATION.PARTITION
    TYPE.SOURCE = LocationRef.LOCATION.SOURCE
    TYPE.SREPO = LocationRef.LOCATION.SREPO
    TYPE.UPSTREAM = LocationRef.LOCATION.UPSTREAM
    TYPE.REMOTE = LocationRef.LOCATION.REMOTE


    def __init__(self, db, query=None):

        self.db = db

        self._query = query


    @property
    def all(self):
        '''Return all records that match the internal query'''
        return self._query.all()

    @property
    def first(self):
        '''Return the first record that matched the internal query'''
        return self._query.first()


    @property
    def one(self):
        '''Return the first record that matched the internal query,
        with the expectation that there is only one'''
        return self._query.one()


    @property
    def one_maybe(self):
        '''Return the first record that matched the internal query,
        with the expectation that there is only one'''
        from sqlalchemy.orm.exc import NoResultFound

        try:
            return self._query.one()
        except NoResultFound:
            return None

    def delete(self):
        '''Delete all of the records in the query'''
        return self._query.delete()

    @property
    def query(self):
        return Files(self.db, self.db.session.query(File))

    #
    # Filters
    #

    def _check_query(self):
        if not self._query:
            from ..dbexceptions import ObjectStateError
            raise ObjectStateError("Must use query() before filter methods")

    def ref(self, v):
        self._check_query()
        self._query = self._query.filter(File.ref == v)
        return self


    def state(self, v):
        self._check_query()
        self._query = self._query.filter(File.state == v)
        return self

    def path(self, v):
        self._check_query()
        self._query = self._query.filter(File.path == v)
        return self

    def type(self, v):
        self._check_query()
        self._query = self._query.filter(File.type_ == v)
        return self

    def group(self, v):
        self._check_query()
        self._query = self._query.filter(File.group == v)
        return self


    #
    # pre-defined type filters
    #

    @property
    def installed(self):
        self._check_query()
        self._query = self._query.filter( or_ (File.type_ == self.TYPE.BUNDLE,
                                         File.type_ == self.TYPE.PARTITION))
        return self

    def new_file(self, merge=False, **kwargs):

        if merge:
            f = File(**kwargs)
            self.merge(f)
            return f
        else:
            return File(**kwargs)


    def merge(self, f):
        from sqlalchemy.exc import IntegrityError

        s = self.db.session

        path = f.path

        if os.path.exists(path):
            stat = os.stat(path)
            f.modified = int(stat.st_mtime)
            f.size = stat.st_size
        else:
            f.modified = None
            f.size = None



        # Sqlalchemy doesn't automatically rollback on exceptions, and you
        # can't re-try the commit until you roll back.
        try:
            s.add(f)
            self.db.commit()
        except IntegrityError as e:
            self.db.rollback()
            s.merge(f)
            self.db.commit()

        self.db._mark_update()


#============================

def add_remote_file(self, identity):
    self.add_file(identity.cache_key, 'remote', identity.vid, state='remote')


def add_file(self,
             path,
             group,
             ref,
             state='new',
             type_='bundle',
             data=None,
             source_url=None,
             hash=None
):
    from ambry.orm import File

    assert type_ is not None

    self.merge_file(File(path=path,
                         group=group,
                         ref=ref,
                         state=state,
                         type_=type_,
                         data=data,
                         source_url=source_url,
                         hash=hash
    ))

    self._mark_update()


def get_file_by_state(self, state, type_=None):
    """Return all files in the database with the given state"""
    from ambry.orm import File

    s = self.session

    # The orderby clause should put bundles before partitions, which is
    # required to install correctly.

    if state == 'all':
        q = s.query(File).order_by(File.ref)
    else:
        q = s.query(File).filter(File.state == state).order_by(File.ref)

    if type_:
        q = q.filter(File.type_ == type_)

    return q.all()


def get_file_by_ref(self, ref, type_=None, state=None):
    """Return all files in the database with the given state"""
    from ambry.orm import File
    from sqlalchemy.orm.exc import NoResultFound

    s = self.session

    try:
        q = s.query(File).filter(File.ref == ref)

        if type_ is not None:
            q = q.filter(File.type_ == type_)

        if state is not None:
            q = q.filter(File.state == state)

        return q.all()

    except NoResultFound:
        return None


def get_file_by_type(self, type_=None):
    """Return all files in the database with the given state"""
    from ambry.orm import File
    from sqlalchemy.orm.exc import NoResultFound

    s = self.session

    try:
        return s.query(File).filter(File.type_ == type_).all()

    except NoResultFound:
        return None


def get_file_by_path(self, path):
    """Return all files in the database with the given state"""
    from ambry.orm import File
    from sqlalchemy.orm.exc import NoResultFound

    s = self.session

    try:
        return s.query(File).filter(File.path == path).one()

    except NoResultFound:
        return None


def remove_file(self, ref, type_=None, state=None):
    from ambry.orm import File
    from sqlalchemy.orm.exc import NoResultFound

    s = self.session

    try:
        q = s.query(File).filter(File.ref == ref)

        if type_ is not None:
            q = q.filter(File.type_ == type_)

        if state is not None:
            q = q.filter(File.state == state)

        return q.delete()

    except NoResultFound:
        return None


