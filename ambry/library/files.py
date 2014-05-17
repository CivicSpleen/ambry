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


    def order(self, c):
        '''Return the first record that matched the internal query,
        with the expectation that there is only one'''
        self._query = self._query.order_by(c)

        return self

    def delete(self):
        '''Delete all of the records in the query'''

        if self._query.count() > 0:
            self._query.delete()

    def update(self,d):
        '''Delete all of the records in the query'''

        self._query.update(d)

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

    def new_file(self, merge=False, commit = True,  **kwargs):

        if merge:
            f = File(**kwargs)
            self.merge(f, commit=commit)
            return f
        else:
            return File(**kwargs)


    def merge(self, f, commit = True):
        from sqlalchemy.exc import IntegrityError

        s = self.db.session

        path = f.path

        if os.path.exists(path):
            stat = os.stat(path)

            if not f.modified or stat.st_mtime > f.modified:
                f.modified = int(stat.st_mtime)

            f.size = stat.st_size
        else:
            f.modified = None
            f.size = None


        # Sqlalchemy doesn't automatically rollback on exceptions, and you
        # can't re-try the commit until you roll back.
        try:
            s.add(f)
            if commit:
                self.db.commit()
        except IntegrityError as e:
            self.db.rollback()
            s.merge(f)
            self.db.commit()

        self.db._mark_update()

