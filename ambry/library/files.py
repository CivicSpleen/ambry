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
    TYPE.REMOTEPARTITION = LocationRef.LOCATION.REMOTEPARTITION


    def __init__(self, db, query=None):

        self.db = db

        self._query = query

        self._collection = []

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
            self.db.commit()

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

    def new_file(self, merge=False, commit = True, **kwargs):
        """
        If merge is 'collect', the files will be added to the collection, for later
        insertion.
        """

        if merge:
            f = File(**kwargs)
            self.merge(f, commit=commit)
            return f
        else:
            return File(**kwargs)

    def insert_collection(self):

        if len(self._collection) == 0:
            return

        self.db.session.execute(File.__table__.insert(), self._collection)

        self._collection = []

    def merge(self, f, commit = True):
        '''If commit is 'collect' add the files to the collectoin for later insertion. '''
        from sqlalchemy.exc import IntegrityError

        s = self.db.session

        path = f.path

        if True:
            if os.path.exists(path):
                stat = os.stat(path)

                if not f.modified or stat.st_mtime > f.modified:
                    f.modified = int(stat.st_mtime)

                f.size = stat.st_size
            else:
                f.modified = f.modified if f.modified else None
                f.size = f.size if f.size else None

        if commit == 'collect':
            self._collection.append(f.insertable_dict)
            return

        # Sqlalchemy doesn't automatically rollback on exceptions, and you
        # can't re-try the commit until you roll back.
        try:
            s.add(f)
            if commit:
                self.db.commit()

        except IntegrityError as e:
            s.rollback()

            s.merge(f)
            try:
                self.db.commit()
            except IntegrityError as e:
                s.rollback()
                pass


        self.db._mark_update()

    def install_bundle_file(self, bundle, cache, commit=True, state='installed'):
        """Mark a bundle file as having been installed in the library"""

        ident = bundle.identity

        if self.query.group(cache.repo_id).type(Files.TYPE.BUNDLE).path(bundle.database.path).one_maybe:
            return False

        return self.new_file(
            commit=commit,
            merge=True,
            path=bundle.database.path,
            group=cache.repo_id,
            ref=ident.vid,
            state=state,
            type_=Files.TYPE.BUNDLE,
            data=None,
            source_url=None)


    def install_partition_file(self, partition, cache, commit=True, state='installed'):
        """Mark a partition file as having been installed in the library

        """

        if self.query.group(cache.repo_id).type(Files.TYPE.PARTITION).path(partition.database.path).one_maybe:
            return False

        ident = partition.identity

        return self.new_file(
            commit=commit,
            merge=True,
            path=partition.database.path,
            group=cache.repo_id,
            ref=ident.vid,
            state=state,
            type_=Files.TYPE.PARTITION,
            data=None,
            source_url=None)


    def install_remote_bundle(self, ident, upstream, metadata, commit=True):
        """Set a reference to a remote bundle"""

        return self.new_file(
            commit=commit,
            merge=True,
            path=ident.cache_key,
            group=upstream.repo_id,
            ref=ident.vid,
            state='installed',
            type_=Files.TYPE.REMOTE,
            data=metadata,
            hash=metadata.get('md5', None),
            priority=upstream.priority,
            source_url=upstream.repo_id )


    def install_remote_partition(self, ident, upstream, metadata, commit=True):
        """Set a reference to a remote partition"""


        return self.new_file(
            commit = commit,
            merge=True,
            path=ident.cache_key,
            group=upstream.repo_id,
            ref=ident.vid,
            state='installed',
            type_=Files.TYPE.REMOTEPARTITION,
            data=metadata,
            hash=metadata.get('md5', None),
            priority=upstream.priority,
            source_url=upstream.repo_id, )

    def install_bundle_source(self, bundle, source, commit=True):
        """Set a reference a bundle source"""

        return self.new_file(
            commit = commit,
            merge=True,
            path=bundle.bundle_dir,
            group=source.base_dir,
            ref=bundle.identity.vid,
            state='installed',
            type_=Files.TYPE.SOURCE,
            data=None,
            hash=None,
            priority=None,
            source_url=None, )

