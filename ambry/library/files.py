"""Dedicated interface to the Files table of the library."""

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

    TYPE.MANIFEST = 'manifest'
    TYPE.DOC = 'doc'
    TYPE.EXTRACT = 'extract'
    TYPE.STORE = 'store'

    def __init__(self, db, query=None):

        self.db = db

        self._query = query

        self._collection = []

    @property
    def all(self):
        """Return all records that match the internal query."""
        return self._query.all()

    @property
    def first(self):
        """Return the first record that matched the internal query."""
        return self._query.first()

    @property
    def one(self):
        """Return the first record that matched the internal query, with the
        expectation that there is only one."""
        return self._query.one()

    @property
    def one_maybe(self):
        """Return the first record that matched the internal query, with the
        expectation that there is only one."""
        from sqlalchemy.orm.exc import NoResultFound

        try:
            return self._query.one()
        except NoResultFound:
            return None

    def order(self, c):
        """Return the first record that matched the internal query, with the
        expectation that there is only one."""
        self._query = self._query.order_by(c)

        return self

    def delete(self):
        """Delete all of the records in the query."""

        if self._query.count() > 0:
            self._query.delete()
            self.db.commit()

    def update(self, d):
        """Delete all of the records in the query."""

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

    def oid(self, v):
        self._check_query()
        self._query = self._query.filter(File.oid == v)
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

        if not v:
            return self

        if isinstance(v, basestring):
            self._query = self._query.filter(File.type_ == v)
        else:
            self._query = self._query.filter(File.type_.in_(v))

        return self


    def source_url(self, v):
        self._check_query()
        self._query = self._query.filter(File.source_url == v)
        return self

    #
    # pre-defined type filters
    #

    @property
    def installed(self):
        self._check_query()
        self._query = self._query.filter(or_(File.type_ == self.TYPE.BUNDLE, File.type_ == self.TYPE.PARTITION))
        return self

    def new_file(self,  commit = True, **kwargs):
        """If merge is 'collect', the files will be added to the collection,
        for later insertion."""
        from sqlalchemy.exc import IntegrityError

        if 'oid' in kwargs:
            del kwargs['oid']


        f = File(**kwargs)

        extant = self.query.ref(f.ref).type(f.type_).source_url(f.source_url).one_maybe

        if extant:
            for k, v in kwargs.items():
                setattr(extant, k, v)
                f = extant

        path = f.path

        if path and os.path.exists(path):
            stat = os.stat(path)

            if not f.modified or stat.st_mtime > f.modified:
                f.modified = int(stat.st_mtime)

            f.size = stat.st_size
        else:
            f.modified = f.modified if f.modified else None
            f.size = f.size if f.size else None

        # Debugging code. Kill on sight.
        #for i in self.db.session.identity_map.items():
        #    print i

        f = self.db.session.merge(f)

        if commit:
            self.db.commit()

        self.db._mark_update()


        return f



    def install_bundle_file(self,bundle,source,commit=True,state='installed'):
        """Mark a bundle file as having been installed in the library."""

        ident = bundle.identity

        return self.new_file(
            commit=commit,
            merge=True,
            path=bundle.database.path,
            ref=ident.vid,
            state=state,
            type_=Files.TYPE.BUNDLE,
            data=None,
            source_url=source)

    def install_partition_file(self,partition,source,commit=True,state='installed'):
        """Mark a partition file as having been installed in the library."""

        ident = partition.identity

        return self.new_file(
            commit=commit,
            merge=True,
            path=partition.database.path,
            ref=ident.vid,
            state=state,
            type_=Files.TYPE.PARTITION,
            data=None,
            source_url=source)


    def install_bundle_source(self, bundle, source, commit=True):
        """Set a reference a bundle source."""

        return self.new_file(
            commit=commit,
            merge=True,
            path=bundle.bundle_dir,
            ref=bundle.identity.vid,
            state=bundle.build_state,
            type_=Files.TYPE.SOURCE,
            data=None,
            hash=None,
            priority=None,
            source_url=None, )

    def install_data_store(self, w,name=None, title=None, summary=None,cache=None, url=None, commit=True):
        """A reference for a data store, such as a warehouse or a file
        store."""

        assert bool(w.dsn)
        assert bool(w.uid)



        kw = dict(commit=commit,
                  path=w.dsn,
                  ref=w.uid,
                  state=None,
                  type_=self.TYPE.STORE,
                  data=dict(
                      name=name,
                      title=title,
                      summary=summary,
                      cache=cache,
                      url=url,
                      db_class=w.database_class,

                  ),
                  source_url=w.dsn)

        f = self.new_file(**kw)

        return f

    def _process_source_content(self, path, source=None, content=None):
        """Install a file reference, possibly with binary content."""
        import os
        from ..util import md5_for_file
        import hashlib
        import time

        hash = None
        size = None
        modified = None

        if bool(path) and path.startswith('http'):

            import requests

            r = requests.get(path)

            r.raise_for_status()

            content = r.content

        elif bool(path):

            source = path

        if source:

            try:
                # Try source as file-like
                content = source.read()

                hash = hashlib.md5(content).hexdigest()
                size = len(content)
                modified = int(time.time())

            except (IOError, AttributeError):  # Nope, try it as a filename.

                with open(source) as sf:
                    content = sf.read()

                stat = os.stat(source)

                if not modified or stat.st_mtime > modified:
                    modified = int(stat.st_mtime)

                size = stat.st_size

                hash = md5_for_file(source)

        elif content:

            hash = hashlib.md5(content).hexdigest()
            size = len(content)
            modified = int(time.time())

        else:
            raise ValueError(
                "Must provied an existing path, source or content. ")

        if not content:
            raise ValueError(
                "Didn't get non-zero sized content from path , source or content")

        return dict(hash=hash, size=size, modified=modified, content=content)

    def install_manifest(self, manifest, warehouse=None, commit=True):
        """Store a references to, and content for, a manifest."""


        try:
            # manifest if a real Manifest object

            ref = manifest.uid
            path = manifest.path
            data = manifest.dict
            extra = (self._process_source_content(path))

        except AttributeError:
            # The manifest is actually a File object, from a warehouse database.
            ref = manifest.ref
            path = manifest.path
            data = manifest.data
            extra = (self._process_source_content(path))

        assert bool(ref)
        assert bool(path)

        f = self.new_file(ref=ref,
                          path=path,
                          type=self.TYPE.MANIFEST,
                          source_url=ref,
                          state=None,
                          data=data,
                          **extra)

        if warehouse:

            whf = self.query.path(warehouse.database.dsn).type(self.TYPE.STORE).one_maybe

            if not whf:
                from ..dbexceptions import NotFoundError
                raise NotFoundError(
                    "No record for database with dsn: '{}'".format(
                        warehouse.database.dsn))

            f.link_store(whf)
            whf.link_manifest(f)

            self.db.commit()

        return f

    def install_extract(self, path, source, d, commit=True):
        """Create a references for an extract.

        The extract hasn't been extracted yet, so there is no content,
        hash, etc.

        """

        f = self.query.path(path).type(self.TYPE.EXTRACT).one_maybe

        if f:
            # This interacts with marking it 'deletable' in install_manifest
            f.state = 'installed'
            self.merge(f)
            return f

        return self.new_file(
            commit=commit,
            merge=True,
            path=path,
            group=self.TYPE.MANIFEST,
            ref=path,
            state='installed',
            type_=self.TYPE.EXTRACT,
            data=d,

            source_url=source
        )
