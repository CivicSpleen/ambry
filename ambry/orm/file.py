"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'


from sqlalchemy import event
from sqlalchemy import Column as SAColumn, Integer, UniqueConstraint
from sqlalchemy import  Text, Binary

from ..util import Constant
from ..identity import LocationRef

from . import Base, MutationDict, JSONEncodedObj, BigIntegerType

class File(Base):
    __tablename__ = 'files'

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
    TYPE.DOWNLOAD = 'download'

    PROCESS = Constant()
    PROCESS.MODIFIED = 'modified'
    PROCESS.UNMODIFIED = 'unmodified'
    PROCESS.DOWNLOADED = 'downloaded'
    PROCESS.CACHED = 'cached'

    oid = SAColumn('f_id', Integer, primary_key=True, nullable=False)
    path = SAColumn('f_path', Text, nullable=False)
    ref = SAColumn('f_ref', Text, index=True, nullable=False)
    type_ = SAColumn('f_type', Text, nullable=False)
    source_url = SAColumn('f_source_url', Text, nullable=False)
    process = SAColumn('f_process', Text)
    state = SAColumn('f_state', Text)
    hash = SAColumn('f_hash', Text)
    modified = SAColumn('f_modified', Integer)
    size = SAColumn('f_size', BigIntegerType)

    priority = SAColumn('f_priority', Integer)

    data = SAColumn('f_data', MutationDict.as_mutable(JSONEncodedObj))

    content = SAColumn('f_content', Binary)

    __table_args__ = (
        UniqueConstraint('f_ref', 'f_type', 'f_source_url', name='u_ref_path'),
    )

    def __init__(self, **kwargs):

        if not kwargs.get("ref", None):
            import hashlib

            kwargs['ref'] = hashlib.md5(kwargs['path']).hexdigest()

        self.oid = kwargs.get("oid", None)
        self.path = kwargs.get("path", None)
        self.source_url = kwargs.get("source_url", kwargs.get("source", None))
        self.process = kwargs.get("process", None)
        self.state = kwargs.get("state", None)
        self.modified = kwargs.get("modified", None)
        self.size = kwargs.get("size", None)
        self.group = kwargs.get("group", None)
        self.ref = kwargs.get("ref", None)
        self.hash = kwargs.get("hash", None)
        self.type_ = kwargs.get("type", kwargs.get("type_", None))

        self.data = kwargs.get('data', None)
        self.priority = kwargs.get('priority', 0)
        self.content = kwargs.get('content', None)




    def __repr__(self):
        return "<file: {}; {}>".format(self.path, self.ref, self.state)

    def update(self, f):
        """Copy another files properties into this one."""

        for p in self.__mapper__.attrs:

            if p.key == 'oid':
                continue
            try:
                setattr(self, p.key, getattr(f, p.key))

            except AttributeError:
                # The dict() method copies data property values into the main dict,
                # and these don't have associated class properties.
                continue

    @property
    def dict(self):

        d = dict((col, getattr(self,col)) for col in [
                'oid','path','ref','type_','source_url','process',
                'state','hash','modified','size', 'priority'])

        if self.data:
            for k in self.data:
                assert k not in d
                d[k] = self.data[k]

        return d

    @property
    def record_dict(self):
        """Like dict, but does not move data items into the top level."""
        return {p.key: getattr(self, p.key) for p in self.__mapper__.attrs}

    @property
    def insertable_dict(self):
        """Like record_dict, but prefixes all of the keys with 'f_', so it can
        be used in inserts."""
        # .strip('_') is for type_
        return {
            'f_' +
            p.key.strip('_'): getattr(
                self,
                p.key) for p in self.__mapper__.attrs}



    @staticmethod
    def before_update(mapper, conn, target):
        """Set the column id number based on the table number and the sequence
        id for the column."""

        assert bool(target.ref), "File.ref can't be null (before_update)"

    @staticmethod
    def set_ref(target, value, oldvalue, initiator):
        "Strip non-numeric characters from a phone number"

        assert bool(value), "File.ref can't be null (set_ref)"


event.listen(File, 'before_insert', File.before_update)
event.listen(File, 'before_update', File.before_update)
event.listen(File.ref, 'set', File.set_ref)