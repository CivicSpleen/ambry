"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'


from sqlalchemy import Column as SAColumn, Integer, UniqueConstraint
from sqlalchemy import event
from sqlalchemy import Text, Binary, String, ForeignKey, Float
from sqlalchemy.orm import deferred

import six

from . import Base, MutationDict, JSONEncodedObj, BigIntegerType, DictableMixin
from ..util import Constant


class File(Base, DictableMixin):
    __tablename__ = 'files'

    MAJOR_TYPE = Constant()
    MAJOR_TYPE.BUILDSOURCE = 'bs'

    BSFILE = Constant()
    BSFILE.BUILD = 'build_bundle'
    BSFILE.LIB = 'lib'
    BSFILE.TEST = 'test'
    BSFILE.DOC = 'documentation'
    BSFILE.META = 'bundle_meta'
    BSFILE.SCHEMA = 'schema'
    BSFILE.SOURCESCHEMA = 'sourceschema'
    BSFILE.SOURCES = 'sources'
    BSFILE.SQL = 'sql'
    BSFILE.NOTEBOOK = 'notebook'

    # The preferences are primarily implemented in the prepare phase. WIth FILE preference, the
    # objects are always cleared before loading file values. With O, file values are never loaded, but objects
    # are written to files. With MERGE, Files are loaded to objects at the start of prepare, and  objects are
    # written back at the end, with no clearing.
    PREFERENCE = Constant()
    PREFERENCE.FILE = 'F'
    PREFERENCE.OBJECT = 'O'
    PREFERENCE.MERGE = 'M'

    id = SAColumn('f_id', Integer, primary_key=True)
    d_vid = SAColumn('f_d_vid', String(16), ForeignKey('datasets.d_vid'),
                     primary_key=True, nullable=False, index=True)

    path = SAColumn('f_path', Text, nullable=False)
    major_type = SAColumn('f_major_type', Text, nullable=False, index=True)
    minor_type = SAColumn('f_minor_type', Text, nullable=False, index=True)

    source = SAColumn('f_source', Text, nullable=False)

    mime_type = SAColumn('f_mime_type', Text)
    preference = SAColumn('f_preference', String(1), default=PREFERENCE.MERGE)  # 'F' for filesystem, 'O' for objects, "M" for merge
    state = SAColumn('f_state', Text)
    hash = SAColumn('f_hash', Text)  # Hash of the contents
    modified = SAColumn('f_modified', Float)
    size = SAColumn('f_size', BigIntegerType)

    contents = deferred(SAColumn('f_contents', Binary))
    source_hash = SAColumn('f_source_hash', Text)  # Hash of the source_file
    data = SAColumn('f_data', MutationDict.as_mutable(JSONEncodedObj))

    synced_fs = SAColumn('f_synced_fs', Float, doc='Time of last sync from filesystem')

    __table_args__ = (
        UniqueConstraint('f_d_vid', 'f_path', 'f_major_type', 'f_minor_type',  name='u_ref_path'),
    )

    def incver(self):
        """Increment all of the version numbers"""
        from . import  incver
        return incver(self, ['d_vid'])

    def update(self, of):
        """Update a file from another file, for copying"""

        # The other values should be set when the file object is created with dataset.bsfile()
        for p in ('mime_type', 'preference', 'state', 'hash', 'modified', 'size', 'contents', 'source_hash', 'data'):
            setattr(self, p, getattr(of, p))

        return self

    @property
    def unpacked_contents(self):
        """
        :return:
        """

        from nbformat import read

        import msgpack

        if self.mime_type == 'text/plain':
            return self.contents.decode('utf-8')
        elif self.mime_type == 'application/msgpack':
            # FIXME: Note: I'm not sure that encoding='utf-8' will not break old data.
            # We need utf-8 to make python3 to work. (kazbek)
            # return msgpack.unpackb(self.contents)
            return msgpack.unpackb(self.contents, encoding='utf-8')
        else:
            return self.contents

    @property
    def dict_row_reader(self):
        """ Unpacks message pack rows into a stream of dicts. """

        rows = self.unpacked_contents

        if not rows:
            return

        header = rows.pop(0)

        for row in rows:
            yield dict(list(zip(header, row)))

    def update_contents(self, contents, mime_type):
        """Update the contents and set the hash and modification time"""
        import hashlib
        import time

        new_size = len(contents)

        self.mime_type = mime_type

        if mime_type == 'text/plain':
            self.contents = contents.encode('utf-8')
        else:
            self.contents = contents

        old_hash = self.hash

        self.hash = hashlib.md5(self.contents).hexdigest()

        if self.size and (old_hash != self.hash):
            self.modified = int(time.time())

        self.size = new_size

    @property
    def has_contents(self):
        return (self.size or 0) > 0 and self.unpacked_contents is not None

    @property
    def row(self):
        from collections import OrderedDict

        # Use an Ordered Dict to make it friendly to creating CSV files.

        d = OrderedDict((p.key, getattr(self, p.key)) for p in self.__mapper__.attrs)

        return d



    @property
    def modified_datetime(self):
        from datetime import datetime
        try:
            return datetime.fromtimestamp(int(self.modified))
        except TypeError:
            return None

    @property
    def modified_ago(self):
        from ambry.util import pretty_time
        from time import time
        try:
            return pretty_time(int(time()) - int(self.modified))
        except TypeError:
            return None

    @property
    def dict(self):
        """A dict that holds key/values for all of the properties in the
        object.

        :return:

        """
        d = {p.key: getattr(self, p.key) for p in self.__mapper__.attrs
             if p.key not in ('contents', 'dataset')}

        d['modified_datetime'] = self.modified_datetime
        d['modified_ago'] = self.modified_ago

        return d

    def __init__(self,  **kwargs):
        super(File, self).__init__(**kwargs)

    def __repr__(self):
        return '<file: {}; {}/{}>'.format(self.path, self.major_type, self.minor_type)

    @staticmethod
    def validate_path(target, value, oldvalue, initiator):
        pass

    @staticmethod
    def before_insert(mapper, conn, target):
        """event.listen method for Sqlalchemy to set the sequence for this
        object and create an ObjectNumber value for the id_"""
        from sqlalchemy import text

        if not target.id:
            sql = text('SELECT max(f_id)+1 FROM files WHERE f_d_vid = :did')

            target.id, = conn.execute(sql, did=target.d_vid).fetchone()

            if not target.id:
                target.id = 1

        if target.contents and isinstance(target.contents, six.text_type):
            target.contents = target.contents.encode('utf-8')

        File.before_update(mapper, conn, target)

    @staticmethod
    def before_update(mapper, conn, target):

        pass


event.listen(File, 'before_insert', File.before_insert)
event.listen(File, 'before_update', File.before_update)

event.listen(File.path, 'set', File.validate_path)
