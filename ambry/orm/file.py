"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'


from sqlalchemy import Column as SAColumn, Integer, UniqueConstraint
from sqlalchemy import  Text, Binary, String, ForeignKey, Float

from ..util import Constant
from sqlalchemy import event

from . import Base, MutationDict, JSONEncodedObj, BigIntegerType, DictableMixin

class File(Base, DictableMixin):
    __tablename__ = 'files'

    MAJOR_TYPE = Constant()
    MAJOR_TYPE.BUILDSOURCE = 'bs'

    BSFILE = Constant
    BSFILE.BUILD = 'build_bundle'
    BSFILE.LIB = 'lib'
    BSFILE.META = 'bundle_meta'
    BSFILE.SOURCESCHEMA = 'sourceschema'
    BSFILE.SCHEMA = 'schema'
    BSFILE.COLMAP = 'column_map'
    BSFILE.SOURCES = 'sources'
    BSFILE.PARTITIONS = 'partitions'
    BSFILE.DOC = 'documentation'

    path_map = {
        BSFILE.BUILD: 'bundle.py',
        BSFILE.LIB: 'lib.py',
        BSFILE.DOC: 'documentation.md',
        BSFILE.META: 'bundle.yaml',
        BSFILE.SCHEMA: 'schema.csv',
        BSFILE.SOURCESCHEMA: 'source_schema.csv',
        BSFILE.SOURCES: 'sources.csv',
    }

    # The preferences are primarily implemented in the prepare phase. WIth FILE preference, the
    # objects are always cleared before loading file values. With O, file values are never loaded, but objects
    # are written to files. With MERGE, Files are loaded to objects at the start of prepare, and  objects are
    # written back at the end, with no clearing.
    PREFERENCE = Constant
    PREFERENCE.FILE = 'F'
    PREFERENCE.OBJECT = 'O'
    PREFERENCE.MERGE = 'M'

    id = SAColumn('f_id', Integer, primary_key=True)
    d_vid = SAColumn('f_d_vid', String(16), ForeignKey('datasets.d_vid'), primary_key=True, nullable=False, index=True)

    path = SAColumn('f_path', Text, nullable=False)
    major_type = SAColumn('f_major_type', Text, nullable=False, index=True)
    minor_type = SAColumn('f_minor_type', Text, nullable=False, index=True)

    source = SAColumn('f_source', Text, nullable=False)

    mime_type = SAColumn('f_mime_type', Text)
    preference = SAColumn('f_preference', String(1), default=PREFERENCE.MERGE) # 'F' for filesystem, 'O' for objects, "M" for merge
    state = SAColumn('f_state', Text)
    hash = SAColumn('f_hash', Text) # Hash of the contents
    modified = SAColumn('f_modified', Float)
    size = SAColumn('f_size', BigIntegerType)
    contents = SAColumn('f_contents', Binary)
    source_hash = SAColumn('f_source_hash', Text)  # Hash of the source_file
    data = SAColumn('f_data', MutationDict.as_mutable(JSONEncodedObj))

    __table_args__ = (
        UniqueConstraint('f_d_vid', 'f_path', 'f_major_type', 'f_minor_type',  name='u_ref_path'),
    )

    def update(self, of):
        """Update a file from another file, for copying"""

        # The other values hsould be set when the file object is created with dataset.bsfile()
        for p in ('mime_type', 'preference','state','hash','modified','size','contents','source_hash','data'):
            setattr(self, p, getattr(of, p))

        return self

    @property
    def unpacked_contents(self):
        """
        :return:
        """

        import msgpack

        if self.mime_type == 'text/plain':
            return self.contents
        elif self.mime_type == 'application/msgpack':
            return msgpack.unpackb(self.contents)
        else:
            return self.contents

    @property
    def dict_row_reader(self):
        """Unpacks message pack rows into a srtream of dicts"""

        rows = self.unpacked_contents

        if not rows:
            return

        header = rows.pop(0)

        for row in rows:
            yield dict(list(zip(header, row)))

    def update_contents(self, contents):
        """Update the contents and set the hash and modification time"""
        import hashlib
        import time

        old_size = self.size
        new_size = len(contents)

        # TODO remove this, debugging only.
        #assert not old_size or  new_size > .5*old_size, "new={} old={}".format(new_size, old_size)

        self.contents = contents

        self.hash = hashlib.md5(contents).hexdigest()

        self.modified = time.time()
        self.size = new_size

    @property
    def has_contents(self):
        return self.size > 0 and self.unpacked_contents is not None

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
            return datetime.fromtimestamp(self.modified)
        except TypeError:
            return None
    @property
    def modified_ago(self):
        from ambry.util import pretty_time
        from time import time
        try:
            return pretty_time(time() - self.modified)
        except TypeError:
            return None

    def __init__(self,  **kwargs):
        super(File, self).__init__( **kwargs)

    def __repr__(self):
        return "<file: {}; {}/{}>".format(self.path, self.major_type, self.minor_type)

    @staticmethod
    def validate_path(target, value, oldvalue, initiator):
        "Strip non-numeric characters from a phone number"
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

        File.before_update(mapper, conn, target)

    @staticmethod
    def before_update(mapper, conn, target):
        """"""
        pass

event.listen(File, 'before_insert', File.before_insert)
event.listen(File, 'before_update', File.before_update)


event.listen(File.path, 'set', File.validate_path)