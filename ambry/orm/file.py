"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'


from sqlalchemy import event
from sqlalchemy import Column as SAColumn, Integer, UniqueConstraint
from sqlalchemy import  Text, Binary, String, ForeignKey

from ..util import Constant
from ..identity import LocationRef

from . import Base, MutationDict, JSONEncodedObj, BigIntegerType, DictableMixin

class File(Base, DictableMixin):
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

    id = SAColumn('id', Integer, primary_key=True)
    d_vid = SAColumn('f_d_vid', String(16), ForeignKey('datasets.d_vid'), nullable=False, index=True)
    path = SAColumn('f_path', Text, nullable=False)
    major_type = SAColumn('f_major_type', Text, nullable=False, index=True)
    minor_type = SAColumn('f_minor_type', Text, nullable=False, index=True)

    source = SAColumn('f_source', Text, nullable=False)

    state = SAColumn('f_state', Text)
    hash = SAColumn('f_hash', Text)
    modified = SAColumn('f_modified', Integer)
    size = SAColumn('f_size', BigIntegerType)

    content = SAColumn('f_content', Binary)

    data = SAColumn('f_data', MutationDict.as_mutable(JSONEncodedObj))

    __table_args__ = (
        UniqueConstraint('f_d_vid', 'f_path', 'f_major_type', 'f_minor_type',  name='u_ref_path'),
    )

    def __init__(self,  **kwargs):
        super(File, self).__init__( **kwargs)

    def __repr__(self):
        return "<file: {}; {}/{}>".format(self.path, self.major_type, self.minor_type)

