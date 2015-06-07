"""Column map to transform column names from the source files to the schema.
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

class ColumnMap(Base, DictableMixin):
    __tablename__ = 'colmap'

    d_vid = SAColumn('cm_d_vid', String(16), ForeignKey('datasets.d_vid'), nullable=False, primary_key=True)
    table_name = SAColumn('cm_table', Text, nullable=False, primary_key=True)
    source = SAColumn('cm_source', Text, nullable=False, primary_key=True)
    dest = SAColumn('cm_dest', Text, nullable=False, index=True)
