"""Column map to transform column names from the source files to the schema.
Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'


from sqlalchemy import event
from sqlalchemy import Column as SAColumn, Integer, UniqueConstraint
from sqlalchemy import  Text, Binary, String, ForeignKey
import datetime
from sqlalchemy import event

from ..util import Constant


from . import Base, MutationDict, JSONEncodedObj, BigIntegerType, DictableMixin

class SourceColumn(Base):

    __tablename__ = 'sourcecolumns'

    DATATYPE = Constant()
    DATATYPE.INT = 'int'
    DATATYPE.FLOAT = 'float'
    DATATYPE.STRING = 'str'
    DATATYPE.DATE = 'date'
    DATATYPE.TIME = 'time'
    DATATYPE.DATETIME = 'datetime'

    type_map = {
        DATATYPE.INT: int,
        DATATYPE.FLOAT: float,
        DATATYPE.STRING: str,
        DATATYPE.DATE: datetime.date,
        DATATYPE.TIME: datetime.time,
        DATATYPE.DATETIME: datetime.datetime
    }

    id = SAColumn('sc_id', Integer, primary_key=True)

    source = SAColumn('ds_sc_id', Integer, ForeignKey('datasources.ds_id'), nullable=False)

    d_vid = SAColumn('sc_d_vid', String(16), ForeignKey('datasets.d_vid'), nullable=False)

    position = SAColumn('sc_position', Integer) # Integer position of column

    orig_header = SAColumn('sc_orig_header', Text) # Column header, After coalescing but before mangling.
    header = SAColumn('sc_header', Text)  # orig_header, mangled

    name = SAColumn('sc_name', Text) # Mapped column name, often ( & by default ) same as header.

    datatype = SAColumn('sc_datatype', Text) # Basic data type, usually intuited
    valuetype = SAColumn('sc_valuetype', Text) # Describes the meaning of the value: state, county, address, etc.

    start = SAColumn('sc_start', Integer) # For fixed width, the column starting position
    width = SAColumn('sc_width', Integer) # for Fixed width, the field width
    size = SAColumn('sc_size', Integer)  # Max size of the column values, after conversion to strings.

    summary = SAColumn('sc_summary', Text) # Short text description
    description = SAColumn('sc_description', Text) # Long text description

    value_labels = SAColumn('sc_value_labels', MutationDict.as_mutable(JSONEncodedObj))

    @staticmethod
    def mangle_name(name):
        """Mangles a column name to a standard form, removing illegal characters.

        :param name:
        :return:

        """
        import re

        try:
            return re.sub('_+', '_', re.sub('[^\w_]', '_', name).lower()).rstrip('_')
        except TypeError:
            raise TypeError(
                'Trying to mangle name with invalid type of: ' + str(type(name)))

@event.listens_for(SourceColumn.header, 'set')
def receive_set(target, value, oldvalue, initiator):

    if not target.name:
        target.name = SourceColumn.mangle_name(value)


