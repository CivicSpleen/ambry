"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'


from sqlalchemy import Column as SAColumn
from sqlalchemy import  Text, String, ForeignKey


from . import Base,  DictableMixin

class DataSource(Base, DictableMixin):
    """A source of data, such as a remote file or bundle"""

    __tablename__ = 'datasources'

    d_vid = SAColumn('ds_d_vid', String(16), ForeignKey('datasets.d_vid'), nullable=False, primary_key=True)
    name = SAColumn('ds_name', Text, primary_key=True)

    title = None
    table = None
    segment = None
    time = None
    space = None
    grain = None
    start_line = None
    end_line = None
    comment_lines = None
    header_lines = None
    description = None
    url = None
    ref = None

