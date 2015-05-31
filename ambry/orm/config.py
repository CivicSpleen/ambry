"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'


from sqlalchemy import Column as SAColumn
from sqlalchemy import  Text, String

from . import Base, JSONAlchemy

class Config(Base):

    __tablename__ = 'config'

    d_vid = SAColumn('co_d_vid', String(16), primary_key=True)
    group = SAColumn('co_group', String(200), primary_key=True)
    key = SAColumn('co_key', String(200), primary_key=True)

    value = SAColumn('co_value', JSONAlchemy(Text()))

    # What does this do?
    source = SAColumn('co_source', String(200))

    def __init__(self, **kwargs):
        self.d_vid = kwargs.get("d_vid", None)
        self.group = kwargs.get("group", None)
        self.key = kwargs.get("key", None)
        self.value = kwargs.get("value", None)
        self.source = kwargs.get("source", None)

    @property
    def dict(self):
        return {p.key: getattr(self, p.key) for p in self.__mapper__.attrs}

    def __repr__(self):
        return "<config: {},{},{} = {}>".format(self.d_vid,self.group,self.key,self.value)