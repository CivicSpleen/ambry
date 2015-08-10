"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'

from sqlalchemy import event
from sqlalchemy import Column as SAColumn, Integer
from sqlalchemy import  Text, String, ForeignKey

from ambry.identity import ObjectNumber

from . import Base, MutationDict, JSONEncodedObj

class Code(Base):

    """Code entries for variables."""
    __tablename__ = 'codes'

    c_vid = SAColumn('cd_c_vid',String(20),ForeignKey('columns.c_vid'), primary_key=True,index=True,nullable=False)

    d_vid = SAColumn('cd_d_vid', String(20), ForeignKey('datasets.d_vid'), primary_key=True, nullable=False, index=True)

    key = SAColumn('cd_skey',String(20), primary_key=True,nullable=False,index=True)  # String version of the key, the value in the dataset
    ikey = SAColumn( 'cd_ikey',Integer,index=True)  # Set only if the key is actually an integer

    value = SAColumn('cd_value', Text,nullable=False)  # The value the key maps to
    description = SAColumn('cd_description', Text)

    source = SAColumn('cd_source', Text)

    data = SAColumn('cd_data', MutationDict.as_mutable(JSONEncodedObj))

    def __init__(self, **kwargs):

        for p in self.__mapper__.attrs:
            if p.key in kwargs:
                setattr(self, p.key, kwargs[p.key])
                del kwargs[p.key]

        if self.data:
            self.data.update(kwargs)


    def __repr__(self):
        return "<code: {}->{} >".format(self.key, self.value)

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
    def insertable_dict(self):

        d =  {('cd_' + k).strip('_'): v for k, v in self.dict.items()}

        # the `key` property is not named after its db column
        d['cd_skey'] = d['cd_key']
        del d['cd_key']

        return d


    @staticmethod
    def before_insert(mapper, conn, target):

        target.d_vid = str(ObjectNumber.parse(target.c_vid).as_dataset)

event.listen(Code, 'before_insert', Code.before_insert)