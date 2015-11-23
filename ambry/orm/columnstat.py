"""Object-Relational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'

from sqlalchemy import Column as SAColumn, Integer, Float, UniqueConstraint
from sqlalchemy import  String, ForeignKey
from sqlalchemy.orm import relationship

from . import Base, MutationDict, MutationList, JSONEncodedObj

from . import BigIntegerType

lom_enums = "nom ord int ratio".split()

class ColumnStat(Base):

    """Table for per column, per partition stats."""
    __tablename__ = 'colstats'

    p_vid = SAColumn('cs_p_vid', String(20), ForeignKey('partitions.p_vid'), primary_key=True, nullable=False, index=True)
    #partition = relationship('Partition', backref='stats')

    c_vid = SAColumn('cs_c_vid', String(20), ForeignKey('columns.c_vid'), primary_key=True, nullable=False, index=True)
    column = relationship('Column', backref='stats')

    d_vid = SAColumn('cs_d_vid', String(20), ForeignKey('datasets.d_vid'), nullable=False, index=True)
    dataset = relationship('Dataset', backref='stats')

    lom = SAColumn('cs_lom', String(1))
    count = SAColumn('cs_count', BigIntegerType)
    mean = SAColumn('cs_mean', Float)
    std = SAColumn('cs_std', Float)
    min = SAColumn('cs_min', Float)
    p25 = SAColumn('cs_p25', Float)
    p50 = SAColumn('cs_p50', Float)
    p75 = SAColumn('cs_p75', Float)
    max = SAColumn('cs_max', Float)
    nuniques = SAColumn('cs_nuniques', BigIntegerType)

    skewness = SAColumn('cs_skewness', Float)
    kurtosis = SAColumn('cs_kurtosis', Float)

    uvalues = SAColumn('f_uvalues', MutationDict.as_mutable(JSONEncodedObj))
    hist = SAColumn('f_hist', MutationList.as_mutable(JSONEncodedObj))

    text_hist = SAColumn('cs_text_hist', String)

    __table_args__ = (
        UniqueConstraint('cs_p_vid', 'cs_c_vid', name='u_cols_stats'),
    )

    @property
    def dict(self):

        return {p.key: getattr(self,p.key) for p in self.__mapper__.attrs if p.key not in
                ('data','column', 'table','partition')}

