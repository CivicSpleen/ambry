"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'


from sqlalchemy import event
from sqlalchemy import Column as SAColumn, Integer, UniqueConstraint
from sqlalchemy import String, ForeignKey
from sqlalchemy.orm import relationship, object_session

from . import Base, MutationDict, JSONEncodedObj, BigIntegerType

from ambry.identity import  Identity, PartitionNumber, ObjectNumber, PartialPartitionName, PartitionIdentity
from ambry.orm.columnstat import ColumnStat
from ambry.orm.dataset import Dataset
from ambry.orm import DictableMixin

class Partition(Base, DictableMixin):
    __tablename__ = 'partitions'

    sequence_id = SAColumn('p_sequence_id', Integer)
    vid = SAColumn('p_vid', String(20), primary_key=True, nullable=False)
    id = SAColumn('p_id', String(20), nullable=False)
    d_vid = SAColumn('p_d_vid',String(20),ForeignKey('datasets.d_vid'),nullable=False,index=True)
    t_vid = SAColumn('p_t_vid', String(20), ForeignKey('tables.t_vid'), nullable=False, index=True)
    name = SAColumn('p_name', String(200), nullable=False, index=True)
    vname = SAColumn('p_vname',String(200),unique=True,nullable=False,index=True)
    fqname = SAColumn('p_fqname',String(200),unique=True,nullable=False,index=True)
    cache_key = SAColumn('p_cache_key',String(200),unique=True,nullable=False,index=True)
    ref = SAColumn('p_ref', String(200), index=True)
    time = SAColumn('p_time', String(20))
    table_name = SAColumn('p_table_name', String(50))
    space = SAColumn('p_space', String(50))
    grain = SAColumn('p_grain', String(50))
    variant = SAColumn('p_variant', String(50))
    format = SAColumn('p_format', String(50))
    segment = SAColumn('p_segment', Integer)
    min_key = SAColumn('p_min_key', BigIntegerType)
    max_key = SAColumn('p_max_key', BigIntegerType)
    count = SAColumn('p_count', Integer)
    state = SAColumn('p_state', String(50))
    data = SAColumn('p_data', MutationDict.as_mutable(JSONEncodedObj))

    installed = SAColumn('p_installed', String(100))

    __table_args__ = (#ForeignKeyConstraint( [d_vid, d_location], ['datasets.d_vid','datasets.d_location']),
        UniqueConstraint('p_sequence_id', 'p_t_vid', name='_uc_partitions_1'),
    )

    # For the primary table for the partition. There is one per partition, but a table
    # can be primary in multiple partitions.
    table = relationship('Table', backref='partitions', foreign_keys='Partition.t_vid')

    stats = relationship(ColumnStat, backref='partition', cascade="delete, delete-orphan")

    @property
    def identity(self):
        """Return this partition information as a PartitionId."""

        if self.dataset is None:
            # The relationship will be null until the object is committed
            s = object_session(self)

            ds = s.query(Dataset).filter(Dataset.id_ == self.d_id).one()
        else:
            ds = self.dataset

        d = {
            'id': self.id,
            'vid': self.vid,
            'name': self.name,
            'vname': self.vname,
            'ref': self.ref,
            'space': self.space,
            'time': self.time,
            'table': self.table_name,
            'grain': self.grain,
            'segment': self.segment,
            'format': self.format if self.format else 'db'
        }

        return PartitionIdentity.from_dict(dict(ds.dict.items() + d.items()))

    def __repr__(self):
        return "<{} partition: {}>".format(self.format, self.vname)

    def set_ids(self, sequence_id):

        if not self.vid or not self.id_:

            self.sequence_id = sequence_id

            don = ObjectNumber.parse(self.d_vid)
            pon = PartitionNumber(don, self.sequence_id)

            self.vid = str(pon)
            self.id_ = str(pon.rev(None))

        self.fqname = Identity._compose_fqname(self.vname, self.vid)


    def add_stat(self, c_vid, stats):
        """Add a statistics records for a column of a table in the partition.

        :param c_vid: The column vid.
        :param stats:  A dict of stats values. See the code for which values are valid.
        :return:

        """

        # Names that come from the Pandas describe() method
        stat_map = {'25%': 'p25', '50%': 'p50', '75%': 'p75'}

        stats = {stat_map.get(k, k): v for k, v in stats.items()}

        cs = ColumnStat(p_vid=self.vid, c_vid=c_vid, **stats)

        self._stats.append(cs)

        return cs

    @property
    def stats(self):

        class Bunch(object):
            """Dict and object access to properties"""
            def __init__(self, o):
                self.__dict__.update(o)

            def __str__(self):
                return str(self.__dict__)

            def __repr__(self):
                return str(self.__dict__)

            def items(self):
                return self.__dict__.items()

        cols = {s.column.name: Bunch(s.dict) for s in self._stats}

        return Bunch(cols)

    @staticmethod
    def before_insert(mapper, conn, target):
        """event.listen method for Sqlalchemy to set the sequence for this
        object and create an ObjectNumber value for the id_"""

        if not target.vid:
            assert bool(target.d_vid)
            assert bool(target.sequence_id)
            on = ObjectNumber.parse(target.d_vid).as_partition(target.sequence_id)
            target.vid = str(on)
            target.id = str(on.rev(None))

        if not target.data:
            target.data = {}

        Partition.before_update(mapper, conn, target)

    @staticmethod
    def before_update(mapper, conn, target):
        """"""

        from ..identity import ObjectNumber, PartialPartitionName

        d = target.dict
        d['table'] = target.table_name

        name = PartialPartitionName(**d).promote(target.dataset.identity.name)

        target.name = str(name.name)
        target.vname = name.vname
        target.cache_key = name.cache_key
        target.fqname = target.identity.fqname



event.listen(Partition, 'before_insert', Partition.before_insert)
event.listen(Partition, 'before_update', Partition.before_update)