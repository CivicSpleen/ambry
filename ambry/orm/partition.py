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

from sqlalchemy.sql import text

from . import Base, MutationDict, JSONEncodedObj, BigIntegerType

from ambry.identity import  Identity, PartitionNumber, ObjectNumber, PartialPartitionName, PartitionIdentity
from ambry.orm.columnstat import ColumnStat
from ambry.orm.dataset import Dataset


class Partition(Base):
    __tablename__ = 'partitions'

    vid = SAColumn('p_vid', String(20), primary_key=True, nullable=False)
    id_ = SAColumn('p_id', String(20), nullable=False)
    name = SAColumn('p_name', String(200), nullable=False, index=True)
    vname = SAColumn('p_vname',String(200),unique=True,nullable=False,index=True)
    fqname = SAColumn('p_fqname',String(200),unique=True,nullable=False,index=True)
    cache_key = SAColumn('p_cache_key',String(200),unique=True,nullable=False,index=True)
    sequence_id = SAColumn('p_sequence_id', Integer)
    t_vid = SAColumn('p_t_vid',String(20),ForeignKey('tables.t_vid'),nullable=False,index=True)
    t_id = SAColumn('p_t_id', String(20))
    d_vid = SAColumn('p_d_vid',String(20),ForeignKey('datasets.d_vid'),nullable=False,index=True)
    d_id = SAColumn('p_d_id', String(20))
    ref = SAColumn('p_ref', String(200), index=True)
    time = SAColumn('p_time', String(20))
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

    def __init__(self, dataset, t_id, **kwargs):

        self.vid = kwargs.get("vid", kwargs.get("id_", None))
        self.id_ = kwargs.get("id", kwargs.get("id_", None))
        self.name = kwargs.get("name", kwargs.get("name", None))
        self.vname = kwargs.get("vname", None)
        self.ref = kwargs.get("ref", None)
        self.fqname = kwargs.get("fqname", None)
        self.cache_key = kwargs.get("cache_key", None)
        self.sequence_id = kwargs.get("sequence_id", None)

        self.space = kwargs.get("space", None)
        self.time = kwargs.get("time", None)
        self.grain = kwargs.get('grain', None)
        self.format = kwargs.get('format', None)
        self.segment = kwargs.get('segment', None)
        self.data = kwargs.get('data', None)

        self.d_vid = dataset.vid
        self.d_id = dataset.id_

        self.t_id = t_id


        tables = { t.id_: t.name for t in dataset.tables }

        don = ObjectNumber.parse(self.d_vid)
        ton = ObjectNumber.parse(self.t_id)
        self.t_vid = str(ton.rev(don.revision))



        kwargs['table'] = tables[self.t_id]
        ppn = PartialPartitionName(**kwargs)

        if not self.vname:
            self.vname = ppn.promote(dataset.identity.name).vname
            self.name = ppn.promote(dataset.identity.name).name

        if not self.cache_key:
            self.cache_key = ppn.promote(dataset.identity.name).cache_key

        assert self.cache_key is not None

        if True:  # Debugging
            from partition import extension_for_format_name

            ext = extension_for_format_name(self.format)

            assert self.cache_key.endswith(ext)

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
            'id': self.id_,
            'vid': self.vid,
            'name': self.name,
            'vname': self.vname,
            'ref': self.ref,
            'space': self.space,
            'time': self.time,
            'table': self.table.name if self.t_vid is not None else None,
            'grain': self.grain,
            'segment': self.segment,
            'format': self.format if self.format else 'db'
        }

        return PartitionIdentity.from_dict(dict(ds.dict.items() + d.items()))

    @property
    def dict(self):
        from geoid.civick import GVid

        d = {
            'id': self.id_,
            'sequence_id': self.sequence_id,
            'vid': self.vid,
            'name': self.name,
            'vname': self.vname,
            'ref': self.ref,
            'fqname': self.fqname,
            'cache_key': self.cache_key,
            'd_id': self.d_id,
            'd_vid': self. d_vid,
            't_id': self.t_id,
            't_vid': self. t_vid,
            'space': self.space,
            'time': self.time,
            'table': self.table.name if self.t_vid is not None else None,
            'table_vid': self.t_vid,
            'grain': self.grain,
            'segment': self.segment,
            'format': self.format if self.format else 'db',
            'count': self.count,
            'min_key': self.min_key,
            'max_key': self.max_key
        }

        for k in self.data:
            assert k not in d
            d[k] = self.data[k]

        d['dataset'] = self.dataset.dict

        d['colstats'] = {s.column.id_: s.dict for s in self._stats}

        if 'geo_grain' in d:
            d['geo_grain'] = {
                'vids': d['geo_grain'],
                'names': []
            }

            for gvid_str in d['geo_grain']['vids']:
                try:
                    gvid = GVid.parse(gvid_str)
                    d['geo_grain']['names'].append(gvid.level.title())
                except KeyError:
                    d['geo_grain']['names'].append(gvid_str)

        if 'geo_coverage' in d:
            d['geo_coverage'] = {
                'vids': d['geo_coverage'],
                'names': []
            }

        if 'time_coverage' in d:
            from util.datestimes import expand_to_years

            all_years = expand_to_years(d['time_coverage'])

            d['time_coverage'] = {
                'years': d['time_coverage'],
                'min': min(all_years) if all_years else None,
                'max': max(all_years) if all_years else None,
            }

        d['foreign_indexes'] = list(set([c.data['index'].split(':')[0] for c in self.table.columns
                                 if c.data.get('index', False)]))

        return d

    @property
    def nonull_dict(self):
        d = {k: v for k, v in self.dict.items() if v}

        d['format'] = self.format if self.format else 'db'
        d['table'] = self.table.name if self.t_vid is not None else None

        return d

    @property
    def insertable_dict(self):
        return {('p_' + k).strip('_'): v for k, v in self.dict.items()}

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

        if target.sequence_id is None:
            # These records can be added in an multi-process environment, we
            # we need exclusive locking here, where we don't for other sequence
            # ids.
            conn.execute("BEGIN IMMEDIATE")
            sql = text("SELECT max(p_sequence_id)+1 FROM Partitions WHERE p_d_id = :did")

            max_id, = conn.execute(sql, did=target.d_id).fetchone()

            if not max_id:
                max_id = 1

            target.sequence_id = max_id

        target.set_ids(target.sequence_id)

        Partition.before_update(mapper, conn, target)

    @staticmethod
    def before_update(mapper, conn, target):
        """Set the column id number based on the table number and the sequence
        id for the column."""
        if not target.id_:
            dataset = ObjectNumber.parse(target.d_id)
            target.id_ = str(PartitionNumber(dataset, target.sequence_id))

    @staticmethod
    def set_t_vid(target, value, oldvalue, initiator):
        "Check that t_vid isn't set to Null"
        if not bool(value):
            raise AssertionError("Partition.t_vid can't be null (set_t_vid): {} -> {}".format(oldvalue, value))

#event.listen(Partition.t_vid, 'set', Partition.set_t_vid)
event.listen(Partition, 'before_insert', Partition.before_insert)
event.listen(Partition, 'before_update', Partition.before_update)