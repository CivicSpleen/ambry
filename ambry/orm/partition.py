"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'

from collections import OrderedDict

from geoid.util import isimplify
from geoid.civick import GVid

from dateutil import parser

from sqlalchemy import event
from sqlalchemy import Column as SAColumn, Integer, UniqueConstraint
from sqlalchemy import String, ForeignKey
from sqlalchemy.orm import relationship, object_session, backref

from ambry.identity import Identity, PartitionNumber, ObjectNumber, PartialPartitionName, PartitionIdentity
from ambry.orm.columnstat import ColumnStat
from ambry.orm.dataset import Dataset
from ambry.orm import DictableMixin
from ambry.util import Constant

from . import Base, MutationDict, MutationList, JSONEncodedObj, BigIntegerType


class Partition(Base, DictableMixin):
    __tablename__ = 'partitions'

    STATES = Constant()
    STATES.SYNCED = 'synced'
    STATES.CLEANING = 'cleaning'
    STATES.CLEANED = 'cleaned'
    STATES.PREPARING = 'preparing'
    STATES.PREPARED = 'prepared'
    STATES.BUILDING = 'building'
    STATES.BUILT = 'built'
    STATES.ERROR = 'error'
    STATES.FINALIZING = 'finalizing'
    STATES.FINALIZED = 'finalized'
    STATES.INSTALLING = 'installing'
    STATES.INSTALLED = 'installed'

    TYPE = Constant
    TYPE.SEGMENT = 's'
    TYPE.UNION = 'u'

    sequence_id = SAColumn('p_sequence_id', Integer)
    vid = SAColumn('p_vid', String(20), primary_key=True, nullable=False)
    id = SAColumn('p_id', String(20), nullable=False)
    d_vid = SAColumn('p_d_vid', String(20), ForeignKey('datasets.d_vid'), nullable=False, index=True)
    t_vid = SAColumn('p_t_vid', String(20), ForeignKey('tables.t_vid'), nullable=False, index=True)
    name = SAColumn('p_name', String(200), nullable=False, index=True)
    vname = SAColumn('p_vname', String(200), unique=True, nullable=False, index=True)
    fqname = SAColumn('p_fqname', String(200), unique=True, nullable=False, index=True)
    cache_key = SAColumn('p_cache_key', String(200), unique=True, nullable=False, index=True)
    parent_vid = SAColumn('p_p_vid', String(20), ForeignKey('partitions.p_vid'), nullable=True, index=True)
    ref = SAColumn('p_ref', String(20), index=True,
        doc='VID reference to an eariler version to use instead of this one.')
    type = SAColumn('p_type', String(20), default=TYPE.UNION,
        doc='u - normal partition, s - segment')
    table_name = SAColumn('p_table_name', String(50))
    time = SAColumn('p_time', String(20))  # FIXME: add helptext
    space = SAColumn('p_space', String(50))
    grain = SAColumn('p_grain', String(50))
    variant = SAColumn('p_variant', String(50))
    format = SAColumn('p_format', String(50))
    segment = SAColumn('p_segment', Integer,
        doc='Part of a larger partition. segment_id is usually also a source ds_id')
    min_id = SAColumn('p_min_id', BigIntegerType)
    max_id = SAColumn('p_max_id', BigIntegerType)
    count = SAColumn('p_count', Integer)
    state = SAColumn('p_state', String(50))
    data = SAColumn('p_data', MutationDict.as_mutable(JSONEncodedObj))

    space_coverage = SAColumn('p_scov', MutationList.as_mutable(JSONEncodedObj))
    time_coverage = SAColumn('p_tcov', MutationList.as_mutable(JSONEncodedObj))
    grain_coverage = SAColumn('p_gcov', MutationList.as_mutable(JSONEncodedObj))

    installed = SAColumn('p_installed', String(100))
    location = SAColumn('p_location', String(100))  # Location of the data file

    __table_args__ = (
        # ForeignKeyConstraint( [d_vid, d_location], ['datasets.d_vid','datasets.d_location']),
        UniqueConstraint('p_sequence_id', 'p_d_vid', name='_uc_partitions_1'),
    )

    # For the primary table for the partition. There is one per partition, but a table
    # can be primary in multiple partitions.
    table = relationship('Table', backref='partitions', foreign_keys='Partition.t_vid')

    stats = relationship(ColumnStat, backref='partition', cascade='all, delete, delete-orphan')

    children = relationship('Partition', backref=backref('parent', remote_side=[vid]), cascade='all')

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

        return PartitionIdentity.from_dict(dict(list(ds.dict.items()) + list(d.items())))

    @property
    def is_segment(self):

        return self.type == self.TYPE.SEGMENT

    def __repr__(self):
        return '<{} partition: {}>'.format(self.format, self.vname)

    def set_ids(self, sequence_id):

        if not self.vid or not self.id_:

            self.sequence_id = sequence_id

            don = ObjectNumber.parse(self.d_vid)
            pon = PartitionNumber(don, self.sequence_id)

            self.vid = str(pon)
            self.id_ = str(pon.rev(None))

        self.fqname = Identity._compose_fqname(self.vname, self.vid)

    def set_stats(self, stats):

        sd = dict(stats)

        for c in self.table.columns:

            if c.name not in sd:
                continue

            d = sd[c.name].dict

            del d['name']
            del d['flags']
            cs = ColumnStat(p_vid=self.vid, d_vid=self.d_vid, c_vid=c.vid, **d)
            self.stats.append(cs)

    def parse_gvid_or_place(self, gvid_or_place):
        try:
            return GVid.parse(gvid_or_place)
        except KeyError:

            places = list(self._bundle._library.search.search_identifiers(gvid_or_place))

            if not places:
                err_msg = "Failed to find space identifier '{}' in full "\
                    "text identifier search  for partition '{}'"\
                    .format(gvid_or_place, str(self.identity))
                self._bundle.error(err_msg)
                return None

            return GVid.parse(places[0].vid)

    def set_coverage(self, stats):
        """"Extract time space and grain coverage from the stats and store them in the partition"""
        from ambry.util.datestimes import expand_to_years
        sd = dict(stats)

        scov = set()
        tcov = set()
        grains = set()

        for c in self.table.columns:
            if c.name not in sd:
                continue

            if sd[c.name].is_gvid:
                scov |= set(x for x in isimplify(GVid.parse(gvid) for gvid in sd[c.name].uniques))
                grains |= set(GVid.parse(gvid).summarize() for gvid in sd[c.name].uniques)
            elif sd[c.name].is_year:

                tcov |= set(int(x) for x in sd[c.name].uniques)
            elif sd[c.name].is_date:
                # The fuzzy=True argument allows ignoring the '-' char in dates produced by .isoformat()
                tcov |= set(parser.parse(x, fuzzy=True).year if isinstance(x, basestring) else x.year for x in sd[c.name].uniques)

        #
        # Space Coverage

        if 'source_data' in self.data:

            for source_name, source in list(self.data['source_data'].items()):
                    scov.add(self.parse_gvid_or_place(source['space']))

        if self.identity.space:  # And from the partition name
            scov.add(self.parse_gvid_or_place(self.identity.space))

        # For geo_coverage, only includes the higher level summary levels, counties, states, places and urban areas
        self.space_coverage = sorted([str(x) for x in scov if bool(x) and x.sl in (10, 40, 50, 60, 160, 400)])

        #
        # Time Coverage

        # From the source
        # If there was a time value in the source that this partition was created from, then
        # add it to the years.
        if 'source_data' in self.data:
            for source_name, source in list(self.data['source_data'].items()):
                if 'time' in source:
                    for year in expand_to_years(source['time']):
                        tcov.add(year)

        # From the partition name
        if self.identity.name.time:
            for year in expand_to_years(self.identity.name.time):
                tcov.add(year)

        self.time_coverage = tcov

        #
        # Grains

        if 'source_data' in self.data:
            for source_name, source in list(self.data['source_data'].items()):
                if 'grain' in source:
                    grains.add(source['grain'])

        self.grain_coverage = sorted(str(g) for g in grains if g)

    @property
    def stats_dict(self):

        class Bunch(object):
            """Dict and object access to properties"""
            def __init__(self, o):
                self.__dict__.update(o)

            def __str__(self):
                return str(self.__dict__)

            def __repr__(self):
                return str(self.__dict__)

            def items(self):
                return list(self.__dict__.items())

            def __getitem__(self, k):
                return self.__dict__[k]

        cols = {s.column.name: Bunch(s.dict) for s in self.stats}

        return Bunch(cols)

    def build_sample(self):

        name = self.table.name

        count = int(
            self.database.connection.execute('SELECT count(*) FROM "{}"'.format(name)).fetchone()[0])

        skip = count / 20

        if count > 100:
            sql = 'SELECT * FROM "{}" WHERE id % {} = 0 LIMIT 20'.format(name, skip)
        else:
            sql = 'SELECT * FROM "{}" LIMIT 20'.format(name)

        sample = []

        for j, row in enumerate(self.database.connection.execute(sql)):
            sample.append(list(row.values()))

        self.record.data['sample'] = sample

        s = self.bundle.database.session
        s.merge(self.record)
        s.commit()

    @property
    def row(self):
        # Use an Ordered Dict to make it friendly to creating CSV files.
        SKIP_KEYS = [
            'sequence_id', 'vid', 'id', 'd_vid', 't_vid', 'min_key', 'max_key',
            'installed', 'ref', 'count', 'state', 'data', 'space_coverage',
            'time_coverage', 'grain_coverage', 'name', 'vname', 'fqname', 'cache_key'
        ]

        d = OrderedDict([('table', self.table.name)] +
                        [(p.key, getattr(self, p.key)) for p in self.__mapper__.attrs
                         if p.key not in SKIP_KEYS])
        return d

    def update(self, **kwargs):

        if 'table' in kwargs:
            del kwargs['table']  # In source_schema.csv, this is the name of the table, not the object

        for k, v in list(kwargs.items()):
            if hasattr(self, k):
                setattr(self, k, v)

    def finalize(self, stats=None):

        self.state = self._partition.STATES.BUILT

        # Write the stats for this partition back into the partition

        if stats:
            self.set_stats(stats.stats())
            self.set_coverage(stats.stats())
            self.table.update_from_stats(stats.stats())

        self.location = 'build'

        self.state = self._partition.STATES.FINALIZED

    @staticmethod
    def before_insert(mapper, conn, target):
        """event.listen method for Sqlalchemy to set the sequence for this
        object and create an ObjectNumber value for the id_"""
        from sqlalchemy import text

        if not target.sequence_id:
            conn.execute('BEGIN IMMEDIATE')
            sql = text('SELECT max(p_sequence_id)+1 FROM partitions WHERE p_d_vid = :did')

            target.sequence_id, = conn.execute(sql, did=target.d_vid).fetchone()

            if not target.sequence_id:
                target.sequence_id = 1

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

        d = target.dict
        d['table'] = target.table_name

        name = PartialPartitionName(**d).promote(target.dataset.identity.name)

        target.name = str(name.name)
        target.vname = name.vname
        target.cache_key = name.cache_key
        target.fqname = target.identity.fqname

event.listen(Partition, 'before_insert', Partition.before_insert)
event.listen(Partition, 'before_update', Partition.before_update)
