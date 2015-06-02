"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'


from sqlalchemy import event
from sqlalchemy import Column as SAColumn, Integer, UniqueConstraint
from sqlalchemy import  Text, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import text

from ambry.identity import TableNumber,  ObjectNumber
from ambry.orm import Column
from ambry.orm.exc import NotFoundError
from . import Base, MutationDict, JSONEncodedObj

class Table(Base):
    __tablename__ = 'tables'

    vid = SAColumn('t_vid', String(20), primary_key=True)
    id = SAColumn('t_id', String(20), primary_key=False)
    d_id = SAColumn('t_d_id', String(20))
    d_vid = SAColumn('t_d_vid',String(20),ForeignKey('datasets.d_vid'),index=True)

    sequence_id = SAColumn('t_sequence_id', Integer, nullable=False)
    name = SAColumn('t_name', String(200), nullable=False)
    altname = SAColumn('t_altname', Text)
    description = SAColumn('t_description', Text)
    universe = SAColumn('t_universe', String(200))
    keywords = SAColumn('t_keywords', Text)
    type = SAColumn('t_type', String(20))
    # Reference to a column that provides an example of how this table should be used.
    proto_vid = SAColumn('t_proto_vid', String(20), index=True)

    installed = SAColumn('t_installed', String(100))
    data = SAColumn('t_data', MutationDict.as_mutable(JSONEncodedObj))

    __table_args__ = (
        UniqueConstraint('t_sequence_id', 't_d_vid', name='_uc_tables_1'),
        UniqueConstraint('t_name', 't_d_vid', name='_uc_tables_2'),
    )

    columns = relationship(Column, backref='table', order_by="asc(Column.sequence_id)",
                           cascade="all, delete-orphan", lazy='joined')

    def __init__(self,  **kwargs):

        super(Table, self).__init__(**kwargs)

    def link_columns(self, other):
        """Return columns that can be used to link another table to this one"""

        def protos(t):

            protos = {}

            protos.update({ c.fk_vid:c for c in t.columns if c.fk_vid })
            protos.update({ c.proto_vid:c for c in t.columns if c.proto_vid})

            # HACK: The numbering in the proto dataset changes, so we have to make substitutions
            if 'c00104002' in protos:
                protos['c00109003'] = protos['c00104002']
                del protos['c00104002']

            protos = { str(ObjectNumber.parse(n).rev(None)):c for n, c in protos.items() } # Remove revisions

            return protos

        protos_s = protos(self)
        protos_o = protos(other)

        inter =  set(protos_s.keys())  & set(protos_o.keys())

        return list(set( (protos_s[n], protos_o[n])  for n in inter ) )

    @staticmethod
    def mangle_name(name, preserve_case=False):
        import re
        try:
            r = re.sub('[^\w_]', '_', name.strip())

            if not preserve_case:
                r = r.lower()

            return r
        except TypeError:
            raise TypeError('Not a valid type for name ' + str(type(name)))

    @property
    def oid(self):
        return TableNumber(self.d_id, self.sequence_id)

    def column(self, ref):
        # AFAIK, all of the columns in the relationship will get loaded if any one is accessed,
        # so iterating over the collection only involves one SELECT.

        for c in self.columns:
            if str(ref) == c.name or str(ref) == c.id or str(ref) == c.vid:
                return c

        raise NotFoundError("Failed to find column '{}' in table '{}' for ref: '{}' ".format(ref, self.name, ref))

    def add_column(self, name, **kwargs):
        """Add a column to the table, or update an existing one."""

        try:
            c = self.column(name)
            extant = True
        except NotFoundError:
            c = Column(t_vid=self.vid, sequence_id=len(self.columns), name=name, datatype='varchar')
            extant = False

        # Update possibly existing data
        c.data = dict((c.data.items() if c.data else []) + kwargs.get('data', {}).items())

        for key, value in kwargs.items():

            if key[0] != '_' and key not in ['t_vid', 'name',  'sequence_id', 'data']:
                try:
                    setattr(c, key, value)
                except AttributeError:
                    raise AttributeError("Column record has no attribute {}".format(key))

            if key == 'is_primary_key' and isinstance(value, basestring) and len(value) == 0:
                value = False
                setattr(c, key, value)

        # If the id column has a description and the table does not, add it to
        # the table.
        if c.name == 'id' and c.is_primary_key and not self.description:
            self.description = c.description

        if not extant:
            self.columns.append(c)
        else:
            from sqlalchemy.orm import object_session

            object_session(self).merge(c)

        return c

    def add_id_column(self, description=None):
        self.add_column(name='id',datatype='integer',is_primary_key = True,
                        description = self.description if not description else description)

    @property
    def primary_key(self):
        for c in self.columns:
            if c.is_primary_key:
                return c
        return None

    def get_fixed_regex(self):
        """Using the size values for the columns for the table, construct a
        regular expression to  parsing a fixed width file."""
        import re

        pos = 0
        regex = ''
        header = []

        for col in self.columns:

            size = col.width if col.width else col.size

            if not size:
                continue

            pos += size

            regex += "(.{{{}}})".format(size)
            header.append(col.name)

        return header, re.compile(regex), regex

    def get_fixed_unpack(self):
        """Using the size values for the columns for the table, construct a
        regular expression to  parsing a fixed width file."""
        from functools import partial
        import struct
        unpack_str = ''
        header = []
        length = 0

        for col in self.columns:

            size = col.width

            if not size:
                continue

            length += size

            unpack_str += "{}s".format(size)

            header.append(col.name)

        return partial(struct.unpack, unpack_str), header, unpack_str, length

    def get_fixed_colspec(self):
        """Return the column specification suitable for use in  the Panads
        read_fwf function.

        This will ignore any columns that don't have one or both of the
        start and width values

        """

        # Warning! Assuming th start values are sorted. Really should check.

        return (
            [c.name for c in self.columns if c.start and c.width],
            [(c.start, c.start + c.width) for c in self.columns if c.start and c.width]
        )

    @property
    def null_dict(self):
        if self._null_row is None:
            self._null_row = {}
            for col in self.columns:
                if col.is_primary_key:
                    v = None
                elif col.default:
                    v = col.default
                else:
                    v = None

                self._null_row[col.name] = v

        return self._null_row

    @property
    def header(self):
        """Return an array of column names in the same order as the column
        definitions, to be used zip with a row when reading a CSV file.

        >> row = dict(zip(table.header, row))

        """

        return [c.name for c in self.columns]

    @property
    def caster(self):
        """Returns a function that takes a row that can be indexed by positions
        which returns a new row with all of the values cast to schema types."""
        from ambry.transform import CasterTransformBuilder

        bdr = CasterTransformBuilder()

        for c in self.columns:
            bdr.append(c.name, c.python_type)

        return bdr

    @property
    def dict(self):
        d = {
            k: v for k,
                     v in self.__dict__.items() if k in [
                'id', 'vid', 'd_id', 'd_vid', 'sequence_id', 'name', 'altname', 'vname', 'description', 'universe',
            'keywords',
                'installed', 'proto_vid', 'type', 'codes']}

        if self.data:
            for k in self.data:
                assert k not in d, "Value '{}' is a table field and should not be in data ".format(k)
                d[k] = self.data[k]

        d['is_geo'] = False

        for c in self.columns:
            if c in ('geometry', 'wkt', 'wkb', 'lat'):
                d['is_geo'] = True

        d['foreign_indexes'] = list(
            set([c.data['index'].split(":")[0] for c in self.columns if c.data.get('index', False)]))

        return d

    @property
    def nonull_dict(self):
        return {k: v for k, v in self.dict.items() if v and k not in 'codes'}

    @property
    def nonull_col_dict(self):

        tdc = {}

        for c in self.columns:
            tdc[c.id] = c.nonull_dict
            tdc[c.id]['codes'] = {cd.key: cd.dict for cd in c.codes}

        td = self.nonull_dict
        td['columns'] = tdc

        return td

    @property
    def insertable_dict(self):
        x = {('t_' + k).strip('_'): v for k, v in self.dict.items()}

        if not 't_vid' in x or not x['t_vid']:
            raise ValueError("Must have vid set: {} ".format(x))

        return x

    @staticmethod
    def before_insert(mapper, conn, target):
        """event.listen method for Sqlalchemy to set the seqience_id for this
        object and create an ObjectNumber value for the id"""
        if target.sequence_id is None:
            sql = text('''SELECT max(t_sequence_id)+1 FROM tables WHERE t_d_id = :did''')

            max_id, = conn.execute(sql, did=target.d_id).fetchone()

            if not max_id:
                max_id = 1

            target.sequence_id = max_id

        Table.before_update(mapper, conn, target)

    @staticmethod
    def before_update(mapper, conn, target):
        """Set the Table ID based on the dataset number and the sequence number
        for the table."""

        target.name = Table.mangle_name(target.name)

        if isinstance(target, Column):
            raise TypeError('Got a column instead of a table')

        if target.id is None:
            dataset_id = ObjectNumber.parse(target.d_id)
            target.id = str(TableNumber(dataset_id, target.sequence_id))

        if target.vid is None:
            dataset_id = ObjectNumber.parse(target.d_vid)
            target.vid = str(TableNumber(dataset_id, target.sequence_id))


event.listen(Table, 'before_insert', Table.before_insert)
event.listen(Table, 'before_update', Table.before_update)

