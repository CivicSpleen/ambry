"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'

from sqlalchemy import event
from sqlalchemy import Column as SAColumn, Integer, UniqueConstraint
from sqlalchemy import Text, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import text

from ambry.identity import TableNumber,  ObjectNumber
from ambry.orm import Column, DictableMixin
from ambry.orm.exc import NotFoundError
from . import Base, MutationDict, JSONEncodedObj


class Table(Base, DictableMixin):
    __tablename__ = 'tables'

    vid = SAColumn('t_vid', String(15), primary_key=True)
    id = SAColumn('t_id', String(12), primary_key=False)
    d_id = SAColumn('t_d_id', String(10))
    d_vid = SAColumn('t_d_vid', String(13), ForeignKey('datasets.d_vid'), index=True)

    sequence_id = SAColumn('t_sequence_id', Integer, nullable=False)
    name = SAColumn('t_name', String(200), nullable=False)
    altname = SAColumn('t_altname', Text)
    summary = SAColumn('t_summary', Text)
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

    _column_sequence = {}

    def link_columns(self, other):
        """Return columns that can be used to link another table to this one"""

        def protos(t):

            protos = {}

            protos.update({c.fk_vid: c for c in t.columns if c.fk_vid})
            protos.update({c.proto_vid: c for c in t.columns if c.proto_vid})

            # HACK: The numbering in the proto dataset changes, so we have to make substitutions
            if 'c00104002' in protos:
                protos['c00109003'] = protos['c00104002']
                del protos['c00104002']

            protos = {str(ObjectNumber.parse(n).rev(None)): c for n, c in list(protos.items())}  # Remove revisions

            return protos

        protos_s = protos(self)
        protos_o = protos(other)

        inter = set(protos_s.keys()) & set(protos_o.keys())

        return list(set((protos_s[n], protos_o[n]) for n in inter))

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
        from .column import Column

        column_name = Column.mangle_name(str(ref))

        for c in self.columns:
            if str(column_name) == c.name or str(ref) == c.id or str(ref) == c.vid:
                return c

        raise NotFoundError("Failed to find column '{}' in table '{}' for ref: '{}' ".format(ref, self.name, ref))


    def add_column(self, name, update_existing = False, **kwargs):
        """
        Add a column to the table, or update an existing one.
        :param name: Name of the new or existing column.
        :param update_existing: If True, alter existing column values. Defaults to False
        :param kwargs: Other arguments for the the Column() constructor
        :return: a Column object
        """
        from . import next_sequence_id
        from sqlalchemy.orm import object_session
        from ..identity import ColumnNumber

        try:
            c = self.column(name)
            extant = True

            if not update_existing:
                return c

        except NotFoundError:

            sequence_id = next_sequence_id(object_session(self), self._column_sequence, self.vid, Column)

            assert sequence_id > len(self.columns), "{}: {} ! > {} ".format(name, sequence_id, len(self.columns))

            c = Column(t_vid=self.vid,
                       sequence_id=sequence_id,
                       vid=str(ColumnNumber(ObjectNumber.parse(self.vid), sequence_id)),
                       name=name,
                       datatype='str')
            extant = False

        # Update possibly existing data
        c.data = dict((list(c.data.items()) if c.data else []) + list(kwargs.get('data', {}).items()))

        for key, value in list(kwargs.items()):

            if key[0] != '_' and key not in ['t_vid', 'name',  'sequence_id', 'data']:

                # Don't update the type if the user has specfied a custom type
                if key == 'datatype' and not c.type_is_builtin():
                    continue

                try:
                    setattr(c, key, value)
                except AttributeError:
                    raise AttributeError("Column record has no attribute {}".format(key))

            if key == 'is_primary_key' and isinstance(value, str) and len(value) == 0:
                value = False
                setattr(c, key, value)

        # If the id column has a description and the table does not, add it to
        # the table.
        if c.name == 'id' and c.is_primary_key and not self.description:
            self.description = c.description



        if not extant:
            self.columns.append(c)

        return c

    def add_id_column(self, description=None):
        from . import Column
        self.add_column(name='id', datatype=Column.DATATYPE_INTEGER, is_primary_key=True,
                        description=self.description if not description else description)




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
    def dict(self):
        d = {
            k: v for k,
                     v in list(self.__dict__.items()) if k in [
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
        return {k: v for k, v in list(self.dict.items()) if v and k not in 'codes'}

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
        x = {('t_' + k).strip('_'): v for k, v in list(self.dict.items())}

        if 't_vid' not in x or not x['t_vid']:
            raise ValueError("Must have vid set: {} ".format(x))

        return x

    def update_from_stats(self, stats):
        """Update columns based on partition statistics"""

        sd = dict(stats)

        for c in self.columns:

            if c not in sd:
                continue

            stat = sd[c]

            if stat.size and stat.size > c.size:
                c.size = stat.size

            c.lom = stat.lom

    def __str__(self):
        from tabulate import tabulate

        headers = "Seq Vid Name Datatype ".split()
        rows = [ (c.sequence_id, c.vid, c.name, c.datatype ) for c in self.columns ]

        return ('Dest Table: {}\n'.format(self.name)) + tabulate(rows, headers)



    @staticmethod
    def before_insert(mapper, conn, target):
        """event.listen method for Sqlalchemy to set the seqience_id for this
        object and create an ObjectNumber value for the id"""
        if target.sequence_id is None:
            from ambry.orm.exc import DatabaseError
            raise DatabaseError("Must have sequence id before insertion")

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
