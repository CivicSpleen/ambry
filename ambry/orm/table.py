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

import six

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

    c_sequence_id = SAColumn('t_c_sequence_id', Integer, default=1)

    __table_args__ = (
        UniqueConstraint('t_sequence_id', 't_d_vid', name='_uc_tables_1'),
        UniqueConstraint('t_name', 't_d_vid', name='_uc_tables_2'),
    )

    columns = relationship(Column, backref='table', order_by='asc(Column.sequence_id)',
                           cascade='all, delete-orphan', lazy='joined')

    _column_sequence = {}

    @staticmethod
    def mangle_name(name, preserve_case=False):
        import re

        assert name

        try:
            r = re.sub('[^\w_]', '_', name.strip())

            if not preserve_case:
                r = r.lower()

            return r
        except TypeError:
            raise TypeError('Not a valid type for name ' + str(type(name)))

    @property
    def primary_columns(self):
        """Iterate over the primary columns, columns which do not have a parent"""
        for c in self.columns:
            if not c.parent:
                yield c

    @property
    def dimensions(self):
        """Iterate over the dimension columns, regardless of parent/child status

        """
        from ambry.valuetype.core import ROLE

        for c in self.columns:

            if c.role == ROLE.DIMENSION:
                yield c

    @property
    def primary_dimensions(self):
        """Iterate over the primary dimension columns, columns which do not have a parent

        """
        from ambry.valuetype.core import ROLE

        for c in self.columns:

            if not c.parent and c.role == ROLE.DIMENSION:
                    yield c

    @property
    def primary_measures(self):
        """Iterate over the primary columns, columns which do not have a parent

        Also sets the property partition_stats to the stats collection for the partition and column.
        """
        from ambry.valuetype.core import ROLE

        for c in self.columns:

            if not c.parent and c.role == ROLE.MEASURE:
                    yield c


    def column(self, ref):
        # AFAIK, all of the columns in the relationship will get loaded if any one is accessed,
        # so iterating over the collection only involves one SELECT.
        from .column import Column

        column_name = Column.mangle_name(str(ref))

        for c in self.columns:
            if str(column_name) == c.name or str(ref) == c.id or str(ref) == c.vid:
                return c

        raise NotFoundError(
            "Failed to find column '{}' in table '{}' for ref: '{}' ".format(ref, self.name, ref))

    def add_column(self, name, update_existing=False, **kwargs):
        """
        Add a column to the table, or update an existing one.
        :param name: Name of the new or existing column.
        :param update_existing: If True, alter existing column values. Defaults to False
        :param kwargs: Other arguments for the the Column() constructor
        :return: a Column object
        """
        from ..identity import ColumnNumber

        try:
            c = self.column(name)
            extant = True

            if not update_existing:
                return c

        except NotFoundError:

            sequence_id = len(self.columns) + 1

            assert sequence_id

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

                # Don't change a datatype if the value is set and the new value is unknown
                if key == 'datatype' and value == 'unknown' and c.datatype:
                    continue

                # Don't change a datatype if the value is set and the new value is unknown
                if key == 'description' and not value:
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

    def is_empty(self):
        """Return True if the table has no columns or the only column is the id"""
        if len(self.columns) == 0:
            return True

        if len(self.columns) == 1 and self.columns[0].name == 'id':
            return True

        return False

    @property
    def header(self):
        """Return an array of column names in the same order as the column
        definitions, to be used zip with a row when reading a CSV file.

        >> row = dict(zip(table.header, row))

        """

        return [c.name for c in self.columns]

    @property
    def dict(self):
        INCLUDE_FIELDS = [
            'id', 'vid', 'd_id', 'd_vid', 'sequence_id', 'name', 'altname', 'vname',
            'description', 'universe', 'keywords', 'installed', 'proto_vid', 'type', 'codes']

        d = {k: v for k, v in six.iteritems(self.__dict__) if k in INCLUDE_FIELDS}

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

    def update_id(self, sequence_id=None, force=True):
        """Alter the sequence id, and all of the names and ids derived from it. This
        often needs to be don after an IntegrityError in a multiprocessing run"""
        from ..identity import ObjectNumber

        if sequence_id:
            self.sequence_id = sequence_id

        assert self.d_vid

        if self.id is None or force:
            dataset_id = ObjectNumber.parse(self.d_vid).rev(None)
            self.d_id = str(dataset_id)
            self.id = str(TableNumber(dataset_id, self.sequence_id))

        if self.vid is None or force:
            dataset_vid = ObjectNumber.parse(self.d_vid)
            self.vid = str(TableNumber(dataset_vid, self.sequence_id))

    @property
    def transforms(self):
        """Return an array of arrays of column transforms.

        The return value is an list of list, with each list being a segment of column transformations, and
        each segment having one entry per column.

        """

        tr = []
        for c in self.columns:
            tr.append(c.expanded_transform)

        return six.moves.zip_longest(*tr)

    @property
    def row(self):
        from collections import OrderedDict
        import six

        # Use an Ordered Dict to make it friendly to creating CSV files.
        SKIP_KEYS = ['id', 'd_id', 'd_vid', 'dataset', 'columns', 'data',
                     'partitions', 'sources', 'process_records']

        d = OrderedDict([(p.key, getattr(self, p.key)) for p in self.__mapper__.attrs
                         if p.key not in SKIP_KEYS])

        for k, v in six.iteritems(self.data):
            d['d_' + k] = v

        return d

    def __str__(self):
        from tabulate import tabulate

        headers = 'Seq Vid Name Datatype ValueType'.split()
        rows = [(c.sequence_id, c.vid, c.name, c.datatype, c.valuetype) for c in self.columns]

        return ('Dest Table: {}\n'.format(self.name)) + tabulate(rows, headers)

    def _repr_html_(self):
        from tabulate import tabulate
        from ambry.util import drop_empty

        def record_gen():
            for i, row in enumerate([c.row for c in self.columns]):
                if i == 0:
                    yield row.keys()
                yield row.values()

        records = list(record_gen())

        records = drop_empty(records)

        return "<h2>{}</h2>".format(self.name)+tabulate(records[1:], headers=records[0], tablefmt="html")

    @staticmethod
    def before_insert(mapper, conn, target):
        """event.listen method for Sqlalchemy to set the seqience_id for this
        object and create an ObjectNumber value for the id"""
        if target.sequence_id is None:
            from ambry.orm.exc import DatabaseError
            raise DatabaseError('Must have sequence id before insertion')

        Table.before_update(mapper, conn, target)

    @staticmethod
    def before_update(mapper, conn, target):
        """Set the Table ID based on the dataset number and the sequence number
        for the table."""

        target.name = Table.mangle_name(target.name)

        if isinstance(target, Column):
            raise TypeError('Got a column instead of a table')

        target.update_id(target.sequence_id, False)


event.listen(Table, 'before_insert', Table.before_insert)
event.listen(Table, 'before_update', Table.before_update)
