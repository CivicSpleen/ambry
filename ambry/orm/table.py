"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy import event
from sqlalchemy import Column as SAColumn, Integer, UniqueConstraint
from sqlalchemy import  Text, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import text
import sqlalchemy.orm


from ambry.identity import TableNumber,  ObjectNumber
from ambry.orm import NotFoundError, Column, File
from . import Base, MutationDict, JSONEncodedObj



class Table(Base):
    __tablename__ = 'tables'

    vid = SAColumn('t_vid', String(20), primary_key=True)
    id_ = SAColumn('t_id', String(20), primary_key=False)
    d_id = SAColumn('t_d_id', String(20))
    d_vid = SAColumn('t_d_vid',String(20),ForeignKey('datasets.d_vid'),index=True)
    # This is a freign key, but is not declared as such
    p_vid = SAColumn('t_p_vid', String(20), index=True, nullable=True)
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
        #ForeignKeyConstraint([d_vid, d_location], ['datasets.d_vid', 'datasets.d_location']),
        UniqueConstraint('t_sequence_id', 't_d_vid', name='_uc_tables_1'),
        UniqueConstraint('t_name', 't_d_vid', name='_uc_tables_2'),
    )

    columns = relationship(Column, backref='table', order_by="asc(Column.sequence_id)",
                           cascade="merge, delete, delete-orphan", lazy='joined')


    def __init__(self, dataset, **kwargs):

        assert 'proto' not in kwargs

        self.sequence_id = kwargs.get("sequence_id", None)
        self.name = kwargs.get("name", None)
        self.vname = kwargs.get("vname", None)
        self.altname = kwargs.get("altname", None)
        self.description = kwargs.get("description", None)
        self.universe = kwargs.get("universe", None)
        self.keywords = kwargs.get("keywords", None)
        self.type = kwargs.get("type", 'table')
        self.proto_vid = kwargs.get("proto_vid")
        self.data = kwargs.get("data", None)

        self.d_id = dataset.id_
        self.d_vid = dataset.vid

        don = ObjectNumber.parse(dataset.vid)
        ton = TableNumber(don, self.sequence_id)

        self.vid = str(ton)
        self.id_ = str(ton.rev(None))

        if self.name:
            self.name = self.mangle_name(self.name, kwargs.get('preserve_case',False))

        self.init_on_load()

    @property
    def dict(self):
        d = {
            k: v for k,
            v in self.__dict__.items() if k in [
                'id_','vid','d_id','d_vid','sequence_id','name','altname','vname','description','universe','keywords',
                'installed','proto_vid','type','codes']}

        if self.data:
            for k in self.data:
                assert k not in d, "Value '{}' is a table field and should not be in data ".format(k)
                d[k] = self.data[k]

        d['is_geo'] = False

        for c in self.columns:
            if c in ('geometry', 'wkt', 'wkb', 'lat'):
                d['is_geo'] = True

        d['foreign_indexes'] =  list(set([c.data['index'].split(":")[0] for c in self.columns if c.data.get('index',False)]))

        return d

    @property
    def nonull_dict(self):
        return {k: v for k, v in self.dict.items() if v and k not in 'codes'}

    @property
    def nonull_col_dict(self):

        tdc = {}

        for c in self.columns:
            tdc[c.id_] = c.nonull_dict
            tdc[c.id_]['codes'] = {cd.key: cd.dict for cd in c.codes}

        td = self.nonull_dict
        td['columns'] = tdc

        return td

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


    @property
    def insertable_dict(self):
        x = {('t_' + k).strip('_'): v for k, v in self.dict.items()}

        if not 't_vid' in x or not x['t_vid']:
            raise ValueError("Must have vid set: {} ".format(x))

        return x

    # For linking tables to manifests
    @property
    def linked_files(self):
        return self._get_link_array('files', File, File.ref)

    def link_file(self, f):
        return self._append_link('files', f.ref)

    def delink_file(self, f):
        return self._remove_link('files', f.ref)

    @property
    def info(self):

        x =  """
------ Table: {name} ------
id   : {id_}
vid  : {vid}
name : {name}
Columns:
""".format(**self.dict)

        for c in self.columns:
            # ['id','vid','sequence_id', 't_vid', 'name', 'description', 'keywords', 'datatype', 'size', 'is_primary_kay', 'data']}

            x += "   {sequence_id:3d} {name:12s} {schema_type:8s} {description}\n".format(
                **c.dict)

        return x


    @orm.reconstructor
    def init_on_load(self):
        self._or_validator = None
        self._and_validator = None
        self._null_row = None
        self._row_hasher = None

    @staticmethod
    def before_insert(mapper, conn, target):
        """event.listen method for Sqlalchemy to set the seqience_id for this
        object and create an ObjectNumber value for the id_"""
        if target.sequence_id is None:
            sql = text(
                '''SELECT max(t_sequence_id)+1 FROM tables WHERE t_d_id = :did''')

            max_id, = conn.execute(sql, did=target.d_id).fetchone()

            if not max_id:
                max_id = 1

            target.sequence_id = max_id

        Table.before_update(mapper, conn, target)

    @staticmethod
    def before_update(mapper, conn, target):
        """Set the Table ID based on the dataset number and the sequence number
        for the table."""
        if isinstance(target, Column):
            raise TypeError('Got a column instead of a table')

        if target.id_ is None:
            dataset_id = ObjectNumber.parse(target.d_id)
            target.id_ = str(TableNumber(dataset_id, target.sequence_id))


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

    def add_column(self, name, **kwargs):
        """Add a column to the table, or update an existing one."""

        import sqlalchemy.orm.session


        s = sqlalchemy.orm.session.Session.object_session(self)

        assert s, "Can't create column with this method unless the table has a session"

        name = Column.mangle_name(name)

        if not kwargs.get('fast', False):
            try:
                row = self.column(name)
            except NotFoundError:
                row = None
        else:
            row = None

        if row:
            extant = True

        else:
            row = Column(self, name=name, **kwargs)
            extant = False

        if kwargs.get('data', False):
            row.data = dict(row.data.items() + kwargs['data'].items())

        for key, value in kwargs.items():

            excludes = ['d_id', 't_id', 'name', 'schema_type', 'data']

            # Proto is the name of the object.
            if key == 'proto' and isinstance(value, basestring):
                key = 'proto_vid'

            if extant:
                excludes.append('sequence_id')

            if key[0] != '_' and key not in excludes:
                try:
                    setattr(row, key, value)
                except AttributeError:
                    raise AttributeError(
                        "Column record has no attribute {}".format(key))

            if isinstance(value, basestring) and len(value) == 0:
                if key == 'is_primary_key':
                    value = False
                    setattr(row, key, value)

        # If the id column has a description and the table does not, add it to
        # the table.
        if row.name == 'id' and row.is_primary_key and not self.description:
            self.description = row.description
            s.merge(self)

        if extant:
            row = s.merge(row)
        else:
            s.add(row)

        if kwargs.get('commit', True):
            s.commit()

        return row

    def add_id_column(self):
        self.add_column(name='id',datatype='integer',is_primary_key = True, description = self.description)

    def column(self, ref, default=None):


        # AFAIK, all of the columns in the relationship will get loaded if any one is accessed,
        # so iterating over the collection only involved one SELECT.

        for c in self.columns:
            if str(ref) == c.name or str(ref) == c.id_ or str(ref) == c.vid:
                return c

        raise NotFoundError("Failed to find column '{}' in table '{}' for ref: '{}' "
                            .format(ref,self.name, ref))

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
    def null_row(self):
        if self._null_row is None:
            self._null_row = []
            for col in self.columns:
                if col.is_primary_key:
                    v = None
                elif col.default:
                    v = col.default
                else:
                    v = None

                self._null_row.append(v)

        return self._null_row

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

    def _get_validator(self, and_join=True):
        """Return a lambda function that, when given a row to this table,
        returns true or false to indicate the validitity of the row.

        :param and_join: If true, join multiple column validators with AND, other
        wise, OR
        :type and_join: Bool

        :rtype: a `LibraryDb` object

        """

        f = prior = lambda row: True
        first = True
        for i, col in enumerate(self.columns):

            if col.data.get('mandatory', False):
                default_value = col.default
                index = i

                if and_join:
                    f = lambda row, default_value=default_value, index=index, prior=prior: prior(
                        row) and str(row[index]) != str(default_value)
                elif first:
                    # OR joins would either need the initial F to be 'false',
                    # or just don't use it
                    f = lambda row, default_value=default_value, index=index: str(
                        row[index]) != str(default_value)
                else:
                    f = lambda row, default_value=default_value, index=index, prior=prior: prior(
                        row) or str(row[index]) != str(default_value)

                prior = f
                first = False

        return f

    def validate_or(self, values):

        if self._or_validator is None:
            self._or_validator = self._get_validator(and_join=False)

        return self._or_validator(values)

    def validate_and(self, values):

        if self._and_validator is None:
            self._and_validator = self._get_validator(and_join=True)

        return self._and_validator(values)

    def _get_hasher(self):
        """Return a  function to generate a hash for the row."""
        import hashlib

        # Try making the hash set from the columns marked 'hash'
        indexes = [i for i, c in enumerate(self.columns) if
                   c.data.get('hash', False) and not c.is_primary_key]

        # Otherwise, just use everything by the primary key.
        if len(indexes) == 0:
            indexes = [
                i for i,
                c in enumerate(
                    self.columns) if not c.is_primary_key]

        def hasher(values):
            m = hashlib.md5()
            for index in indexes:
                x = values[index]
                try:
                    m.update(
                        x.encode('utf-8') +
                        '|')  # '|' is so 1,23,4 and 12,3,4 aren't the same
                except:
                    m.update(str(x) + '|')
            return int(m.hexdigest()[:14], 16)

        return hasher

    def row_hash(self, values):
        """Calculate a hash from a database row."""

        if self._row_hasher is None:
            self._row_hasher = self._get_hasher()

        return self._row_hasher(values)

    @property
    def caster(self):
        """Returns a function that takes a row that can be indexed by positions
        which returns a new row with all of the values cast to schema types."""
        from ambry.transform import CasterTransformBuilder

        bdr = CasterTransformBuilder()

        for c in self.columns:
            bdr.append(c.name, c.python_type)

        return bdr

    def add_installed_name(self, name):
        self._append_string_to_list('installed_names', name)



event.listen(Table, 'before_insert', Table.before_insert)
event.listen(Table, 'before_update', Table.before_update)