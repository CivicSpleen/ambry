"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'

from sqlalchemy import Column as SAColumn, Integer
from sqlalchemy import String
from sqlalchemy.orm import relationship, object_session

from six import string_types

from . import Base, MutationDict, JSONEncodedObj, MutationList
from .config import ConfigGroupAccessor

from ambry.identity import DatasetNumber, ObjectNumber
from ambry.orm.file import File


class Dataset(Base):
    __tablename__ = 'datasets'

    vid = SAColumn('d_vid', String(13), primary_key=True)
    id = SAColumn('d_id', String(10))
    name = SAColumn('d_name', String(200), nullable=False, index=True)
    vname = SAColumn('d_vname', String(200), unique=True, nullable=False, index=True)
    fqname = SAColumn('d_fqname', String(200), unique=True, nullable=False)
    cache_key = SAColumn('d_cache_key', String(200), unique=True, nullable=False, index=True)
    source = SAColumn('d_source', String(200), nullable=False)
    dataset = SAColumn('d_dataset', String(200), nullable=False)
    subset = SAColumn('d_subset', String(200))
    variation = SAColumn('d_variation', String(200))
    btime = SAColumn('d_btime', String(200))
    bspace = SAColumn('d_bspace', String(200))
    revision = SAColumn('d_revision', Integer, nullable=False)
    version = SAColumn('d_version', String(20), nullable=False)

    space_coverage = SAColumn('d_scov', MutationList.as_mutable(JSONEncodedObj))
    time_coverage = SAColumn('d_tcov', MutationList.as_mutable(JSONEncodedObj))
    grain_coverage = SAColumn('d_gcov', MutationList.as_mutable(JSONEncodedObj))

    p_sequence_id = SAColumn('d_p_sequence_id', Integer, default=1)
    t_sequence_id = SAColumn('d_t_sequence_id', Integer, default=1)
    st_sequence_id = SAColumn('d_st_sequence_id', Integer, default=1)

    state = SAColumn('d_state', String(20), doc='Indicates last operation on the dataset') # Note! Different from Bundle.state!

    upstream = SAColumn('d_upstream', String(200), doc='The URL of the upstream source')

    data = SAColumn('d_data', MutationDict.as_mutable(JSONEncodedObj))

    # ----

    tables = relationship('Table', backref='dataset', cascade='all, delete-orphan')

    partitions = relationship('Partition', backref='dataset', cascade='all, delete-orphan')

    configs = relationship('Config', backref='dataset', cascade='all, delete-orphan')

    files = relationship('File', backref='dataset', cascade='all, delete-orphan')

    source_tables = relationship('SourceTable', backref='dataset', cascade='all, delete-orphan')

    source_columns = relationship('SourceColumn', backref='dataset', cascade='all, delete-orphan')

    sources = relationship('DataSource', backref='dataset', cascade='all, delete-orphan')

    codes = relationship('Code', backref='dataset', cascade='all, delete-orphan')

    path = None  # Set by the Library and other queries.
    _database = None  # Reference to the database, when dataset is retrieved from a database object

    _sequence_ids = {}  # Cache of sequence numbers ( Is this still used? )

    def __init__(self, *args, **kwargs):

        super(Dataset, self).__init__(*args, **kwargs)

        if self.vid and not self.id:
            self.revision = ObjectNumber.parse(self.vid).revision
            self.id = str(ObjectNumber.parse(self.vid).rev(None))

        if not self.id:
            dn = DatasetNumber(None, self.revision)
            self.vid = str(dn)
            self.id = str(dn.rev(None))
        elif not self.vid:
            try:
                self.vid = str(ObjectNumber.parse(self.id).rev(self.revision))
            except ValueError as e:
                raise ValueError('Could not parse id value; ' + e.message)

        if not self.revision:
            self.revision = 1

        if self.cache_key is None:
            self.cache_key = self.identity.name.cache_key

        if not self.name:
            self.name = str(self.identity.name)

        if not self.vname:
            self.vname = str(self.identity.vname)

        if not self.fqname:
            self.fqname = str(self.identity.fqname)

        if not self.version:
            self.version = str(self.identity.version)

        assert self.vid[0] == 'd'

    def incver(self):
        """Increment all of the version numbers"""
        d = {}
        for p in self.__mapper__.attrs:
            if p.key in ['vid','vname','fqname', 'version', 'cache_key']:
                continue
            if p.key == 'revision':
                d[p.key] = self.revision + 1
            else:
                d[p.key] = getattr(self, p.key)

        n =  Dataset(**d)

        return n


    def commit(self):
        self._database.commit()

    def rollback(self):
        self._database.rollback()

    def rollback(self):
        self._database.close()

    @property
    def session(self):
        return self._database.session

    def query(self, *args, **kwargs):
        return self.session.query(*args, **kwargs)

    def close(self):
        return self._database.close()

    def close_session(self):
        return self._database.close_session()

    @property
    def identity(self):
        from ..identity import Identity
        return Identity.from_dict(self.dict)

    @property
    def config(self):
        return ConfigAccessor(self)

    def next_sequence_id(self, table_class, force_query=False):
        """Return the next sequence id for a object, identified by the vid of the parent object, and the database prefix
        for the child object. On the first call, will load the max sequence number
        from the database, but subsequence calls will run in process, so this isn't suitable for
        multi-process operation -- all of the tables in a dataset should be created by one process

        The child table must have a sequence_id value.

        """

        from . import next_sequence_id
        from sqlalchemy.orm import object_session

        # NOTE: This next_sequence_id uses a different algorithm than dataset.next_sequence_id
        # FIXME replace this one with dataset.next_sequence_id
        return next_sequence_id(object_session(self), self._sequence_ids, self.vid, table_class, force_query=force_query)

    def new_unique_object(self, table_class, sequence_id=None, force_query=False, **kwargs):
        """Use next_sequence_id to create a new child of the dataset, with a unique id"""
        from sqlalchemy.exc import IntegrityError
        from sqlalchemy.orm.exc import FlushError

        # If a sequence ID was specified, the caller is certain
        #  that there is no potential for conflicts,
        # so there is no need to commit here.
        if not sequence_id:
            commit = True
            sequence_id = self.next_sequence_id(table_class, force_query=force_query)
        else:
            commit = False

        o = table_class(
            d_vid=self.vid,
            **kwargs
        )

        o.update_id(sequence_id)

        if commit is False:
            return o

        self.commit()

        if self._database.driver == 'sqlite':
            # The Sqlite database can't have concurrency, so there no problem.
            self.session.add(o)
            self.commit()
            return o
        else: # Postgres. Concurrency is a bitch.
            table_name = table_class.__tablename__
            child_sequence_id = table_class.sequence_id.property.columns[0].name

        try:

            self.session.add(o)
            self.commit()
            return o

        except (IntegrityError, FlushError) as e:
            self.rollback()
            self.session.merge(self)
            print 'Failed'
            return None

        return
        # This is horrible, but it's the only thing that has worked for both
        # Sqlite and Postgres in both single processes and multiprocesses.
        d_vid = self.vid
        while True:
            try:
                self.session.add(o)
                self.commit()
                return o

            except (IntegrityError, FlushError) as e:

                self.rollback()

                self.session.expunge_all()
                ds = self._database.dataset(d_vid)
                sequence_id = ds.next_sequence_id(table_class, force_query=True)

                o.update_id(sequence_id)

            except Exception as e:

                print('Completely failed to get a new {} sequence_id; {}'.format(table_class, e))
                self.rollback()
                import traceback

                # This bit is helpful in a multiprocessing run.
                tb = traceback.format_exc()

                print(tb)
                raise

    def table(self, ref):
        from .exc import NotFoundError
        from .table import Table

        table_name = Table.mangle_name(str(ref))

        for t in self.tables:
            if table_name == t.name or str(ref) == t.id or str(ref) == t.vid:
                return t

        raise NotFoundError("Failed to find table for ref '{}' in dataset '{}'".format(ref, self.name))

    def new_table(self, name, add_id=True, **kwargs):
        '''Add a table to the schema, or update it it already exists.

        If updating, will only update data.
        '''
        from . import Table
        from .exc import NotFoundError

        try:
            table = self.table(name)
            extant = True
        except NotFoundError:

            extant = False

            if 'sequence_id' not in kwargs:
                kwargs['sequence_id'] = self._database.next_sequence_id(Dataset, self.vid, Table)

            table = Table(name=name, d_vid=self.vid, **kwargs)

            table.update_id()

        # Update possibly extant data
        table.data = dict(
            (list(table.data.items()) if table.data else []) + list(kwargs.get('data', {}).items()))

        for key, value in list(kwargs.items()):

            if not key:
                continue
            if key[0] != '_' and key not in ['vid', 'id', 'id_', 'd_id', 'name', 'sequence_id', 'table', 'column', 'data']:
                setattr(table, key, value)

        if add_id:
            table.add_id_column()

        if not extant:
            self.tables.append(table)

        return table

    def new_partition(self, table, **kwargs):
        """ Creates new partition and returns it.

        Args:
            table (orm.Table):

        Returns:
            orm.Partition
        """

        from . import Partition

        # Create the basic partition record, with a sequence ID.

        if isinstance(table, string_types):
            table = self.table(table)

        if 'sequence_id' in kwargs:
            sequence_id = kwargs['sequence_id']
            del kwargs['sequence_id']
        else:
            sequence_id = self._database.next_sequence_id(Dataset, self.vid, Partition)

        p = Partition(
            t_vid=table.vid,
            table_name=table.name,
            sequence_id=sequence_id,
            dataset=self,
            d_vid=self.vid,
            **kwargs
        )

        p.update_id()

        return p

    def partition(self, ref=None, **kwargs):
        """ Returns partition by ref. """
        from .exc import NotFoundError
        from six import text_type

        if ref:

            for p in self.partitions:
                if (text_type(ref) == text_type(p.name) or text_type(ref) == text_type(p.id) or
                            text_type(ref) == text_type(p.vid)):
                    return p

            raise NotFoundError("Failed to find partition for ref '{}' in dataset '{}'".format(ref, self.name))

        elif kwargs:
            from ..identity import PartitionNameQuery

            pnq = PartitionNameQuery(**kwargs)
            return self._find_orm

    def _find_orm(self, pnq):
        """Return a Partition object from the database based on a PartitionId.

        An ORM object is returned, so changes can be persisted.

        """
        # import sqlalchemy.orm.exc
        from ..identity import PartitionNameQuery, NameQuery
        from ambry.orm import Partition as OrmPartition  # , Table
        from sqlalchemy.orm import joinedload  # , joinedload_all

        assert isinstance(pnq, PartitionNameQuery), "Expected PartitionNameQuery, got {}".format(type(pnq))

        pnq = pnq.with_none()

        q = self.bundle.database.session.query(OrmPartition)

        if pnq.fqname is not NameQuery.ANY:
            q = q.filter(OrmPartition.fqname == pnq.fqname)
        elif pnq.vname is not NameQuery.ANY:
            q = q.filter(OrmPartition.vname == pnq.vname)
        elif pnq.name is not NameQuery.ANY:
            q = q.filter(OrmPartition.name == str(pnq.name))
        else:
            if pnq.time is not NameQuery.ANY:
                q = q.filter(OrmPartition.time == pnq.time)

            if pnq.space is not NameQuery.ANY:
                q = q.filter(OrmPartition.space == pnq.space)

            if pnq.grain is not NameQuery.ANY:
                q = q.filter(OrmPartition.grain == pnq.grain)

            if pnq.format is not NameQuery.ANY:
                q = q.filter(OrmPartition.format == pnq.format)

            if pnq.segment is not NameQuery.ANY:
                q = q.filter(OrmPartition.segment == pnq.segment)

            if pnq.table is not NameQuery.ANY:

                if pnq.table is None:
                    q = q.filter(OrmPartition.t_id is None)
                else:
                    tr = self.bundle.schema.table(pnq.table)

                    if not tr:
                        raise ValueError(
                            "Didn't find table named {} in {} bundle path = {}".format(
                                pnq.table,
                                pnq.vname,
                                self.bundle.database.path))

                    q = q.filter(OrmPartition.t_id == tr.id_)

        ds = self.bundle.dataset

        q = q.filter(OrmPartition.d_vid == ds.vid)

        q = q.order_by(
            OrmPartition.vid.asc()).order_by(
            OrmPartition.segment.asc())

        q = q.options(joinedload(OrmPartition.table))

        return q

    def delete_tables_partitions(self):
        self.t_sequence_id = 1
        self.p_sequence_id = 1
        return self._database.delete_tables_partitions(self)


    def delete_partitions(self):
        self.p_sequence_id = 1
        return self._database.delete_partitions(self)

    def new_source(self, name, **kwargs):
        from .source import DataSource
        from ..identity import GeneralNumber1

        if 'sequence_id' not in kwargs:
            kwargs['sequence_id'] = self.next_sequence_id(DataSource)

        if 'd_vid' not in kwargs:
            kwargs['d_vid'] = self.vid
        else:
            assert kwargs['d_vid'] == self.vid

        if 'vid' not in kwargs:
            kwargs['vid'] = str(GeneralNumber1('S', self.vid, int(kwargs['sequence_id'])))

        source = DataSource(name=name, **kwargs)

        object_session(self).add(source)

        return source


    def source_file(self, name):
        from .source import DataSource

        source = object_session(self)\
            .query(DataSource)\
            .filter(DataSource.name == name)\
            .filter(DataSource.d_vid == self.vid)\
            .first()

        if not source:  # Try as a source vid
            source = object_session(self) \
                .query(DataSource) \
                .filter(DataSource.vid == name) \
                .filter(DataSource.d_vid == self.vid) \
                .first()

        if not source:
            from .exc import NotFoundError
            raise NotFoundError("Failed to find source for name : '{}' ".format(name))

        return source

    def new_source_table(self, name, sequence_id=None):
        from .source_table import SourceTable

        extant = next(iter(e for e in self.source_tables if e.name == name), None)

        if extant:
            return extant

        if not sequence_id:
            sequence_id = self._database.next_sequence_id(Dataset, self.vid, SourceTable)

        assert sequence_id

        table = SourceTable(name=name, d_vid=self.vid, sequence_id=sequence_id)

        table.update_id()

        self.source_tables.append(table)

        assert table.sequence_id

        return table

    def source_table(self, name):

        for st in self.source_tables:
            if st.name == name:
                return st

        return None

    def bsfile(self, path):
        """Return a Build Source file ref, creating a new one if the one requested does not exist"""
        from sqlalchemy.orm.exc import NoResultFound
        from ambry.orm.exc import NotFoundError

        try:

            f =  object_session(self)\
                .query(File)\
                .filter(File.d_vid == self.vid)\
                .filter(File.major_type == File.MAJOR_TYPE.BUILDSOURCE)\
                .filter(File.path == path)\
                .one()

            return f

        except NoResultFound:
            raise NotFoundError("Failed to find file for path '{}' ".format(path))

    def new_bsfile(self, file_const, path):
        import time

        fr = File(d_vid=self.vid,
                  major_type=File.MAJOR_TYPE.BUILDSOURCE,
                  minor_type=file_const,
                  path=path,
                  #modified = int(time.time()), # In case content isn't set, which is where modified is set normally
                  source='fs')

        self.files.append(fr)
        object_session(self).add(fr)
        return fr

    def find_or_new_bsfile(self, file_const, path):
        from ambry.orm.exc import NotFoundError
        try:
            return self.bsfile(path)
        except NotFoundError:
            import time
            f = self.new_bsfile(file_const, path)
            return f




    @property
    def dict(self):
        d = {
            'id': self.id,
            'vid': self.vid,
            'name': self.name,
            'vname': self.vname,
            'fqname': self.fqname,
            'cache_key': self.cache_key,
            'source': self.source,
            'dataset': self.dataset,
            'subset': self.subset,
            'variation': self.variation,
            'btime': self.btime,
            'bspace': self.bspace,
            'revision': self.revision,
            'version': self.version,
            'upstream': self.upstream
        }

        if self.data:
            for k in self.data:
                assert k not in d
                d[k] = self.data[k]

        return d

    def row(self, fields):
        """Return a row for fields, for CSV files, pretty printing, etc, give a set of fields to return"""

        d = self.dict

        row = [None] * len(fields)

        for i, f in enumerate(fields):
            if f in d:
                row[i] = d[f]

        return row

    def __repr__(self):
        return """<datasets: id={} vid={} name={} source={} ds={} ss={} var={} rev={}>""".format(
            self.id,
            self.vid,
            self.name,
            self.source,
            self.dataset,
            self.subset,
            self.variation,
            self.revision)

class ConfigAccessor(object):

    def __init__(self, dataset):

        self.dataset = dataset

    @property
    def metadata(self):
        """Access process configuarion values as attributes. See self.process
        for a usage example"""
        from ambry.metadata.schema import Top  # cross-module import
        top = Top()
        top.build_from_db(self.dataset)
        return top

    @property
    def sync(self):
        """Access sync configuration values as attributes. See self.process for a usage example"""
        return ConfigGroupAccessor(self.dataset, 'sync')

    @property
    def requirements(self):
        """Access sync configuration values as attributes. See self.process for a usage example"""
        return ConfigGroupAccessor(self.dataset, 'requirements')

    @property
    def library(self):
        """Access library configuration values as attributes. The library config
         is really only relevant to the root dataset. See self.process for a usage example"""
        return ConfigGroupAccessor(self.dataset, 'library')

    def rows(self):
        """Return configuration in a form that can be used to reconstitute a
        Metadata object. Returns all of the rows for a dataset.

        This is distinct from get_config_value, which returns the value
        for the library.

        """
        from ambry.orm import Config as SAConfig
        from sqlalchemy import or_

        rows = []
        configs = self.dataset.session\
            .query(SAConfig)\
            .filter(or_(SAConfig.group == 'config', SAConfig.group == 'process'),
                    SAConfig.d_vid == self.dataset.vid)\
            .all()

        for r in configs:
            parts = r.key.split('.', 3)

            if r.group == 'process':
                parts = ['process'] + parts

            cr = ((parts[0] if len(parts) > 0 else None,
                   parts[1] if len(parts) > 1 else None,
                   parts[2] if len(parts) > 2 else None
                   ), r.value)

            rows.append(cr)

        return rows
