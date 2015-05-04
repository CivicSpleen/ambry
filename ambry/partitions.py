"""Access classess and identity for partitions.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import os

from identity import PartitionIdentity, PartitionNameQuery, PartitionName, PartialPartitionName, NameQuery
from sqlalchemy.orm.exc import NoResultFound
from util.typecheck import accepts, returns
from dbexceptions import ConflictError
from util import Constant


class Partitions(object):

    """Continer and manager for the set of partitions.

    This object is always accessed from Bundle.partitions""

    """

    STATE = Constant()
    STATE.NEW = 'new'
    STATE.BUILDING = 'building'
    STATE.BUILT = 'built'
    STATE.ERROR = 'error'
    STATE.FINALIZED = 'finalized'

    bundle = None
    _partitions = None

    def __init__(self, bundle):
        self.bundle = bundle
        self._partitions = {}

    def partition(self, arg, **kwargs):
        """Get a local partition object from either a Partion ORM object, or a
        partition name.

        :param arg:
        :param kwargs:
        Arguments:
        arg    -- a orm.Partition or Partition object.

        """

        from ambry.orm import Partition as OrmPartition
        from ambry.identity import PartitionNumber
        from identity import PartitionIdentity
        from sqlalchemy import or_

        from partition import new_partition

        session = self.bundle.database.session

        orm_partition = None

        if isinstance(arg, OrmPartition):
            orm_partition = arg

        elif isinstance(arg, basestring):

            orm_query = session.query(OrmPartition).filter(
                or_(OrmPartition.id_ == arg, OrmPartition.vid == arg))

        elif isinstance(arg, PartitionNumber):
            orm_query = session.query(OrmPartition).filter(
                OrmPartition.id_ == str(arg))

        elif isinstance(arg, PartitionIdentity):
            orm_query = session.query(OrmPartition).filter(
                OrmPartition.id_ == str(
                    arg.id_))

        else:
            raise ValueError(
                "Arg must be a Partition or PartitionNumber. Got {}".format(
                    type(arg)))

        if not orm_partition:
            orm_partition = orm_query.one()

        vid = orm_partition.vid

        if vid in self._partitions:
            return self._partitions[vid]
        else:
            p = new_partition(self.bundle, orm_partition, **kwargs)
            self._partitions[vid] = p
            return p

    @property
    def count(self):
        from ambry.orm import Partition as OrmPartition

        return (self.bundle.database.session.query(OrmPartition)
                .filter(OrmPartition.d_vid == self.bundle.dataset.vid)).count()

    @property
    def all(self):  # @ReservedAssignment
        """Return an iterator of all partitions.

        :type self: object

        """
        from ambry.orm import Partition as OrmPartition
        from sqlalchemy.orm import joinedload_all
        import sqlalchemy.exc

        try:
            ds = self.bundle.dataset

            q = (self.bundle.database.session.query(OrmPartition)
                 .filter(OrmPartition.d_vid == ds.vid)
                 .order_by(OrmPartition.vid.asc())
                 .order_by(OrmPartition.segment.asc()))

            partitions = []

            for op in q.all():
                try:
                    partitions.append(self.partition(op))
                except KeyError as e:  # Unknown partition type, usually 'hdf'
                    raise

            return partitions
        except sqlalchemy.exc.OperationalError:
            raise
            return []

    def __iter__(self):
        return iter(self.all)

    def close(self):
        for vid, p in self._partitions.items():
            p.close()

        self._partitions = {}

    def get(self, id_):
        """Get a partition by the id number.

        Arguments:
            id_ -- a partition id value

        Returns:
            A partitions.Partition object

        Throws:
            a Sqlalchemy exception if the partition either does not exist or
            is not unique

        Because this method works on the bundle, it the id_ ( without version information )
        is equivalent to the vid ( with version information )

        """
        from ambry.orm import Partition as OrmPartition
        from sqlalchemy import or_

        if isinstance(id_, PartitionIdentity):
            id_ = id_.id_

        s = self.bundle.database.session

        q = (s
             .query(OrmPartition)
             .filter(or_(
                 OrmPartition.id_ == str(id_).encode('ascii'),
                 OrmPartition.vid == str(id_).encode('ascii')
             )))

        try:
            orm_partition = q.one()

            return self.partition(orm_partition)
        except NoResultFound:
            orm_partition = None

        if not orm_partition:
            q = (s.query(OrmPartition)
                 .filter(OrmPartition.name == id_.encode('ascii')))

            try:
                orm_partition = q.one()

                return self.partition(orm_partition)
            except NoResultFound:
                orm_partition = None

        return orm_partition

    def find_table(self, table_name):
        """Return the first partition that has the given table name."""

        for partition in self.all:
            if partition.table and partition.table.name == table_name:
                return partition

        return None

    def find_id(self, id_):
        """Find a partition from an id or vid.

        :param id_:

        """

        from ambry.orm import Partition as OrmPartition
        from sqlalchemy import or_

        q = (self.bundle.database.session.query(OrmPartition)
             .filter(or_(
                 OrmPartition.id_ == str(id_).encode('ascii'),
                 OrmPartition.vid == str(id_).encode('ascii')
             )))

        return q.first()

    def find(self, pnq=None, use_library=False, **kwargs):
        """Return a Partition object from the database based on a PartitionId.

        The object returned is immutable; changes are not persisted

        """
        import sqlalchemy.orm.exc

        if pnq is None:
            pnq = PartitionNameQuery(**kwargs)

        assert isinstance(
            pnq, PartitionNameQuery), "Expected NameQuery, got {}".format(
            type(pnq))

        try:

            partitions = [
                self.partition(
                    op,
                    memory=kwargs.get(
                        'memory',
                        False)) for op in self._find_orm(pnq).all()]

            if len(partitions) == 1:
                p = partitions.pop()

                if use_library and not p.database.exists:
                    # Try to get it from the library, if it exists.
                    b = self.bundle.library.get(p.identity.vname)

                    if not b or not b.partition:
                        return p
                    else:
                        return b.partition
                else:
                    return p

            elif len(partitions) > 1:
                from ambry.dbexceptions import ResultCountError

                rl = ";\n".join([p.identity.vname for p in partitions])

                raise ResultCountError(
                    "Got too many results:  for {}\n{}".format(
                        vars(pnq),
                        rl))
            else:
                return None

        except sqlalchemy.orm.exc.NoResultFound:
            return None

    def find_all(self, pnq=None, **kwargs):
        """Return a Partition object from the database based on a PartitionId.

        The object returned is immutable; changes are not persisted

        """
        from identity import Identity
        from sqlalchemy.orm.exc import NoResultFound

        if pnq is None:
            pnq = PartitionNameQuery(**kwargs)

        try:
            ops = self._find_orm(pnq).all()
        except NoResultFound:
            from dbexceptions import NotFoundError

            raise NotFoundError(
                "Failed to find partition for '{}' ".format(
                    pnq.as_partialname()))

        return [self.partition(op) for op in ops]

    def _find_orm(self, pnq):
        """Return a Partition object from the database based on a PartitionId.

        An ORM object is returned, so changes can be persisted.

        """
        import sqlalchemy.orm.exc
        from ambry.orm import Partition as OrmPartition, Table
        from sqlalchemy.orm import joinedload_all, joinedload

        assert isinstance(
            pnq, PartitionNameQuery), "Expected PartitionNameQuery, got {}".format(
            type(pnq))

        pnq = pnq.with_none()

        q = self.bundle.database.session.query(OrmPartition)

        if pnq.fqname is not NameQuery.ANY:
            q = q.filter(OrmPartition.fqname == pnq.fqname)
        elif pnq.vname is not NameQuery.ANY:
            q = q.filter(OrmPartition.vname == pnq.vname)
        elif pnq.name is not NameQuery.ANY:
            q = q.filter(OrmPartition.name == pnq.name)
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

    def _new_orm_partition(self, pname, tables=None, data=None, memory=False):
        """Create a new ORM Partrition object, or return one if it already
        exists."""
        from ambry.orm import Partition as OrmPartition, Table
        from sqlalchemy.exc import IntegrityError

        assert isinstance(pname, PartialPartitionName), "Expected PartialPartitionName, got {}".format(
            type(pname))

        if tables and not isinstance(tables, (list, tuple, set)):
            raise ValueError(
                "If specified, 'tables' must be a list, set or tuple")

        if not data:
            data = {}

        pname = pname.promote(self.bundle.identity)

        pname.is_valid()

        session = self.bundle.database.session

        if pname.table:
            q = session.query(Table).filter( (Table.name == pname.table) | (
                    Table.id_ == pname.table))
            try:
                table = q.one()
            except:
                from dbexceptions import NotFoundError
                raise NotFoundError(
                    'Failed to find table for name or id: {}'.format(
                        pname.table))
        else:
            table = None

        if tables:
            tables = list(tables)

        if tables and pname and pname.table and pname.table not in tables:
            tables = list(tables)
            tables.append(pname.table)

        if tables:
            data['tables'] = tables

        d = pname.dict

        # Promoting to a PartitionName create the partitionName subclass from
        # the format, which is required to get the correct cache_key
        d['cache_key'] = pname.promote(self.bundle.identity.name).cache_key

        if not 'format' in d:
            d['format'] = 'db'

        try:
            del d['table']  # OrmPartition requires t_id instead
        except:
            pass

        if 'dataset' in d:
            del d['dataset']

        # This code must have the session established in the context be active.

        op = OrmPartition(
            self.bundle.get_dataset(),
            t_id=table.id_ if table else None,
            data=data,
            state=Partitions.STATE.NEW,
            **d
        )

        if memory:
            from random import randint
            from identity import ObjectNumber
            op.dataset = self.bundle.get_dataset()
            op.table = table
            op.set_ids(randint(100000, ObjectNumber.PARTMAXVAL))
            return op

        session.add(op)

        # We need to do this here to ensure that the before_commit()
        # routine is run, which sets the fqname and vid, which are needed later
        try:
            session.commit()
        except IntegrityError as e:
            from dbexceptions import ConflictError
            raise ConflictError(
                'Integrity error in database {}, while creating partition {}\n{}\n{}' .format(
                    self.bundle.database.dsn,
                    str(pname),
                    pname.cache_key,
                    e.message))

        if not op.format:
            raise Exception("Must have a format!")

        return op

    def clean(self, session):
        from ambry.orm import Partition as OrmPartition

        session.query(OrmPartition).delete()

    def _new_partition(self,ppn,tables=None,data=None,clean=False,create=True):
        """Creates a new OrmPartition record."""

        assert isinstance(ppn, PartialPartitionName), "Expected PartialPartitionName, got {}".format(
            type(ppn))

        with self.bundle.session as s:
            op = self._new_orm_partition(ppn, tables=tables, data=data)

            fqname = op.fqname

        partition = self.find(PartitionNameQuery(fqname=fqname))

        try:
            assert bool(partition), '''Failed to find partition that was just created'''
        except AssertionError:
            self.bundle.error("Failed to get partition for: created={}, fqname={}, database={} " .format(
                    ppn,fqname,self.bundle.database.dsn))
            raise

        if create:
            if tables and hasattr(partition, 'create_with_tables'):
                partition.create_with_tables(tables, clean)
            else:
                partition.create()

        partition.close()

        return partition

    def _find_or_new(self, kwargs,clean=False,format=None,tables=None,data=None,create=True):
        """Returns True if the partition was found, not created, False if it
        was created."""

        pnq = PartitionNameQuery(**kwargs)

        ppn = PartialPartitionName(**kwargs)

        if tables:
            tables = set(tables)

        if ppn.table:
            if not tables:
                tables = set()

            tables.add(ppn.table)

        if format:
            ppn.format = format
            pnq.format = format

        partition = self.find(pnq)

        if partition:
            return partition, True

        partition = self._new_partition(
            ppn,
            tables=tables,
            data=data,
            create=create)

        return partition, False

    def new_partition(self, clean=False, tables=None, data=None, **kwargs):
        return self.new_db_partition(
            clean=clean,
            tables=tables,
            data=data,
            **kwargs)

    def find_or_new(self, clean=False, tables=None, data=None, **kwargs):
        return self.find_or_new_db(
            tables=tables,
            clean=clean,
            data=data,
            **kwargs)

    def new_db_partition(
            self,
            clean=False,
            tables=None,
            data=None,
            create=True,
            **kwargs):

        p, found = self._find_or_new(
            kwargs, clean=False, tables=tables, data=data, create=create, format='db')

        if found:
            raise ConflictError("Partition {} already exists".format(p.name))

        return p

    def new_db_from_pandas(
            self,
            frame,
            table=None,
            data=None,
            load=True,
            **kwargs):
        """Create a new db partition from a pandas data frame.

        If the table does not exist, it will be created

        """
        import pandas as pd
        import numpy as np
        from orm import Column
        from dbexceptions import ConfigurationError

        # Create the table from the information in the data frame.
        with self.bundle.session:
            sch = self.bundle.schema
            t = sch.add_table(table)

            if frame.index.name:
                id_name = frame.index.name
            else:
                id_name = 'id'

            sch.add_column(
                t,
                id_name,
                datatype=Column.convert_numpy_type(
                    frame.index.dtype),
                is_primary_key=True)

            for name, type_ in zip([row for row in frame.columns],
                                   [row for row in frame.convert_objects(convert_numeric=True,
                                                                         convert_dates=True).dtypes]):
                sch.add_column(
                    t,
                    name,
                    datatype=Column.convert_numpy_type(type_))
                sch.write_schema()

        p = self.new_partition(table=table, data=data, **kwargs)

        if load:
            pk_name = frame.index.name
            with p.inserter(table) as ins:
                for i, row in frame.iterrows():
                    d = dict(row)
                    d[pk_name] = i

                    ins.insert(d)

        return p

    def find_or_new_db(self, clean=False, tables=None, data=None, **kwargs):
        """Find a partition identified by pid, and if it does not exist, create
        it.

        Args:
            pid A partition Identity
            tables. String or array of tables to copy form the main partition
            data. add a data field to the partition in the database
            clean. Clean the database when it is created
            kwargs. time,space,gran, etc; parameters to name the partition

        """

        p, _ = self._find_or_new(
            kwargs, clean=False, tables=tables, data=data, format='db')

        return p

    def new_geo_partition(
            self,
            clean=False,
            tables=None,
            data=None,
            shape_file=None,
            logger=None,
            **kwargs):
        from sqlalchemy.orm.exc import NoResultFound

        try:
            import gdal

            p, found = self._find_or_new(kwargs, format='geo')

            if found:
                raise ConflictError(
                    "Partition {} alread exists".format(
                        p.name))

            if shape_file:
                p.database.close()

                p.load_shapefile(shape_file, logger=logger)

        except ImportError:
            self.bundle.log("GDAL not installed; using non geo database")
            p, found = self._find_or_new(kwargs)

            if found:
                raise ConflictError(
                    "Partition {} alread exists".format(
                        p.name))

            if shape_file:
                from dbexceptions import RequirementError
                raise RequirementError(
                    "GDAL is not installed, so can't load a shapefile")

        return p

    def find_or_new_geo(self, clean=False, tables=None, data=None,
                        create=False, shape_file=None, **kwargs):
        """Find a partition identified by pid, and if it does not exist, create
        it.

        Args:
            pid A partition Identity
            tables String or array of tables to copy form the main partition

        """

        try:
            import gdal

            p, _ = self._find_or_new(kwargs, clean=False, tables=None,
                                     data=None, create=True, format='geo')

            if shape_file:
                p.load_shapefile(shape_file)

        except ImportError:
            self.bundle.log("GDAL not installed; using non geo database")

            p, _ = self._find_or_new(
                kwargs, clean=False, tables=None, data=None, create=True)

            if shape_file:
                from dbexceptions import RequirementError
                raise RequirementError(
                    "GDAL is not installed, so can't load a shapefile")

        return p

    def find_or_new_hdf(
            self,
            clean=False,
            tables=None,
            data=None,
            create=False,
            **kwargs):
        """
        :param clean:
        :param tables:
        :param data:
        :param create:
        :param kwargs:
        :return:
        """

        raise NotImplementedError()

        p, _ = self._find_or_new(kwargs, clean=False, tables=None,
                                 data=None, create=True, format='hdf')

        return p

    def new_memory_partition(self, tables=None, data=None, **kwargs):
        """Find a partition identified by pid, and if it does not exist, create
        it.

        Args:
            pid A partition Identity
            tables String or array of tables to copy form the main partition

        """

        from partition.sqlite import SqlitePartition
        from partition import partition_classes

        ppn = PartialPartitionName(**kwargs)

        if tables:
            tables = set(tables)

        if ppn.table:
            if not tables:
                tables = set()

            tables.add(ppn.table)

        op = self._new_orm_partition(
            ppn,
            tables=tables,
            data=data,
            memory=True)

        cls = partition_classes().partition_by_format[
            kwargs.get(
                'format',
                'db')]

        p = cls(self.bundle, op, memory=True, **kwargs)

        if tables:
            p.create_with_tables(tables)
        else:
            p.create()

        return p

    def delete(self, partition):
        from ambry.orm import Partition as OrmPartition

        q = (self.bundle.database.session.query(OrmPartition)
             .filter(OrmPartition.id_ == partition.identity.id_))

        q.delete()

    @property
    def info(self):

        out = 'Partitions: ' + str(self.count) + "\n"

        for p in self.all:
            out += str(p.identity.sname) + "\n"

        return out

    def _repr_html_(self):
        from identity import PartitionName

        active_parts = set()

        # Find out which of the name parts are being used, particularly
        # time, space, grain
        for p in self.all:
            active_parts |= set(p.name.partital_dict.keys())

        cols = ['Id', 'Vid', 'Name', 'VName']

        for np, _, _ in PartitionName._name_parts:
            if np in active_parts:
                cols.append(np.title())

        rows = ["<tr>" +
                ''.join(['<th>{}</th>'.format(c) for c in cols]) +
                "</tr>"]

        for p in self.all:
            cols = []
            d = p.name.partital_dict

            cols.append(p.identity.id_)
            cols.append(p.identity.vid)
            cols.append(p.identity.sname)
            cols.append(p.identity.vname)

            for np, _, _ in PartitionName._name_parts:

                if np not in active_parts:
                    continue

                cols.append(d[np] if np in d else '')

            rows.append("<tr>" +
                        ''.join(['<td>{}</td>'.format(c) for c in cols]) +
                        "</tr>")

        return "<table>\n" + "\n".join(rows) + "\n</table>"
