"""Access classess and identity for partitions.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

# import os

from sqlalchemy.orm.exc import NoResultFound

from six import iteritems

from ..identity import PartitionIdentity, PartitionNameQuery, NameQuery  # , PartitionName


# from util.typecheck import accepts, returns
from ..util import Constant


class Partitions(object):

    """Container and manager for the set of partitions.

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

    def partition(self, id_):
        """Get a partition by the id number.

        Arguments:
            id_ -- a partition id value

        Returns:
            A partitions.Partition object

        Throws:
            a Sqlalchemy exception if the partition either does not exist or
            is not unique

        Because this method works on the bundle, the id_ ( without version information )
        is equivalent to the vid ( with version information )

        """
        from ..orm import Partition as OrmPartition
        from sqlalchemy import or_
        from ..identity import PartialPartitionName

        if isinstance(id_, PartitionIdentity):
            id_ = id_.id_
        elif isinstance(id_, PartialPartitionName):
            id_ = id_.promote(self.bundle.identity.name)

        session = self.bundle.dataset._database.session
        q = session\
            .query(OrmPartition)\
            .filter(OrmPartition.d_vid == self.bundle.dataset.vid)\
            .filter(or_(OrmPartition.id == str(id_).encode('ascii'),
                        OrmPartition.vid == str(id_).encode('ascii')))

        try:
            orm_partition = q.one()
            return self.bundle.wrap_partition(orm_partition)
        except NoResultFound:
            orm_partition = None

        if not orm_partition:
            q = session\
                .query(OrmPartition)\
                .filter(OrmPartition.d_vid == self.bundle.dataset.vid)\
                .filter(OrmPartition.name == str(id_).encode('ascii'))

            try:
                orm_partition = q.one()
                return self.bundle.wrap_partition(orm_partition)
            except NoResultFound:
                orm_partition = None

        return orm_partition  # Always None

    def _find_orm(self, pnq):
        """Return a Partition object from the database based on a PartitionId.

        An ORM object is returned, so changes can be persisted.

        """
        # import sqlalchemy.orm.exc
        from ambry.orm import Partition as OrmPartition  # , Table
        from sqlalchemy.orm import joinedload  # , joinedload_all

        assert isinstance(
            pnq, PartitionNameQuery), "Expected PartitionNameQuery, got {}".format(
            type(pnq))

        pnq = pnq.with_none()

        q = self.bundle.dataset._database.session.query(OrmPartition)

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
                        raise ValueError("Didn't find table named {} in {} bundle path = {}".format(
                            pnq.table, pnq.vname, self.bundle.database.path))

                    q = q.filter(OrmPartition.t_vid == tr.vid)

        ds = self.bundle.dataset

        q = q.filter(OrmPartition.d_vid == ds.vid)

        q = q.order_by(
            OrmPartition.vid.asc()).order_by(
            OrmPartition.segment.asc())

        q = q.options(joinedload(OrmPartition.table))

        return q

    def clean(self, session):
        from ambry.orm import Partition as OrmPartition

        session.query(OrmPartition).delete()

        return self

    def new_partition(self, name=None, data=None, **kwargs):

        from ambry.identity import PartialPartitionName

        if name:
            name_parts = [e[0] for e in PartialPartitionName._name_parts]
            kwargs.update((k, str(v)) for k, v in iteritems(name.dict)
                          if k in name_parts)

        p = self.bundle.dataset.new_partition(data=data, **kwargs)

        return self.bundle.wrap_partition(p)

    def get_or_new_partition(self, pname, data=None, **kwargs):

        p = self.bundle.partitions.partition(pname)
        if not p:
            p = self.bundle.partitions.new_partition(pname, data=data, **kwargs)
            self.bundle.commit()

        assert p.d_vid == self.bundle.dataset.vid

        return p

    def __iter__(self):
        """Iterate over the type 'p' partitions, ignoring the 's' type. """
        from ambry.orm.partition import Partition
        for p in self.bundle.dataset.partitions:
            if p.type == Partition.TYPE.UNION:
                yield self.bundle.wrap_partition(p)

    def new_db_from_pandas(self, frame, table=None, data=None, load=True, **kwargs):
        """Create a new db partition from a pandas data frame.

        If the table does not exist, it will be created

        """

        from ..orm import Column
        # from dbexceptions import ConfigurationError

        # Create the table from the information in the data frame.
        with self.bundle.session:
            sch = self.bundle.schema
            t = sch.new_table(table)

            if frame.index.name:
                id_name = frame.index.name
            else:
                id_name = 'id'

            sch.add_column(t, id_name,
                           datatype=Column.convert_numpy_type(frame.index.dtype),
                           is_primary_key=True)

            for name, type_ in zip([row for row in frame.columns],
                                   [row for row in frame.convert_objects(convert_numeric=True,
                                                                         convert_dates=True).dtypes]):
                sch.add_column(t, name, datatype=Column.convert_numpy_type(type_))
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


