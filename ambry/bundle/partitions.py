"""Access classess and identity for partitions.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

# import os

from sqlalchemy.orm.exc import NoResultFound

from ..identity import PartitionIdentity, PartitionNameQuery, NameQuery  # , PartitionName


# from util.typecheck import accepts, returns
from ..util import Constant, Proxy


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
        from ..identity import PartitionIdentity
        from sqlalchemy import or_

        session = self.bundle.database.session

        orm_partition = None

        if isinstance(arg, OrmPartition):
            orm_partition = arg

        elif isinstance(arg, basestring):

            orm_query = session.query(OrmPartition).filter(
                or_(OrmPartition.id_ == arg, OrmPartition.vid == arg))

        elif isinstance(arg, PartitionNumber):
            orm_query = session.query(OrmPartition).filter(OrmPartition.id_ == str(arg))

        elif isinstance(arg, PartitionIdentity):
            orm_query = session.query(OrmPartition).filter(OrmPartition.id_ == str(arg.id_))

        else:
            raise ValueError("Arg must be a Partition or PartitionNumber. Got {}".format(type(arg)))

        if not orm_partition:
            orm_partition = orm_query.one()

        vid = orm_partition.vid

        if vid in self._partitions:
            return self._partitions[vid]
        else:
            p = self.new_partition(self.bundle, orm_partition, **kwargs)
            self._partitions[vid] = p
            return p


    def partition(self, id_):
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
        from ..orm import Partition as OrmPartition
        from sqlalchemy import or_
        from ..identity import PartialPartitionName

        if isinstance(id_, PartitionIdentity):
            id_ = id_.id_
        elif isinstance(id_, PartialPartitionName):
            id_ = id_.promote(self.bundle.identity.name)

        s = self.bundle.dataset._database.session

        q = (s.query(OrmPartition).filter(or_(
                 OrmPartition.id == str(id_).encode('ascii'),
                 OrmPartition.vid == str(id_).encode('ascii')
             )))

        try:
            orm_partition = q.one()

            return  PartitionProxy(self.bundle,orm_partition)
        except NoResultFound:
            orm_partition = None

        if not orm_partition:
            q = (s.query(OrmPartition).filter(OrmPartition.name == str(id_).encode('ascii')))

            try:
                orm_partition = q.one()

                return  PartitionProxy(self.bundle,orm_partition)
            except NoResultFound:
                orm_partition = None

        return orm_partition # Always None


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

    def new_partition(self, name = None, data=None, **kwargs):

        from ambry.identity import PartialPartitionName

        if name:
            kwargs = { k:str(v) for k,v in name.dict.items() if k in [ e[0] for e in PartialPartitionName._name_parts] }

        p = self.bundle.dataset.new_partition(data=data,**kwargs)
        self.bundle.dataset.commit()

        return PartitionProxy(self.bundle, p)

    def __iter__(self):
        for p in self.bundle.dataset.partitions:
            yield PartitionProxy(self.bundle, p)

    def new_db_from_pandas(self,frame,table=None,data=None,load=True, **kwargs):
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

            sch.add_column(t,id_name,
                datatype=Column.convert_numpy_type(frame.index.dtype),is_primary_key=True)

            for name, type_ in zip([row for row in frame.columns],
                                   [row for row in frame.convert_objects(convert_numeric=True,
                                                                         convert_dates=True).dtypes]):
                sch.add_column(t,name,datatype=Column.convert_numpy_type(type_))
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


class PartitionProxy(Proxy):

    def __init__(self, bundle, obj):
        super(PartitionProxy, self).__init__(obj)
        self._partition = obj
        self._bundle = bundle

    def clean(self):
        """Remove all built files and return the partition to a newly-created state"""

    def database(self):
        """Returns self, to deal with old bundles that has a direct reference to their database. """
        return self

    def datafile(self):
        from ambry.etl.partition import new_partition_data_file
        return new_partition_data_file(self._bundle.build_fs, self.cache_key)


    def finalize(self, pipeline, stats):


        self._partition.state = self._partition.STATES.BUILT

        try:

            # Write the stats for this partition back into the partition

            self.set_stats(stats.stats())
            self.set_coverage(stats.stats())
            self.table.update_from_stats(stats.stats())
            self._bundle.dataset.commit()

        except KeyError as e:
            raise
            pass  # No stats in the pipeline.

        self._partition.state = self._partition.STATES.FINALIZED
