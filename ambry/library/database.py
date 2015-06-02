"""A Library is a local collection of bundles.

It holds a database for the configuration of the bundles that have been
installed into it.

"""

# Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt


import os.path

import ambry
import ambry.util
from ambry.util import temp_file_name
from ..identity import  Identity
from ambry.orm import Column, Partition, Table, Dataset, Config, File,  Code, ColumnStat, ConflictError
from ambry.orm.exc import NotFoundError, ConflictError

from collections import namedtuple
from sqlalchemy.exc import IntegrityError





class LibraryDb(object):

    """Represents the Sqlite database that holds metadata for all installed
    bundles."""



    def __init__(self, driver=None, server=None, dbname=None,
                 username=None, password=None, port=None, **kwargs):
        self.driver = driver
        self.server = server
        self.dbname = dbname
        self.username = username
        self.password = password







        self._partition_collection = []

        if self.driver in ['postgres', 'postgis']:
            self._schema = 'library'
        else:
            self._schema = None

        self.logger = ambry.util.get_logger(__name__)
        import logging
        self.logger.setLevel(logging.INFO)

        self.enable_delete = False

    ##
    # Sqlalchemy connection, engine, session, metadata
    ##


    def drop(self):
        """ Drop all of the tables in the database

        :return:
        :raise Exception:
        """
        from sqlalchemy.exc import NoSuchTableError, ProgrammingError, OperationalError

        if not self.enable_delete:
            raise Exception("Deleting not enabled. Set library.database.enable_delete = True")

        try:
            self.metadata.sorted_tables
        except NoSuchTableError:
            # Deleted the tables out from under it, so we're done.
            return


        # Tables and partitions can have a cyclic relationship.
        # Prob should be handled with a cascade on relationship.
        try:
            self.session.query(Table).update({Table.p_vid: None})
            self.session.commit()
        except (ProgrammingError, OperationalError): # Table doesn't exist.
            self._session.rollback()

            pass

        self.metadata.drop_all(self.engine)

    def __del__(self):
        pass



    def inserter(self, table_name, **kwargs):
        from ..database.inserter import ValueInserter
        from sqlalchemy.schema import Table

        table = Table(table_name,self.metadata,autoload=True,autoload_with=self.engine)

        return ValueInserter(self, None, table, **kwargs)



    ##
    # Install and remove bundles and partitions
    ##

    def install_dataset_identity(self, identity, data={}, overwrite=True):
        """Create the record for the dataset.

        Does not add an File objects

        """
        from sqlalchemy.exc import IntegrityError
        from ..dbexceptions import ConflictError

        ds = Dataset(**identity.dict)
        ds.name = identity.sname
        ds.vname = identity.vname
        ds.fqname = identity.fqname
        ds.cache_key = identity.cache_key
        ds.creator = 'N/A'
        ds.data = data

        try:
            self.session.merge(ds)
            self.commit()
        except IntegrityError as e:
            self.session.rollback()

            if not overwrite:
                return

            try:
                self.session.merge(ds)
                self.commit()

            except IntegrityError as e:
                raise ConflictError("Can't install dataset vid={}; \nOne already exists. ('{}');\n {}" .format(
                        identity.vid,e.message,ds.dict))

    def install_bundle_dataset(self, bundle):
        """Install only the dataset record for the bundle"""

        from sqlalchemy.orm import joinedload, noload

        if self.session.query(Dataset).filter(Dataset.vid == str(bundle.identity.vid)).first():
            return False

        dataset = (bundle.database.session.query(Dataset).options(noload('*'), joinedload('configs'))
                   .filter(Dataset.vid == str(bundle.identity.vid)).one() )

        self.session.merge(dataset)

        for cfg in dataset.configs:
            self.session.merge(cfg)

        self.session.commit()

        return dataset


    def install_bundle(self, bundle):
        """Copy the schema and partitions lists into the library database."""

        from sqlalchemy.orm import joinedload, noload

        if self.session.query(Dataset).filter(Dataset.vid == str(bundle.identity.vid) ).first():
            return False

        dataset = self.install_bundle_dataset(bundle)

        d_vid = dataset.vid

        # This is a lot faster than going through the ORM.
        for tbl in [Table, Column, Code, Partition, ColumnStat]:

            rows = [dict(r.items()) for r in bundle.database.session.execute(tbl.__table__.select()) ]

            # There were recent schema updates that add a d_vid to every object, but these will be null
            # in old bundles, so we need to set the value manually.
            if tbl == Column or tbl == ColumnStat or tbl == Code:
                for r in rows:
                    for k,v in r.items():
                        if k.endswith('_d_vid') and not bool(v):
                            r[k] = d_vid
            if rows:
                self.session.execute(tbl.__table__.insert(), rows)

            self.session.commit()

        self._mark_update()

        return dataset

    def mark_table_installed(self, table_or_vid, name=None):
        """Mark a table record as installed."""

        s = self.session
        table = None

        table = s.query(Table).filter(Table.vid == table_or_vid).one()

        if not table:
            table = s.query(Table).filter(Table.name == table.vid).one()

        if not name:
            name = table.name

        table.installed = 'y'

        s.merge(table)
        s.commit()

    def mark_partition_installed(self, p_vid):
        """Mark a table record as installed."""

        s = self.session
        table = None

        p = s.query(Partition).filter(Partition.vid == p_vid).one()

        p.installed = 'y'

        s.merge(p)
        s.commit()

    def remove_bundle(self, bundle):
        """remove a bundle from the database."""
        from ..orm import Dataset
        from ..bundle import LibraryDbBundle

        try:
            dataset, partition = self.get_id( bundle.identity.vid)
        except AttributeError:
            dataset, partition = bundle, None

        if not dataset:
            return False

        dataset =  self.session.query(Dataset).filter( Dataset.vid == dataset.identity.vid).one()

        self.session.delete(dataset)

        self.commit()


    def delete_dataset_colstats(self, dvid):
        """Total hack to deal with not being able to get delete cascades to
        work for colstats.

        :param vid: dataset vid
        :return:

        """
        s = self.session

        # Get the partitions for the dataset
        part_query = s.query(Partition.vid).filter(Partition.d_vid == dvid)

        # Delete those colstats that reference the partitions.
        s.query(ColumnStat).filter(ColumnStat.p_vid.in_(part_query.subquery())).delete(synchronize_session='fetch')


    def remove_dataset(self, vid):
        """Remove all references to a Dataset."""
        from ..orm import Dataset, ColumnStat

        dataset = (self.session.query(Dataset).filter(Dataset.vid == vid).one())

        # Total hack to avoid having to figure out cascades between partitions
        # and colstats
        self.delete_dataset_colstats(dataset.vid)

        # Can't use delete() on the query -- bulk delete queries do not
        # trigger in-python cascades!
        self.session.delete(dataset)


        self.session.commit()

    def remove_partition(self, partition):
        from ..bundle import LibraryDbBundle
        from ..orm import Partition

        try:
            dataset = self.get(partition.identity.vid)  # @UnusedVariable
            p_vid = partition.identity.vid
        except AttributeError:
            # It is actually an identity, we hope
            dataset = partition.as_dataset()
            p_vid = partition.vid

        b = LibraryDbBundle(self, dataset.vid)

        s = self.session

        # TODO: Probably need to manually delete colstats.

        s.query(Partition).filter(Partition.t_vid == p_vid).delete()

        self.commit()

    def remove_partition_record(self, vid):
        from ..orm import ColumnStat

        s = self.session

        # FIXME: The Columstat delete should be cascaded, but I really don't
        # understand cascading.
        s.query(ColumnStat).filter(ColumnStat.p_vid == vid).delete()
        s.query(Partition).filter(Partition.vid == vid).delete()

        s.commit()

    ##
    # Get objects by reference, or resolve a reference
    ##

    def get(self, vid):
        """Get an identity by a vid.

        For partitions, returns a nested Identity

        """
        from ..identity import ObjectNumber, DatasetNumber, PartitionNumber
        from ..orm import Dataset, Partition
        from sqlalchemy.orm.exc import NoResultFound
        from ..dbexceptions import NotFoundError

        try:
            if isinstance(vid, basestring):
                vid = ObjectNumber.parse(vid)

            if isinstance(vid, DatasetNumber):
                d = (self.session.query(Dataset)
                     .filter(Dataset.vid == str(vid)).one())
                did = d.identity

            elif isinstance(vid, PartitionNumber):
                d, p = (self.session.query(Dataset, Partition).join(Partition)
                        .filter(Partition.vid == str(vid)).one())
                did = d.identity
                did.add_partition(p.identity)

            else:
                raise ValueError('vid was wrong type: {}'.format(type(vid)))

            return did
        except NoResultFound:
            raise NotFoundError("No object found for vid {}".format(vid))

    def get_table(self, table_vid):

        s = self.session

        return s.query(Table).filter(Table.vid == table_vid).one()

    def tables(self):

        s = self.session

        out = []

        for t in s.query(Table).all():
            out[t.name] = t.dict

        return out

    def list(self, datasets=None, with_partitions=False, key='vid'):
        """
        :param datasets: If specified, must be a dict, which the internal dataset data will be
        put into.
        :return: vnames of the datasets in the library.
        """

        from ..orm import Dataset, Partition, File
        from .files import Files
        from sqlalchemy.sql import or_

        if datasets is None:
            datasets = {}

        q1 = (self.session.query(Dataset, Partition, File)
              .join(Partition)
              .outerjoin(File, File.ref == Partition.vid)
              .filter(Dataset.vid != ROOT_CONFIG_NAME_V))

        q2 = (self.session.query(Dataset, File)
              .outerjoin(File, File.ref == Dataset.vid)
              .filter(Dataset.vid != ROOT_CONFIG_NAME_V))

        entries = [(d, None, f) for d, f in q2.all()]

        if with_partitions:
            entries += q1.all()

        for d, p, f in entries:

            ck = getattr(d.identity, key)

            if ck not in datasets:
                datasets[ck] = d.identity
                datasets[ck].summary = self.get_bundle_value(
                    d.vid,'config','about.title')

            # Adding the file to the identity gets us the bundle state and
            # modification time.
            if f:
                if not p:

                    datasets[ck].add_file(f)
                    datasets[ck].bundle_state = f.state if (
                        f.state and not datasets[ck].bundle_state) else datasets[ck].bundle_state

                else:
                    p.identity.add_file(f)

            if p and (not datasets[ck].partitions or p.vid not in datasets[ck].partitions):
                datasets[ck].add_partition(p.identity)

        return datasets

    def all_vids(self):

        all = set()

        q = (self.session.query(Dataset,Partition).join(Partition).filter(Dataset.vid != ROOT_CONFIG_NAME_V))

        for row in q.all():
            all.add(row.Dataset.vid)
            all.add(row.Partition.vid)

        return all

    def datasets(self, key='vid'):
        """List only the dataset records."""

        from ..orm import Dataset

        datasets = {}

        for d in (self.session.query(Dataset)
                  .filter(Dataset.location == Dataset.LOCATION.LIBRARY)
                  .filter(Dataset.vid != ROOT_CONFIG_NAME_V).all()):

            ck = getattr(d.identity, key)
            datasets[ck] = d.identity

        return datasets

    @property
    def resolver(self):
        from .query import Resolver
        return Resolver(self.session)



