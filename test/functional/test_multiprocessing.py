# -*- coding: utf-8 -*-
import unittest

from test.test_base import TestBase
from ambry.identity import DatasetNumber
from ambry.orm import Database, Dataset

class Test(TestBase):

    def setUp(self):
        from fs.opener import fsopendir
        from ambry.run import get_runconfig
        from ambry.library import new_library
        from ambry.dbexceptions import ConfigurationError

        # NOTE! When working in an IDE, get_runconfig()  get your .ambry.yaml unless you either have it in
        # $HOME/.ambry.yaml, or have the VIRTUAL_ENV variable set to your virtenv
        try:
            rc = get_runconfig()
        except ConfigurationError:
            raise ConfigurationError("Failed to load config. You probably need to setup a "
                                     "VIRTUAL_ENV env var, or create $HOME/.ambry.yaml")

        self.library = new_library(rc, 'postgresql-test')

        self.dsn = self.library.database.dsn

        self.d_vid = 'd000mptest001'

        # This is needed when calling the MP functions from a non-mp process
        from ambry.bundle.concurrent import alt_init
        alt_init(self.library)

    @classmethod
    def get_rc(self, name='ambry.yaml'):
        import os
        from ambry.run import get_runconfig
        from test import bundlefiles

        def bf_dir(fn):
            return os.path.join(os.path.dirname(bundlefiles.__file__), fn)

        return get_runconfig(bf_dir('ambry.yaml'))

    def new_db_dataset(self, db,  source='source'):

        return db.new_dataset(vid=self.d_vid, source='example.com', dataset='dataset')

    def new_database(self):
        # FIXME: this connection will not be closed properly in a postgres case.
        db = Database(self.dsn)
        db.open()
        return db

    @unittest.skipIf("not os.getenv('VIRTUAL_ENV')", 'Must have a VIRTUAL_ENV value set to get the config')
    def test_multi_partitions(self):
        """Test creating multip partitions in a multiprocessing run"""
        from ambry.orm.exc import NotFoundError

        l = self.library
        db = l.database

        l.processes = 4

        try:
            ds = db.dataset(self.d_vid)
            db.remove_dataset(ds)
            db.commit()
        except NotFoundError:
            pass

        print db.dsn

        ds = self.new_db_dataset(db, self.d_vid)

        ds.new_table('footable')

        # FIXME! The cast_to_subclass is required before the MP run to create the bundle source File record,
        # otherwise, it is create in the MP run, and you get an Integrity violation.
        l.bundle(ds.vid).cast_to_subclass().commit()

        ds.commit()

        self.assertIsNotNone(db.dataset(self.d_vid))
        self.assertIsNotNone(self.library.bundle(self.d_vid))

        N = 40

        pool = self.library.process_pool
        args = [ (ds.vid, i) for i in range(1,N+1)]

        names = pool.map(run_mp, args)

        self.assertEqual(N, len(ds.partitions))

        records = list(sorted(o.name for o in ds.partitions))

        self.assertEquals(sorted(names), records)

    @unittest.skipIf("not os.getenv('VIRTUAL_ENV')", 'Must have a VIRTUAL_ENV value set to get the config')
    def test_unique_object_gen(self):
        """Test creating multip partitions in a multiprocessing run"""
        from ambry.orm.exc import NotFoundError

        l = self.library
        db = l.database

        l.processes = 8

        N = 40

        try:
            ds = db.dataset(self.d_vid)
            db.remove_dataset(ds)
            db.commit()
        except NotFoundError:
            pass

        ds = self.new_db_dataset(db, self.d_vid)

        b = l.bundle(ds.vid)
        # FIXME! The cast_to_subclass is required before the MP run to create the bundle source File record,
        # otherwise, it is create in the MP run, and you get an Integrity violation.
        b = b.cast_to_subclass()
        b.commit()

        self.assertIsNotNone(db.dataset(self.d_vid))
        self.assertIsNotNone(self.library.bundle(self.d_vid))

        pool = self.library.process_pool

        for mp_f, ds_attr in ( (run_mp_tables, 'tables'),
                               (run_mp_sourcetables, 'source_tables'),
                               (run_mp_partitions, 'partitions')
                               ):

            print 'Running: ', ds_attr

            names = pool.map(mp_f, [(ds.vid, i) for i in range(1, N+1)])

            self.assertEqual(N, len(getattr(ds, ds_attr)))

            records = list(sorted(o.name for o in getattr(ds, ds_attr)))

            self.assertEquals(sorted(names), records)

    @unittest.skipIf("not os.getenv('VIRTUAL_ENV')", 'Must have a VIRTUAL_ENV value set to get the config')
    def test_top_levels(self):

        from ambry.orm.exc import NotFoundError

        l = self.library
        db = l.database

        l.processes = 8

        N = 5

        try:
            ds = db.dataset(self.d_vid)
            db.remove_dataset(ds)
            db.commit()
        except NotFoundError:
            pass

        ds = self.new_db_dataset(db, self.d_vid)

        b = l.bundle(ds.vid)
        # FIXME! The cast_to_subclass is required before the MP run to create the bundle source File record,
        # otherwise, it is create in the MP run, and you get an Integrity violation.
        b = b.cast_to_subclass()
        b.commit()

        pool = self.library.process_pool

        names = pool.map(run_mp_combined, [(ds.vid, i) for i in range(1, N + 1)])

        from itertools import chain

        names = sorted(chain(*names))

        self.assertEquals(135, len(names))

from ambry.bundle.concurrent import MPBundleMethod

@MPBundleMethod
def run_mp(args):
    """Ingest a source, using only arguments that can be pickled, for multiprocessing access"""
    from ambry.identity import PartialPartitionName

    from ambry.identity import PartialPartitionName
    from ambry.orm.partition import Partition

    b, i = args

    table = b.table('footable')

    pname = PartialPartitionName(table='footable', segment=i)

    p = b.partitions.new_partition(pname, type=Partition.TYPE.SEGMENT)

    b.commit()

    return p.name

@MPBundleMethod
def run_mp_partitions(args):
    """Ingest a source, using only arguments that can be pickled, for multiprocessing access"""
    from ambry.identity import PartialPartitionName
    from ambry.orm.partition import Partition

    b, i = args

    table = b.table('obj01') # The run_mp_tables test must come before partitions to make the obj01 table

    p = b.dataset.new_unique_object(Partition, t_vid=table.vid, segment=i, type=Partition.TYPE.SEGMENT)

    b.commit()

    return p.name

@MPBundleMethod
def run_mp_tables(args):
    """Ingest a source, using only arguments that can be pickled, for multiprocessing access"""
    from ambry.orm import Table

    b, i = args

    b.commit()
    t = b.dataset.new_unique_object(Table, name="obj{:02d}".format(i))

    b.commit()

    return t.name


@MPBundleMethod
def run_mp_sourcetables(args):
    """Ingest a source, using only arguments that can be pickled, for multiprocessing access"""
    import os
    from ambry.identity import PartialPartitionName
    from ambry.orm import Partition, Table, SourceTable
    from ambry.identity import ObjectNumber

    b, i = args

    t = b.dataset.new_unique_object(SourceTable, name="obj{:02d}".format(i))

    b.commit()

    return t.name

@MPBundleMethod
def run_mp_combined(args):
    """Ingest a source, using only arguments that can be pickled, for multiprocessing access"""

    b, i = args

    ds = b.dataset

    name = "cobj{:02d}".format(i)

    out = []

    for j in range(9):
        name = "cobj{:02d}".format(i*10+j)
        t = ds.new_table(name)
        st = ds.new_source_table(name)
        p = ds.new_partition(t)

        out += [t.name, st.name, p.name]

    b.commit()

    return out




