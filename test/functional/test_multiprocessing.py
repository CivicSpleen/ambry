# -*- coding: utf-8 -*-
import unittest

from ambry.orm import Database

from test.proto import TestBase

# TODO: This is completely broken because base class changed.


@unittest.skip
class Test(TestBase):

    def setUp(self):
        from ambry.run import get_runconfig
        from ambry.library import new_library

        rc = get_runconfig()

        self.library = new_library(rc)

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

    #@unittest.skipIf("not os.getenv('VIRTUAL_ENV')", 'Must have a VIRTUAL_ENV value set to get the config')
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

        # print(db.dsn)

        ds = self.new_db_dataset(db, self.d_vid)

        ds.new_table('footable')

        # FIXME! The cast_to_subclass is required before the MP run to create the bundle source File record,
        # otherwise, it is create in the MP run, and you get an Integrity violation.
        l.bundle(ds.vid).cast_to_subclass().commit()

        ds.commit()

        self.assertIsNotNone(db.dataset(self.d_vid))
        self.assertIsNotNone(self.library.bundle(self.d_vid))

        N = 40

        pool = self.library.process_pool()
        args = [ (ds.vid, i) for i in range(1,N+1)]

        names = pool.map(run_mp, args)

        self.assertEqual(N, len(ds.partitions))

        records = list(sorted(o.name for o in ds.partitions))

        self.assertEquals(sorted(names), records)

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

        pool = self.library.process_pool()

        b.new_table('pre1')
        b.new_table('pre2')
        b.commit()

        self.assertEqual(2, len(ds.tables))

        b.dataset.new_source_table('pre1')
        b.dataset.new_source_table('pre2')
        b.commit()

        self.assertEqual(2, len(ds.source_tables))

        b.dataset.new_partition(table=b.table('pre1'))
        b.dataset.new_partition(table=b.table('pre2'))
        b.commit()

        self.assertEqual(2, len(ds.source_tables))

        for mp_f, ds_attr in ((run_mp_tables, 'tables'),
                              (run_mp_sourcetables, 'source_tables'),
                              (run_mp_partitions, 'partitions')
                              ):

            print('Running: ', ds_attr)

            names = pool.map(mp_f, [(ds.vid, i) for i in range(1, N+1)])

            b.session.refresh(ds)
            self.assertEqual(N+2, len(getattr(ds, ds_attr)))

            records = list(sorted([o.name for o in getattr(ds, ds_attr)]))

            if ds_attr == 'partitions':
                extra_names = ['example.com-dataset-pre1','example.com-dataset-pre2']
            else:
                extra_names = ['pre1','pre2']

            self.assertEquals(sorted(names+extra_names), records)


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

        pool = self.library.process_pool()

        names = pool.map(run_mp_combined, [(ds.vid, i) for i in range(1, N + 1)])

        from itertools import chain

        names = sorted(chain(*names))

        self.assertEquals(135, len(names))



def run_mp(b,i):
    """Ingest a source, using only arguments that can be pickled, for multiprocessing access"""
    from ambry.identity import PartialPartitionName

    from ambry.identity import PartialPartitionName
    from ambry.orm.partition import Partition


    table = b.table('footable')

    pname = PartialPartitionName(table='footable', segment=i)

    p = b.partitions.new_partition(pname, type=Partition.TYPE.SEGMENT)

    b.commit()

    return p.name

def run_mp_partitions(b,i):
    """Ingest a source, using only arguments that can be pickled, for multiprocessing access"""
    from ambry.identity import PartialPartitionName
    from ambry.orm.partition import Partition

    b, i = args

    table = b.table('obj01') # The run_mp_tables test must come before partitions to make the obj01 table

    p = b.dataset.new_partition(table, segment=i, type=Partition.TYPE.SEGMENT)

    b.commit()

    return p.name


def run_mp_tables(b,i):
    """Ingest a source, using only arguments that can be pickled, for multiprocessing access"""
    from ambry.orm import Table

    b.commit()
    t = b.dataset.new_table(name="obj{:02d}".format(i))
    b.commit()

    if t:
        return t.name
    else:
        return None


def run_mp_sourcetables(b,i):
    """Ingest a source, using only arguments that can be pickled, for multiprocessing access"""


    b.commit()
    t = b.dataset.new_source_table(name="obj{:02d}".format(i))
    b.commit()

    if t:
        return t.name
    else:
        return None

def run_mp_combined(b,i):
    """Ingest a source, using only arguments that can be pickled, for multiprocessing access"""

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




