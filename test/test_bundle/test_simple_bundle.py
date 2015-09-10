# -*- coding: utf-8 -*-

import unittest

from boto.exception import S3ResponseError

from test.test_base import TestBase


class Test(TestBase):

    def test_filesystems(self):

        b = self.setup_bundle('simple')
        dir_list = list(b.source_fs.listdir())
        self.assertIn('bundle.py', dir_list)
        self.assertIn('sources.csv', dir_list)
        self.assertIn('bundle.yaml', dir_list)
        self.assertIn('documentation.md', dir_list)

    def test_phases(self):
        """Test copying a bundle to a remote, then streaming it back"""
        import os
        from ambry.etl import IterSource, MatchPredicate, Reduce
        from itertools import izip, cycle

        b = self.setup_bundle('simple')
        l = b._library
        b.sync_in()
        b.prepare()

        # Text a pipeline run outside of the bundle.
        pl = b.pipeline('test')
        source = lambda: IterSource(izip(range(100), range(100,200), cycle('abcdefg'), cycle([1.1,2.1,3.2,4.3])))
        pl.source = source()
        pl.last = [MatchPredicate(lambda r: r[2] == 'g'),
                   Reduce(lambda a,r: (a[0]+r[0],a[1]+r[1]) if a else (r[0],r[1])  )]
        pl.run()
        self.assertEquals((4950, 14950), pl[Reduce].accumulator)
        self.assertEquals(14,len(pl[MatchPredicate].matches))

        # Same pipeline, but with the match and reduce in the configuration.
        pl = b.pipeline('test2')
        pl.source = source()
        pl.run()
        print pl
        self.assertEquals((4950, 14950), pl[Reduce].accumulator)
        self.assertEquals(14, len(pl[MatchPredicate].matches))

        print pl


    def test_simple_process(self):
        """Build the simple bundle"""
        from ambry.orm.file import File
        import time

        # The modification times for mem: files don't seem to change, so we use temp: instead
        b = self.setup_bundle('simple', source_url='temp://')

        b.sync()  # This will sync the files back to the bundle's source dir

        self.assertEquals(7, len(b.dataset.files))
        file_names = [f.path for f in b.dataset.files]

        print file_names

        self.assertEqual([u'bundle.py', u'lib.py', u'documentation.md', u'source_schema.csv',
                          u'sources.csv', u'bundle.yaml', u'schema.csv'], file_names)

        self.assertEqual(12, len(b.dataset.configs))

        self.assertFalse(b.is_prepared)
        self.assertFalse(b.is_built)

        #
        # Test preferences
        #

        def muck_source_file(g, val='grain'):
            """Alter the source_schema file"""
            import csv
            with b.source_fs.open('sources.csv', 'rb') as f:
                rows = list(csv.reader(f))

            rows[1][rows[0].index(val)] = g

            with b.source_fs.open('sources.csv', 'wb') as f:
                csv.writer(f).writerows(rows)

        def source_file(val='grain'):
            import csv

            with b.source_fs.open('sources.csv', 'rb') as f:
                rows = list(csv.reader(f))

            self.assertEquals(2, len(rows))

            return rows[1][rows[0].index(val)]

        def file_record(val='grain'):
            f = b.build_source_files.file(File.BSFILE.SOURCES).record

            rows = f.unpacked_contents

            self.assertEquals(2, len(rows))

            return rows[1][rows[0].index(val)]

        def muck_source_object(g, val='grain'):
            """Alter the source_schema file"""

            setattr(list(b.sources)[0], val, g)

        def source_object(val='grain'):

            try:
                s = list(b.sources)[0]
                return getattr(s, val)
            except IndexError: # Sources list is empty
                return None

        def set_preference(p):
            b.build_source_files.file(File.BSFILE.SOURCES).record.preference = p

        def sync_source_to_record():
            b.build_source_files.file(File.BSFILE.SOURCES).fs_to_record()

        #
        # Basic process with FILE preference

        set_preference(File.PREFERENCE.FILE)
        v1 = 'value1'
        v2 = 'value2'
        v3 = 'value3'
        v4 = 'value4'

        time.sleep(2)
        muck_source_file(v1)

        self.assertEquals(v1, source_file())
        self.assertNotEquals(v1, file_record())
        self.assertNotEquals(v1, source_object())

        self.assertIn(('sources', 'ftr'), b.sync())

        self.assertEquals(v1,source_file())
        self.assertEquals(v1,file_record())
        self.assertNotEquals(v1,source_object())

        b.sync_in()

        self.assertEquals(v1,source_file())
        self.assertEquals(v1,file_record())
        self.assertEquals(v1,source_object())

        muck_source_object(v2)
        self.assertEquals(v1,source_file())
        self.assertEquals(v1,file_record())
        self.assertEquals(v2,source_object())

        # Should overwrite the object with the file.
        b.sync_in()
        self.assertEquals(v1,source_file())
        self.assertEquals(v1,file_record())
        self.assertEquals(v1,source_object())

        # Run meta, alter the source file, then run meta again
        # The file should retain the change.
        muck_source_file(v4)
        sync_source_to_record()
        muck_source_object(v4)

        b.sync()
        time.sleep(1) # Allow modification time to change
        muck_source_file(v1)
        b.sync()
        self.assertEquals(v1, source_file())
        self.assertEquals(v1, file_record())

        ##################
        # Alter the preference to the OBJECT, should
        # cause the object to overwrite the record
        set_preference(File.PREFERENCE.OBJECT)
        muck_source_file(v1)
        sync_source_to_record()
        muck_source_object(v1)

        # Check that a reset works
        self.assertEquals(v1, source_file())
        self.assertEquals(v1, file_record())
        self.assertEquals(v1, source_object())

        muck_source_object(v2)
        self.assertEquals(v1,source_file())
        self.assertEquals(v1,file_record())
        self.assertEquals(v2,source_object())

        # Prepare should move the object to the file record, not the
        # file record to the object

        b.sync_objects()

        self.assertEquals(v1,source_file())
        self.assertEquals(v2,file_record())
        self.assertEquals(v2,source_object())

        ##################
        # Alter the preference
        set_preference(File.PREFERENCE.OBJECT)
        muck_source_file(v1)
        muck_source_file(v2,val='space')
        sync_source_to_record()
        muck_source_object(v1)
        muck_source_object(v2, val='space')

        # Check that a reset works
        self.assertEquals(v1, source_file())
        self.assertEquals(v1, file_record())
        self.assertEquals(v1, source_object())
        self.assertEquals(v2, source_file(val='space'))
        self.assertEquals(v2, file_record(val='space'))
        self.assertEquals(v2, source_object(val='space'))

        # Actually test the merging

        set_preference(File.PREFERENCE.MERGE)
        muck_source_file(v3)
        sync_source_to_record()
        muck_source_object(v1, val='space')

        self.assertEquals(v3, source_file())
        self.assertEquals(v3, file_record())
        self.assertEquals(v1, source_object())

        self.assertEquals(v2, source_file(val='space'))
        self.assertEquals(v2, file_record(val='space'))
        self.assertEquals(v1, source_object(val='space'))

        # Ths last change happens during the prepare.
        def prepare_main(self):
            muck_source_object(v4, val='space')
            return True

        b.__class__.prepare_main = prepare_main

        # This should push the record to the objects, carrying v3 into the object in pos=6. Then, the
        # end of the prepare phase should carry v4 in val='space' back into the record. So, both the record
        # and the object should have v3 in pos=6 and and v4 in val='space'

        b.sync_objects()
        b.sync()
        self.assertEquals(v3, source_file())
        self.assertEquals(v3, file_record())
        self.assertEquals(v3, source_object()) # pre_prepare carried v3 to object

        self.assertEquals(v2, source_file(val='space')) # source file won't change until sync
        #self.assertEquals(v4, file_record(val='space')) # TODO This one still fails ...
        #self.assertEquals(v4, schema_object(val='space'))

    def test_schema_update(self):
        """Check that changes to the source schema persist across re-running meta"""
        from ambry.orm.file import File
        import time

        # The modification times for mem: files don't seem to change, so we use temp: instead
        b = self.setup_bundle('simple', source_url='temp://')

        b.sync_in()  # This will sync the files back to the bundle's source dir

        def muck_schema_file(source_header, dest_header ):
            """Alter the source_schema file"""
            import csv

            with b.source_fs.open('source_schema.csv', 'rb') as f:
                rows = list(csv.reader(f))

            for row in rows:
                if row[2] == source_header:
                    row[3] = dest_header

            with b.source_fs.open('source_schema.csv', 'wb') as f:
                csv.writer(f).writerows(rows)

        def check_schema_file(source_header):
            import csv

            with b.source_fs.open('source_schema.csv', 'rb') as f:
                rows = list(csv.reader(f))

            for row in rows:
                if row[2] == source_header:
                    return row[3]

        def check_schema_record(source_header):
            import csv

            rows = b.build_source_files.file(File.BSFILE.SOURCESCHEMA).record.unpacked_contents

            for row in rows:
                if row[2] == source_header:
                    return row[3]

        def check_schema_object(source_header):

            for col in b.dataset.source_columns:
                if col.source_header == source_header:
                    return col.dest_header

        b.meta()
        b.sync_out()
        time.sleep(1)
        muck_schema_file('uuid','value1')

        self.assertEquals('value1',check_schema_file('uuid'))

        b.sync()

        self.assertEquals('value1',check_schema_record('uuid'))

        b.meta()

        self.assertEquals('value1', check_schema_file('uuid'))

    def test_edit_pipeline(self):
        """Build the simple bundle"""

        b = self.setup_bundle('simple')

        b.sync_in()

        b = b.cast_to_subclass()

        self.assertTrue(b.meta())
        self.assertEquals('schema_done', b.state)

        def edit_pipeline(pl):
            from ambry.etl.pipeline import PrintRows, LogRate

            def prt(m):
                print m

            pl.build_last = [PrintRows(print_at='end'), LogRate(prt, 3000, '')]

        b.set_edit_pipeline(edit_pipeline)

        self.assertTrue(b.build())

        # Two dataset partitions, one segment, one union
        self.assertEquals(1,len(b.dataset.partitions))

        # But, only one partition from the bundle, which is the one union partition
        self.assertEquals(1, len(list(b.partitions)))

        self.assertEquals(4,len(b.dataset.source_columns))

        self.assertEquals('build_done', b.state)

        # Already built can't build again
        self.assertFalse(b.build())

        b.clean()
        self.assertTrue(b.build())

        self.assertTrue(b.finalize())
        self.assertTrue(b.is_finalized)
        self.assertFalse(b.clean())

        return b



    def test_db_copy(self):
        from ambry.orm.database import Database

        b = self.setup_bundle('simple')
        l = b._library

        b.run()

        import tempfile

        try:
            td = tempfile.mkdtemp()

            db = Database('sqlite:////{}/{}.db'.format(td, b.identity.vid))
            db.open()

            db.copy_dataset(b.dataset)

            ds1 = b.dataset
            ds2 = db.dataset(ds1.vid)

            print list(ds1.tables)

            self.assertEquals(len(ds1.tables), len(ds2.tables))
            self.assertEquals(len(ds1.tables[0].columns), len(ds2.tables[0].columns))
            self.assertEquals(len(ds1.partitions), len(ds2.partitions))
            self.assertEquals(len(ds1.files), len(ds2.files))
            self.assertEquals(len(ds1.configs), len(ds2.configs))
            self.assertEquals(len(ds1.stats), len(ds2.stats))

        finally:
            from shutil import rmtree
            rmtree(td)

    # FIXME This test passes when run individually, but fails when run with the other
    # tests in the class, at least when run in PyCharm.
    def test_install(self):
        """Test copying a bundle to a remote, then streaming it back"""

        b = self.setup_bundle('simple', source_url='temp://')
        l = b._library

        b.sync_in()

        b.run()

        self.assertEqual(1, len(list(l.bundles)))

        p = list(b.partitions)[0]
        p_vid = p.vid

        self.assertEquals(497054, int(sum(row[3] for row in p.stream())))

        self.assertEqual('build', l.partition(p_vid).location)

        try:
            remote_name, path = b.checkin()
        except S3ResponseError as exc:
            if exc.status == 403:  # Forbidden.
                raise unittest.SkipTest(
                    'Skip S3 error - {}. It seems S3 credentials are not valid.'.format(exc))
            else:
                raise

        self.assertEqual('build', l.partition(p_vid).location)

        for b in l.bundles:
            l.remove(b)

        self.assertEqual(0, len(list(l.bundles)))

        l.sync_remote(remote_name)

        b = l.bundle(b.identity.vid)
        p = list(b.partitions)[0]

        #self.assertEqual(1, len(list(l.bundles)))

        self.assertEqual('remote',l.partition(p_vid).location)

        self.assertEqual(10000, len(list(p.stream())))

        self.assertEquals(497054, int(sum(row[3] for row in p.stream())))

    def test_msgpack_build(self):
        """Build the simple bundle"""

        from geoid import civick, census

        b = self.setup_bundle('complete-build')
        b.run()

        for p in b.partitions:
            print '---', p.identity.name

            for c in p.children:
                print '   ', c.identity.name
                id_sum = 0
                for row in b.wrap_partition(c).stream(skip_header=True):

                    id_sum += row[0]

                self.assertEqual(18003000, id_sum)


