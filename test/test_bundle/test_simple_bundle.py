# -*- coding: utf-8 -*-

import unittest

from test.test_base import TestBase


class Test(TestBase):

    def test_filesystems(self):

        b = self.setup_bundle('simple')
        dir_list = list(b.source_fs.listdir())
        self.assertIn('bundle.py', dir_list)
        self.assertIn('sources.csv', dir_list)
        self.assertIn('bundle.yaml', dir_list)
        self.assertIn('documentation.md', dir_list)

    def test_simple_process(self):
        """Build the simple bundle"""
        from ambry.orm.file import File
        import time

        # The modification times for mem: files don't seem to change, so we use temp: instead
        b = self.setup_bundle('simple', source_url='temp://')

        b.do_sync()  # This will sync the files back to the bundle's source dir

        self.assertEquals(8, len(b.dataset.files))
        file_names = [f.path for f in b.dataset.files]

        print file_names
        self.assertEqual([u'sources.csv', u'bundle.py', u'source_schema.csv', u'lib.py', u'meta.py',
                          u'documentation.md', u'bundle.yaml', u'schema.csv'], file_names)

        self.assertEqual(12, len(b.dataset.configs))

        self.assertFalse(b.is_prepared)
        self.assertFalse(b.is_built)

        #
        # Test preferences
        #

        def muck_source_file(g, pos = 6):
            """Alter the source_schema file"""
            import csv
            with b.source_fs.open('sources.csv', 'rb') as f:
                rows = list(csv.reader(f))

            rows[1][pos] = g

            with b.source_fs.open('sources.csv', 'wb') as f:
                csv.writer(f).writerows(rows)

        def source_file(pos=6):
            import csv

            with b.source_fs.open('sources.csv', 'rb') as f:
                rows = list(csv.reader(f))

            self.assertEquals(2, len(rows))

            return rows[1][pos]

        def file_record(pos=6):
            f = b.build_source_files.file(File.BSFILE.SOURCES).record

            rows = f.unpacked_contents

            self.assertEquals(2, len(rows))

            return rows[1][pos]

        def muck_source_schema_object(g, pos=6):
            """Alter the source_schema file"""
            if pos == 6:
                b.dataset.sources[0].grain = g
            elif pos == 5:
                b.dataset.sources[0].space = g
            else:
                raise Exception("Unknown pos")

        def schema_object(pos=6):
            try:
                if pos == 6:
                    return b.dataset.sources[0].grain
                elif pos == 5:
                    return b.dataset.sources[0].space
                else:
                    raise Exception("Unknown pos")

            except IndexError:
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
        self.assertNotEquals(v1, schema_object())

        self.assertIn(('sources', 'ftr'), b.do_sync())

        self.assertEquals(v1,source_file())
        self.assertEquals(v1,file_record())
        self.assertNotEquals(v1,schema_object())

        b.do_prepare()

        self.assertEquals(v1,source_file())
        self.assertEquals(v1,file_record())
        self.assertEquals(v1,schema_object())

        muck_source_schema_object(v2)
        self.assertEquals(v1,source_file())
        self.assertEquals(v1,file_record())
        self.assertEquals(v2,schema_object())

        # Should overwrite the object with the file.
        b.do_prepare()
        self.assertEquals(v1,source_file())
        self.assertEquals(v1,file_record())
        self.assertEquals(v1,schema_object())

        # Run meta, alter the source file, then run meta again
        # The file should retain the change.
        muck_source_file(v4)
        sync_source_to_record()
        muck_source_schema_object(v4)

        b.do_sync()
        time.sleep(1) # Allow modification time to change
        muck_source_file(v1)
        b.do_sync()
        self.assertEquals(v1, source_file())
        self.assertEquals(v1, file_record())

        ##################
        # Alter the preference to the OBJECT, should
        # cause the object to overwrite the record
        set_preference(File.PREFERENCE.OBJECT)
        muck_source_file(v1)
        sync_source_to_record()
        muck_source_schema_object(v1)

        # Check that a reset works
        self.assertEquals(v1, source_file())
        self.assertEquals(v1, file_record())
        self.assertEquals(v1, schema_object())

        muck_source_schema_object(v2)
        self.assertEquals(v1,source_file())
        self.assertEquals(v1,file_record())
        self.assertEquals(v2,schema_object())

        # Prepare should move the object to the file record, not the
        # file record to the object
        b.do_prepare()
        self.assertEquals(v1,source_file())
        self.assertEquals(v2,file_record())
        self.assertEquals(v2,schema_object())

        ##################
        # Alter the preference to MERGE
        set_preference(File.PREFERENCE.OBJECT)
        muck_source_file(v1)
        muck_source_file(v2,pos=5)
        sync_source_to_record()
        muck_source_schema_object(v1)
        muck_source_schema_object(v2, pos=5)

        # Check that a reset works
        self.assertEquals(v1, source_file())
        self.assertEquals(v1, file_record())
        self.assertEquals(v1, schema_object())
        self.assertEquals(v2, source_file(pos=5))
        self.assertEquals(v2, file_record(pos=5))
        self.assertEquals(v2, schema_object(pos=5))

        # Actually test the merging

        set_preference(File.PREFERENCE.MERGE)
        muck_source_file(v3)
        sync_source_to_record()
        muck_source_schema_object(v1, pos=5)

        self.assertEquals(v3, source_file())
        self.assertEquals(v3, file_record())
        self.assertEquals(v1, schema_object())

        self.assertEquals(v2, source_file(pos=5))
        self.assertEquals(v2, file_record(pos=5))
        self.assertEquals(v1, schema_object(pos=5))

        # Ths last change happens during the prepare.
        def prepare(self):

            muck_source_schema_object(v4, pos=5)
            return True

        b.__class__.prepare = prepare

        # This should push the record to the objects, carrying v3 into the object in pos=6. Then, the
        # end of the prepare phase should carry v4 in pos=5 back into the record. So, both the record
        # and the object should have v3 in pos=6 and and v4 in pos=5

        self.assertTrue(b.do_prepare())

        self.assertEquals(v3, source_file())
        self.assertEquals(v3, file_record())
        self.assertEquals(v3, schema_object()) # pre_prepare carried v3 to object
        self.assertEquals(v2, source_file(pos=5)) # source file won't change until sync
        self.assertEquals(v4, file_record(pos=5))
        self.assertEquals(v4, schema_object(pos=5))

    def test_schema_update(self):
        """Check that changes to the source schema persist across re-running meta"""
        from ambry.orm.file import File
        import time

        # The modification times for mem: files don't seem to change, so we use temp: instead
        b = self.setup_bundle('simple', source_url='temp://')

        b.do_sync()  # This will sync the files back to the bundle's source dir

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

        b.do_meta()

        time.sleep(1)
        muck_schema_file('uuid','value1')

        self.assertEquals('value1',check_schema_file('uuid'))

        b.do_sync()

        self.assertEquals('value1',check_schema_record('uuid'))

        b.do_meta()

        self.assertEquals('value1', check_schema_file('uuid'))


    def test_simple_build(self):
        """Build the simple bundle"""

        b = self.setup_bundle('simple')

        self.assertTrue(b.sync())

        b = b.cast_to_build_subclass()

        self.assertTrue(b.do_meta())
        self.assertEquals('synced', b.state)
        self.assertTrue(b.do_prepare())
        self.assertEquals('prepared', b.state)

        def edit_pipeline(pl):
            from ambry.etl.pipeline import PrintRows, LogRate

            def prt(m):
                print m

            pl.build_last = [PrintRows(print_at='end'), LogRate(prt, 3000, '')]

        b.set_edit_pipeline(edit_pipeline)

        self.assertTrue(b.do_build())

        # Two dataset partitions, one segment, one union
        self.assertEquals(2,len(b.dataset.partitions))

        # But, only one partition from the bundle, which is the one union partition
        self.assertEquals(1, len(list(b.partitions)))

        self.assertEquals(4,len(b.dataset.source_columns))

        # Already built can't build again
        self.assertFalse(b.do_build())

        self.assertTrue(b.do_clean())
        # Can't build if not prepared
        self.assertFalse(b.do_build())

        self.assertTrue(b.do_prepare)
        self.assertTrue(b.do_build)

        self.assertTrue(b.finalize())
        self.assertTrue(b.is_finalized)
        self.assertFalse(b.do_clean())

        return b

    def test_complete_build(self):
        """Build the simple bundle"""

        from geoid import civick, census

        b = self.setup_bundle('complete-build')
        b.sync()
        b = b.cast_to_build_subclass()
        self.assertEquals('synced', b.state)
        self.assertTrue(b.do_meta())

        self.assertTrue(b.do_prepare())
        self.assertEquals('prepared', b.state)

        def edit_pipeline(pl):
            from ambry.etl.pipeline import PrintRows, LogRate, Edit, WriteToPartition, SelectPartition

            def prt(m):
                print m

            # Converting to the cesus geoid b/c they are just numbers, and when used in a partition name,
            # the names are lowercased, causing the case sensitive GVIDs to alias.
            pl.dest_augment = Edit(
                edit = {'triangle' : lambda e,v : 1}
            )

            pl.build_last =  [PrintRows( print_at='end'), LogRate(prt, 3000,'')]

            def select_part(source, row):
                from ambry.identity import PartialPartitionName
                return PartialPartitionName(table=source.dest_table_name, time = row[8])

            pl.select_partition = [SelectPartition(select_part)]

            # assign a scalar to append, assign a list to replace
            pl.write_to_table = [WriteToPartition] # WriteToSelectedPartition()

        b.set_edit_pipeline(edit_pipeline)

        self.assertTrue(b.do_build())

        for p in b.partitions:

            self.assertIn(int(p.identity.time), p.time_coverage)

        self.assertEquals([u'0O0001', u'0O0002', u'0O0003', u'0O0101', u'0O0102', u'0O0103'],
                          b.dataset.partitions[0].space_coverage)
        self.assertEquals([u'2qZZZZZZZZZZ'], b.dataset.partitions[0].grain_coverage)

        self.assertEquals([u'0O0001', u'0O0002', u'0O0003', u'0O0101', u'0O0102', u'0O0103'],
                          b.dataset.partitions[2].space_coverage)
        self.assertEquals([u'2qZZZZZZZZZZ'], b.dataset.partitions[2].grain_coverage)

        self.assertEqual(4, len(b.dataset.partitions))
        self.assertEqual(2, len(b.dataset.tables))

        c = b.build_fs.getcontents(list(b.build_fs.walkfiles())[0])

        self.assertEquals(6001, len(c.splitlines()))

        self.assertEquals(48, len(b.dataset.stats))

        self.assertEquals('built', b.state)

    def test_complete_load(self):
        """Build the simple bundle"""

        b = self.setup_bundle('complete-load')
        b.sync()
        b = b.cast_to_meta_subclass()
        b.do_meta()
        self.assertEquals('synced', b.state)
        self.assertTrue(b.do_prepare())
        self.assertEquals('prepared', b.state)


    def test_db_copy(self):
        from ambry.orm.database import Database

        b = self.test_simple_build()
        self.assertTrue(b.do_prepare())
        l = b._library

        import tempfile

        try:
            td = tempfile.mkdtemp()

            db = Database('sqlite:////{}/{}.db'.format(td, b.identity.vid))
            db.open()

            db.copy_dataset(b.dataset)

            ds1 = b.dataset
            ds2 = db.dataset(ds1.vid)

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
        import os

        b = self.setup_bundle('simple')

        b = b.run()

        l = b._library
        p = list(b.partitions)[0]

        self.assertEquals(497054, int(sum(row[3] for row in p.stream(skip_header=True))))

        b.checkin()

        #self.assertEqual('build',p.location)

        self.assertEquals(497054, int(sum(row[3] for row  in p.stream( skip_header = True))))

        self.assertEqual(10000, len(list(p.stream(skip_header=True))))

        self.assertEqual(10001, len(list(p.stream(skip_header=False))))
