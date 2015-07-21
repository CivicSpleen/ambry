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
        self.assertIn('schema.csv', dir_list)
        self.assertIn('documentation.md', dir_list)

    def test_simple_prepare(self):
        """Build the simple bundle"""

        b = self.setup_bundle('simple')

        b.sync()  # This will sync the files back to the bundle's source dir

        self.assertEquals(7, len(b.dataset.files))
        file_names = [f.path for f in b.dataset.files]

        print file_names
        self.assertEqual([u'bundle.py', u'documentation.md', u'sourceschema', u'sources.csv', u'bundle.yaml',
                          u'build_meta', u'schema.csv'], file_names)

        self.assertEqual(13, len(b.dataset.configs))

        self.assertFalse(b.is_prepared)
        self.assertFalse(b.is_built)

        b.do_prepare()

        self.assertTrue(b.is_prepared)
        self.assertFalse(b.is_built)

        self.assertTrue(len(b.dataset.configs) > 10)

        self.assertEquals('Simple Example Bundle', b.metadata.about.title)
        self.assertEquals('Example Com', b.metadata.contacts.creator.org)

        # FIXME. This should work, but doesn't currenly, 20150704
        # self.assertEquals([u'example', u'demo'], b.metadata.about.tags )

        self.assertTrue(len(b.dataset.tables) == 1)
        self.assertEqual('example', b.dataset.tables[0].name)

        self.assertEqual([u'id', u'uuid', u'int', u'float', u'categorical', u'ordinal',
                          u'gaussian', u'triangle', u'exponential', u'bg_gvid', u'year', u'date'],
                         [c.name for c in b.dataset.tables[0].columns])

    def test_simple_build(self):
        """Build the simple bundle"""

        b = self.setup_bundle('simple')
        b.source_fs.remove('schema.csv')
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

        self.assertEquals(1,len(b.dataset.partitions))

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

        self.assertEquals(44, len(b.dataset.stats))

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


    def test_install(self):
        """Test copying a bundle to a remote, then streaming it back"""
        import os

        b = self.setup_bundle('simple')
        b.source_fs.remove('schema.csv')
        b = b.run()

        l = b._library
        p = list(b.partitions)[0]

        # Initially, its location is the build director
        self.assertEqual('build', list(b.partitions)[0].location)

        self.assertEquals(497054, int(sum(row[3] for row in l.stream_partition(p, skip_header=True))))

        b.checkin()

        self.assertEqual('build',p.location)

        self.assertEquals(497054, int(sum(row[3] for row  in l.stream_partition(p, skip_header = True))))

        self.assertEqual(10000, len(list(l.stream_partition(p, skip_header=True))))

        self.assertEqual(10001, len(list(l.stream_partition(p, skip_header=False))))

    def test_simple_sync(self):
        """Build the simple bundle"""
        import time
        from ambry.orm.file import File

        # The modification times for mem: files don't seem to change, so we use temp: instead
        b = self.setup_bundle('simple', source_url = 'temp://')
        self.assertTrue(b.do_sync() ) # This will sync the files back to the bundle's source dir

        header = (
            '"name","title","table","segment","time","space","grain","start_line","end_line",'+
            '"comment_lines","header_lines","description","url"'
        )

        lines = [
            '"2009-gs",,"geofile_schema",2009,2009,,,,,,,,"gs://1lKkKVBu0sHSwyuyuGRPMfmNqG8FIjVw2RYMLYgr2GX4"',
            '"2010-gs",,"geofile_schema",2010,2010,,,,,,,,"gs://1lKkKVBu0sHSwyuyuGRPMfmNqG8FIjVw2RYMLYgr2GX4"'
        ]

        time.sleep(2.0) # Make sure the mod time differes
        b.source_fs.setcontents('sources.csv',header+'\n'+lines[0])

        self.assertIn(('sources', 'ftr'),  b.do_sync())

        self.assertEqual(0, len(b.dataset.sources)) # Synced to record, but not to objects

        self.assertTrue(b.do_prepare())

        self.assertEqual(1, len(b.dataset.sources)) # Prepare syncs to objects

        #time.sleep(2.0)  # Make sure the mod time differes
        b.source_fs.setcontents('sources.csv', '\n'.join([header]+lines))

        b.do_prepare()

        self.assertEqual(1, len(b.dataset.sources)) # 2 line file hasn't been synced yet; prepare does nothgin

        self.assertIn(('sources', 'ftr'), b.do_sync())

        self.assertEqual(1, len(b.dataset.sources)) # Synced, but 2nd object only after prepare

        b.do_prepare()

        self.assertEqual(2, len(b.dataset.sources)) # Prepare created second object

        self.assertNotIn(('sources', 'ftr'), b.do_sync()) # No changes, should not sync

        b.dataset.sources = [b.dataset.sources[0]]

        self.assertEqual(1, len(b.dataset.sources))

        ##
        ## Source schema

        self.assertIsNone(b.build_source_files.file(File.BSFILE.SOURCESCHEMA).record.unpacked_contents)

        b.do_meta()

        self.assertEqual(13,len(b.build_source_files.file(File.BSFILE.SOURCESCHEMA).record.unpacked_contents))

        # Check that there are no sync opportunities
        print b.build_source_files.sync_dirs()
        self.assertFalse(any(e[1] for e in b.build_source_files.sync_dirs()))

        #print '!!!', b.source_fs.getcontents('source_schema.csv')

        bsf = b.build_source_files.file(File.BSFILE.SOURCESCHEMA)

        self.assertEquals(bsf.fs_hash, bsf.record.source_hash )

        # Modify the destination name
        time.sleep(2)
        b.source_fs.setcontents('source_schema.csv',
                                b.source_fs.getcontents('source_schema.csv')
                                .replace('geolevels,geolevels', 'geolevels,gl'))

        self.assertNotEquals(bsf.fs_hash, bsf.record.source_hash)


        self.assertIn(('sourceschema', 'ftr'), b.do_sync())  # No changes, should not sync