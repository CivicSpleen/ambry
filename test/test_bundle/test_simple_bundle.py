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
        b.sync()

        b = b.cast_to_build_subclass()
        self.assertEquals('synced', b.state)
        b.do_prepare()
        self.assertEquals('prepared', b.state)
        b.do_meta()

        def edit_pipeline(pl):
            from ambry.etl.pipeline import PrintRows, LogRate

            def prt(m):
                print m

            pl.build_last = [PrintRows(print_at='end'), LogRate(prt, 3000, '')]

        b.set_edit_pipeline(edit_pipeline)

        b.do_build()

        self.assertEquals(1,len(b.dataset.partitions))

        self.assertEquals(4,len(b.dataset.source_columns))

        return b

    def test_complete_build(self):
        """Build the simple bundle"""

        from geoid import civick, census

        b = self.setup_bundle('complete-build')
        b.sync()
        b = b.cast_to_build_subclass()
        self.assertEquals('synced', b.state)
        b.do_prepare()
        self.assertEquals('prepared', b.state)

        b.do_meta()

        def edit_pipeline(pl):
            from ambry.etl.pipeline import PrintRows, LogRate, Edit, WriteToSelectedPartition, WriteToPartition

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

            # assign a scalar to append, assign a list to replace
            pl.write_to_table = [WriteToSelectedPartition(select_part)] # WriteToSelectedPartition()

        b.set_edit_pipeline(edit_pipeline)

        b.do_build()

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
        self.assertEquals('synced', b.state)
        b.do_prepare()
        self.assertEquals('prepared', b.state)
        b.do_meta()

    def test_db_copy(self):
        from ambry.orm.database import Database

        b = self.test_simple_build()
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

        b = self.test_simple_build()
        l = b._library

        l.install_to_remote(b)

        #self.dump_database('partitions')

        p = l.partition(list(b.partitions)[0].vid)

        self.assertEqual(10000, len(list(l.stream_partition(p.vid))))

    def test_simple_meta(self):
        """Build the simple bundle"""

        b = self.setup_bundle('simple')
        b.sync()  # This will sync the files back to the bundle's source dir

        print list(b.source_fs.listdir())

        for i in [u'bundle.yaml', u'schema.csv', u'documentation.md', u'bundle.py', u'sources.csv']:
            pass

