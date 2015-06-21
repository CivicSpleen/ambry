from test.test_base import TestBase

from ambry.bundle import Bundle

class Test(TestBase):

    def setup_bundle(self, name):
        from test import bundles
        from os.path import dirname, join
        from fs.opener import fsopendir
        from ambry.library import new_library

        rc = self.get_rc()

        self.library = new_library(rc)

        self.db = self.library._db

        mem_fs = fsopendir("mem://")
        build_fs = fsopendir("mem://")

        self.copy_bundle_files(fsopendir(join(dirname(bundles.__file__), 'example.com', name)), mem_fs)

        return Bundle(self.new_db_dataset(), self.library, source_fs = mem_fs, build_fs =  build_fs)

    def test_simple_prepare(self):
        """Build the simple bundle"""

        b = self.setup_bundle('simple')

        b.sync()  # This will sync the files back to the bundle's source dir

        self.assertEquals(7,len(b.dataset.files))
        file_names = [ f.path for f in b.dataset.files]

        self.assertEqual([u'bundle.py', u'documentation.md', u'sources.csv', u'bundle.yaml',
                          u'build_meta', u'schema.csv', u'column_map.csv'], file_names)

        self.assertTrue(len(b.dataset.configs) == 10)

        self.assertFalse(b.builder.is_prepared)
        self.assertFalse(b.builder.is_built)

        b.prepare()

        self.assertTrue(b.builder.is_prepared)
        self.assertFalse(b.builder.is_built)

        self.assertTrue(len(b.dataset.configs) > 10)

        self.assertEquals('Simple Example Bundle',b.metadata.about.title)
        self.assertEquals('Example Com', b.metadata.contact_source['creator.org'] )
        self.assertEquals([u'example', u'demo'], b.metadata.about.tags )

        self.assertTrue(len(b.dataset.tables) == 1 )
        self.assertEqual('example', b.dataset.tables[0].name)

        self.assertEqual([u'id', u'uuid', u'int', u'float'],  [ c.name for c in b.dataset.tables[0].columns ] )


    def test_simple_build(self):
        """Build the simple bundle"""

        b = self.setup_bundle('simple')
        b.sync()
        b = b.cast_to_subclass()
        self.assertEquals('synced',b.state)
        b.do_prepare()
        self.assertEquals('prepared', b.state)
        b.do_build()

        self.assertEquals([2000, 2001, 2002], b.dataset.partitions[0].time_coverage)
        self.assertEquals([u'0O0101', u'0O0102'], b.dataset.partitions[0].space_coverage)
        self.assertEquals([u'2qZZZZZZZZZZ'],b.dataset.partitions[0].grain_coverage)

        self.assertEqual(1, len(b.dataset.partitions))
        self.assertEqual(1, len(b.dataset.tables))

        c = b._build_fs.getcontents(list(b._build_fs.walkfiles())[0])

        self.assertEquals(501,len(c.splitlines()))

        self.assertEquals(12, len(b.dataset.stats))

        self.assertEquals('built', b.state )

        return b

    def test_complete_build(self):
        """Build the simple bundle"""

        b = self.setup_bundle('complete')
        b.sync()
        b = b.cast_to_subclass()
        self.assertEquals('synced', b.state)
        b.do_prepare()
        self.assertEquals('prepared', b.state)
        b.do_build()

        self.assertEquals([2000, 2001, 2002, 2010], b.dataset.partitions[0].time_coverage)
        self.assertEquals([u'0O0001', u'0O0002', u'0O0003', u'0O0101', u'0O0102', u'0O0103'],
                          b.dataset.partitions[0].space_coverage)
        self.assertEquals([u'2qZZZZZZZZZZ'], b.dataset.partitions[0].grain_coverage)

        self.assertEquals([1998, 1999, 2000, 2002, 2003, 2004, 2012], b.dataset.partitions[2].time_coverage)
        self.assertEquals([u'0O0001', u'0O0002', u'0O0003', u'0O0101', u'0O0102', u'0O0103'],
                          b.dataset.partitions[2].space_coverage)
        self.assertEquals([u'2qZZZZZZZZZZ'], b.dataset.partitions[2].grain_coverage)

        self.assertEqual(3, len(b.dataset.partitions))
        self.assertEqual(1, len(b.dataset.tables))

        c = b._build_fs.getcontents(list(b._build_fs.walkfiles())[0])

        self.assertEquals(6001, len(c.splitlines()))

        self.assertEquals(39, len(b.dataset.stats))

        self.assertEquals('built', b.state)

    def test_db_copy(self):
        from ambry.orm.database import Database
        from ambry.util import copy_file_or_flo
        import os

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

        b = self.test_simple_build()
        l = b._library

        l.install_to_remote(b)

        remote = l.remote('test')

        p = l.partition(list(b.partitions)[0].vid)

        for row in l.stream_partition(p.vid):
            print row

def suite():
    import unittest
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite