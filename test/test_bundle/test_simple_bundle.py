from test.test_base import TestBase

from ambry.bundle import Bundle

class Test(TestBase):



    def setup_bundle(self):
        from test import bundles
        from os.path import dirname, join
        from fs.opener import fsopendir

        db = self.new_database()

        mem_fs = fsopendir("mem://")
        build_fs = fsopendir("mem://")

        self.copy_bundle_files(fsopendir(join(dirname(bundles.__file__), 'example.com', 'simple')), mem_fs)

        return Bundle(self.new_db_dataset(), None, source_fs = mem_fs, build_fs =  build_fs)

    def test_simple_prepare(self):
        """Build the simple bundle"""

        b = self.setup_bundle()

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

        b = self.setup_bundle()
        b.sync()
        b = b.cast_to_subclass()
        self.assertEquals('synced',b.state)
        b.do_prepare()
        self.assertEquals('prepared', b.state)
        b.do_build()

        for p in b.dataset.partitions:
            print p.time_coverage
            print p.space_coverage

        self.assertEqual(3, len(b.dataset.partitions))
        self.assertEqual(1, len(b.dataset.tables))

        c = b._build_fs.getcontents(list(b._build_fs.walkfiles())[0])

        self.assertEquals(6001,len(c.splitlines()))

        self.assertEquals(39, len(b.dataset.stats))

        self.assertEquals('built', b.state )

def suite():
    import unittest
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite