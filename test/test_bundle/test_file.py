import unittest

from ambry.orm import File
from ambry.identity import DatasetNumber
from test import bundlefiles


class Test(unittest.TestCase):

    def setUp(self):

        super(Test,self).setUp()

        self.dsn = 'sqlite://' # Memory database

        self.ds_params = dict(
            vid=str(DatasetNumber(1, 1)), source='source', dataset='dataset'
        )


    def dump_database(self, db, table):

        for row in db.connection.execute("SELECT * FROM {}".format(table)):
            print row

    def compare_files(self, fs1, fs2, file_const):
        from ambry.util import md5_for_file
        from ambry.bundle.files import file_name
        self.assertEqual(md5_for_file(fs1.open(file_name(file_const))),
                         md5_for_file(fs2.open(file_name(file_const))))

    def test_file_load(self):
        """Basic operations on datasets"""
        from ambry.orm.database import Database
        from ambry.orm import File
        from fs.opener import fsopendir
        from ambry.bundle.files import BuildSourceFileAccessor
        from os.path import dirname
        from test import bundlefiles

        dn = str(DatasetNumber(1, 1))

        source_fs = fsopendir(dirname(bundlefiles.__file__))
        mem_fs = fsopendir("/tmp/foobar/") # fsopendir("mem://")

        db = Database(self.dsn)
        db.open()

        ds = db.new_dataset(**self.ds_params)
        db.commit()
        ds = db.dataset(dn)

        def test_a_file(file_const):

            # Store in the record
            bsfa1 = BuildSourceFileAccessor(ds, source_fs)
            bsf1 = bsfa1.file(file_const)
            bsf1.fs_to_record()

            # Back to the filesystem, then compare.
            bsfa2 = BuildSourceFileAccessor(db.dataset(dn), mem_fs)
            bsf2 = bsfa2.file(file_const)
            bsf2.record_to_fs()

            self.compare_files(source_fs, mem_fs, file_const)

        test_a_file(File.BSFILE.SOURCES)
        test_a_file(File.BSFILE.META)
        test_a_file(File.BSFILE.BUILD)

        bsfa = BuildSourceFileAccessor(ds, source_fs)

        for i in range(5):
            print i, bsfa.sync()

        db.commit()

        self.dump_database(db,'files')

    def test_file_sync(self):
        from ambry.orm.database import Database
        from fs.opener import fsopendir
        from ambry.bundle.files import BuildSourceFileAccessor
        from os.path import dirname
        from test import bundlefiles
        import time

        dn = str(DatasetNumber(1, 1))

        source_fs = fsopendir(dirname(bundlefiles.__file__))

        db = Database(self.dsn)
        db.open()

        db.new_dataset(**self.ds_params)
        db.commit()
        ds = db.dataset(dn)

        bsfa = BuildSourceFileAccessor(ds, source_fs)
        # Report 5 files to sync from file to record
        self.assertEquals(5, dict(bsfa.sync_dirs()).values().count('ftr'))
        # Report did 5 syncs
        self.assertEquals(5, dict(bsfa.sync()).values().count('ftr'))
        # Report non left to do.
        self.assertEquals(0, dict(bsfa.sync()).values().count('ftr'))

        bsf = ds.bsfile(File.BSFILE.META)
        bsf.modified = time.time()

        # Now there is one in the other direction
        self.assertEquals(1, dict(bsfa.sync()).values().count('rtf'))

    def test_sources_file(self):
        """Basic operations on datasets"""
        from ambry.orm.database import Database
        from fs.opener import fsopendir
        from os.path import dirname
        from test import bundlefiles
        from ambry.bundle.files import BuildSourceFileAccessor

        dn = str(DatasetNumber(1, 1))

        source_fs = fsopendir(dirname(bundlefiles.__file__))

        db = Database(self.dsn)
        db.open()
        db.new_dataset(**self.ds_params)
        db.commit()
        ds = db.dataset(dn)

        bsfa = BuildSourceFileAccessor(ds, source_fs)
        bsfa.sync()

        #self.dump_database(db, 'files')

        #ad = AttrDict(ds.bsfile(File.BSFILE.META).unpacked_contents)
        #for x in  ad.flatten():
        #    print x


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

