
import unittest
import tempfile
import uuid
from ambry.orm import Column
from ambry.orm import Partition
from ambry.orm import Table
from ambry.orm import Dataset
from ambry.orm import Config
from ambry.orm import File
from ambry.orm import Code
from ambry.orm import ColumnStat
from sqlalchemy.orm import sessionmaker
from ambry.identity import DatasetNumber, PartitionNumber
from sqlalchemy.exc import IntegrityError

class Test(unittest.TestCase):

    def setUp(self):
        from sqlalchemy import create_engine

        super(Test,self).setUp()

        self.dsn = 'sqlite://'

        self.ds_params = dict(
            vid=str(DatasetNumber(1, 1)), source='source', dataset='dataset'
        )


    def dump_database(self, db, table):
        import sys
        from subprocess import check_output

        for row in db.connection.execute("SELECT * FROM {}".format(table)):
            print row

    def compare_files(self, fs1, fs2, file_const):
        from ambry.util import md5_for_file
        from ambry.bundle.files import file_name
        self.assertEqual(md5_for_file(fs1.open(file_name(file_const))),
                         md5_for_file(fs2.open(file_name(file_const))))

    def test_file_basic(self):
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

        db.commit()

        self.dump_database(db,'files')

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

