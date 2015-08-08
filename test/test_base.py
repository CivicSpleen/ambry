"""
Created on Jun 22, 2012

@author: eric
"""

# Monkey patch logging, because I really don't understand logging

import unittest

from ambry.identity import DatasetNumber

from ambry.orm import Database, Dataset


class TestBase(unittest.TestCase):
    def setUp(self):

        super(TestBase, self).setUp()

        self.dsn = 'sqlite://'  # Memory database

        # Make an array of dataset numbers, so we can refer to them with a single integer
        self.dn = [str(DatasetNumber(x, x)) for x in range(1, 10)]

        self.db = None

    def ds_params(self, n, source='source'):
        return dict(vid=self.dn[n], source=source, dataset='dataset')

    def get_rc(self, name='ambry.yaml'):
        from ambry.run import get_runconfig
        import os
        from test import bundlefiles

        def bf_dir(fn):
            return os.path.join(os.path.dirname(bundlefiles.__file__), fn)

        return get_runconfig(bf_dir('ambry.yaml'))

    def new_dataset(self, n=1, source='source'):
        return Dataset(**self.ds_params(n, source=source))

    def new_db_dataset(self, db, n=1, source='source'):
        return db.new_dataset(**self.ds_params(n, source=source))

    def copy_bundle_files(self, source, dest):
        from ambry.bundle.files import file_info_map
        from fs.errors import ResourceNotFoundError

        for const_name, (path, clz) in file_info_map.items():
            try:
                dest.setcontents(path, source.getcontents(path))
            except ResourceNotFoundError:
                pass

    def dump_database(self, table, db=None):

        if db is None:
            db = self.db

        for row in db.connection.execute('SELECT * FROM {}'.format(table)):
            print row

    def new_database(self):
        db = Database(self.dsn)
        db.open()

        return db

    def setup_bundle(self, name, source_url=None, build_url=None, library=None):
        """Configure a bundle from existing sources"""
        from test import bundles
        from os.path import dirname, join
        from fs.opener import fsopendir
        from fs.errors import ParentDirectoryMissingError
        from ambry.library import new_library
        import yaml

        if not library:
            rc = self.get_rc()
            library = new_library(rc)
        self.library = library

        self.db = self.library._db

        if not source_url:
            source_url = 'mem://{}/source'.format(name)

        if not build_url:
            build_url = 'mem://{}/build'.format(name)

        try:  # One fails for real directories, the other for mem:
            assert fsopendir(source_url, create_dir=True).isdirempty('/')
            assert fsopendir(build_url, create_dir=True).isdirempty('/')
        except ParentDirectoryMissingError:
            assert fsopendir(source_url).isdirempty('/')
            assert fsopendir(build_url).isdirempty('/')

        test_source_fs = fsopendir(join(dirname(bundles.__file__), 'example.com', name))

        config = yaml.load(test_source_fs.getcontents('bundle.yaml'))
        b = self.library.new_from_bundle_config(config)
        b.set_file_system(source_url=source_url, build_url=build_url)

        self.copy_bundle_files(test_source_fs, b.source_fs)

        return b

    def new_bundle(self):
        """Configure a bundle from existing sources"""
        from ambry.library import new_library
        from ambry.bundle import Bundle

        rc = self.get_rc()

        self.library = new_library(rc)

        self.db = self.library._db

        return Bundle(self.new_db_dataset(self.db), self.library, build_url='mem://', source_url='mem://')
