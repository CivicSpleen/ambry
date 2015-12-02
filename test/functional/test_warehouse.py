# -*- coding: utf-8 -*-
import os
import stat
import unittest

from semantic_version import Version

from test.factories import PartitionFactory

import ambry_sources
from ambry_sources import MPRowsFile
from ambry_sources.sources import GeneratorSource, SourceSpec

from test.test_base import TestBase, PostgreSQLTestBase

AMBRY_SOURCES_VERSION = getattr(ambry_sources, '__version__', None) or ambry_sources.__meta__.__version__


class Mixin(object):
    """ Requires successors to inherit from TestBase and provide _get_library method. """

    # helpers
    def _get_generator_source(self):
        def gen():
            # generate header
            yield ['col1', 'col2']

            # generate first row
            yield [0, 0]

            # generate second row
            yield [1, 1]
        return GeneratorSource(SourceSpec('foobar'), gen())

    def test_install_and_query_mpr(self):
        if isinstance(self, PostgreSQLTest):
            if Version(AMBRY_SOURCES_VERSION) < Version('0.1.6'):
                raise unittest.SkipTest('Need ambry_sources >= 0.1.6. Update your installation.')
            assert_shares_group(user='postgres')
        library = self._get_library()

        # FIXME: Find the way how to initialize bundle with partitions and drop partition creation.
        bundle = self.setup_bundle(
            'simple', source_url='temp://', build_url='temp://', library=library)
        PartitionFactory._meta.sqlalchemy_session = bundle.dataset.session
        partition1 = PartitionFactory(dataset=bundle.dataset)
        bundle.wrap_partition(partition1)

        try:
            datafile = MPRowsFile(bundle.build_fs, partition1.cache_key)
            datafile.load_rows(self._get_generator_source())
            partition1._datafile = datafile
            rows = library.warehouse.query('SELECT * FROM {};'.format(partition1.vid))
            self.assertEqual(rows, [(0, 0), (1, 1)])
        finally:
            library.warehouse.close()
            library.database.close()

    def test_install_and_query_materialized_table(self):
        # materialized view for postgres and readonly table for sqlite.
        if isinstance(self, PostgreSQLTest):
            if Version(AMBRY_SOURCES_VERSION) < Version('0.1.6'):
                raise unittest.SkipTest('Need ambry_sources >= 0.1.6. Update your installation.')

        library = self._get_library()

        # FIXME: Find the way how to initialize bundle with partitions and drop partition creation.
        bundle = self.setup_bundle(
            'simple', source_url='temp://', build_url='temp://', library=library)
        PartitionFactory._meta.sqlalchemy_session = bundle.dataset.session
        partition1 = PartitionFactory(dataset=bundle.dataset)
        bundle.wrap_partition(partition1)

        try:
            datafile = MPRowsFile(bundle.build_fs, partition1.cache_key)
            datafile.load_rows(self._get_generator_source())
            partition1._datafile = datafile

            # materialize partition (materialize view for postgres, readonly table for sqlite)
            library.warehouse.materialize(partition1.vid)

            # query partition.
            rows = library.warehouse.query('SELECT * FROM {};'.format(partition1.vid))

            # now drop the *.mpr file and check again. Query should return the same data.
            #
            syspath = datafile.syspath
            os.remove(syspath)
            self.assertFalse(os.path.exists(syspath))
            rows = library.warehouse.query('SELECT * FROM {};'.format(partition1.vid))
            self.assertEqual(rows, [(0, 0), (1, 1)])
        finally:
            library.warehouse.close()
            # FIXME: Use library.warehouse.close() only.
            library.database.close()


class InMemorySQLiteTest(TestBase, Mixin):

    @classmethod
    def get_rc(cls):
        rc = TestBase.get_rc()
        # use file database for library for that test case.
        cls._real_test_database = rc.library.database
        rc.library.database = 'sqlite://'
        return rc

    def _get_library(self):
        library = self.library()

        # assert it is in-memory database.
        assert library.database.dsn == 'sqlite://'

        return library


class FileSQLiteTest(TestBase, Mixin):

    @classmethod
    def setUpClass(cls):
        TestBase.setUpClass()
        cls._warehouse_db = 'sqlite:////tmp/test-warehouse.db'

    def tearDown(self):
        super(self.__class__, self).tearDown()
        os.remove(self._warehouse_db.replace('sqlite:///', ''))

    @classmethod
    def get_rc(cls):
        rc = TestBase.get_rc()
        # use file database for library for that test case.
        if not rc.library.database == cls._warehouse_db:
            cls._real_test_database = rc.library.database
            rc.library.database = cls._warehouse_db
        return rc

    @classmethod
    def tearDownClass(cls):
        rc = TestBase.get_rc()
        if rc.library.database != cls._real_test_database:
            # restore database
            rc.library.database = cls._real_test_database

    def _get_library(self):
        library = self.library()

        # assert it is file database.
        assert library.database.exists()
        return library


class PostgreSQLTest(PostgreSQLTestBase, Mixin):

    @classmethod
    def get_rc(cls):
        rc = TestBase.get_rc()
        # replace database with file database.
        cls._real_test_database = rc.library.database
        rc.library.database = cls.postgres_test_db_data['test_db_dsn']
        return rc

    @classmethod
    def tearDownClass(cls):
        rc = TestBase.get_rc()
        real_test_database = getattr(cls, '_real_test_database', None)
        if real_test_database and rc.library.database != real_test_database:
            # restore database
            rc.library.database = real_test_database
        PostgreSQLTestBase.tearDownClass()

    def _get_library(self):
        library = self.library()

        # assert it is file database.
        assert library.database.exists()
        return library


def assert_shares_group(user=''):
    """ Checks that the given user shares group with user who executes tests.

    Args:
        user (str): system username

    Raises:
        AssertionError: if given user is not the member of the tests executor group.

    """
    assert user, 'user is required attribute.'
    import getpass
    import grp
    import pwd
    current_user_group_id = pwd.getpwnam(getpass.getuser()).pw_gid
    current_user_group = grp.getgrgid(current_user_group_id).gr_name

    other_user_groups = [g.gr_name for g in grp.getgrall() if user in g.gr_mem]
    if current_user_group not in other_user_groups:
        details_link = 'https://github.com/CivicKnowledge/ambry_sources#making-mpr-files-readable-by-postgres-user'
        raise AssertionError(
            'This test requires postgres user to be in the {} group.\n'
            'Hint: see {} for details.'.format(current_user_group, details_link))


def is_group_readable(filepath):
    """ Returns True if given file is group readable, otherwise returns False.

    Args:
        filepath (str):

    """
    st = os.stat(filepath)
    return bool(st.st_mode & stat.S_IRGRP)


def get_perm(filepath):
    return stat.S_IMODE(os.lstat(filepath)[stat.ST_MODE])
