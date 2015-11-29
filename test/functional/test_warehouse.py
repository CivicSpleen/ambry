# -*- coding: utf-8 -*-
import os
import stat

from test.factories import PartitionFactory

from ambry_sources import MPRowsFile
from ambry_sources.sources import GeneratorSource, SourceSpec

from ambry.library.warehouse import Warehouse

from test.test_base import TestBase, PostgreSQLTestBase


class Mixin(object):
    """ Requires successors to inherit from TestBase and provide _get_library method. """

    def test_select_query(self):
        # FIXME: Check that for postgres only.
        if isinstance(self, PostgreSQLTest):
            assert_shares_group(user='postgres')
        library = self._get_library()

        # FIXME: Find the way how to initialize bundle with partitions and drop partition creation.
        bundle = self.setup_bundle(
            'simple', source_url='temp://', build_url='temp://', library=library)
        PartitionFactory._meta.sqlalchemy_session = bundle.dataset.session
        partition1 = PartitionFactory(dataset=bundle.dataset)
        bundle.wrap_partition(partition1)

        def gen():
            # generate header
            yield ['col1', 'col2']

            # generate first row
            yield [0, 0]

            # generate second row
            yield [1, 1]

        try:
            datafile = MPRowsFile(bundle.build_fs, partition1.cache_key)
            datafile.load_rows(GeneratorSource(SourceSpec('foobar'), gen()))
            partition1._datafile = datafile

            # FIXME: ambry_sources should care about *.mpr permissions. Create an issue with test case.
            # Waiting for https://github.com/CivicKnowledge/ambry_sources/issues/20. When the issue will be
            # resolved, remove permissions setting.
            if datafile.syspath.startswith('/tmp'):
                parts = datafile.syspath.split(os.sep)
                parts[0] = os.sep
                for i, dir_ in enumerate(parts):
                    if dir_ in ('/', 'tmp'):
                        continue
                    path = parts[:i]
                    path.append(dir_)
                    path = os.path.join(*path)
                    if not is_group_readable(path):
                        os.chmod(path, get_perm(path) | stat.S_IRGRP | stat.S_IXGRP)

            rows = library.warehouse.query('SELECT * FROM {};'.format(partition1.vid))
            self.assertEqual(rows, [(0, 0), (1, 1)])
        finally:
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
    def get_rc(cls):
        rc = TestBase.get_rc()
        # use file database for library for that test case.
        cls._real_test_database = rc.library.database
        rc.library.database = 'sqlite:////tmp/test-warehouse.db'
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

    def _test_materialized_view(self):
        # create materialized view wrd as select * from words;
        library = self._get_library()

        # FIXME: Find the way how to initialize bundle with partitions and drop partition creation.
        bundle = self.setup_bundle(
            'simple', source_url='temp://', build_url='temp://', library=library)
        PartitionFactory._meta.sqlalchemy_session = bundle.dataset.session
        partition1 = PartitionFactory(dataset=bundle.dataset)
        bundle.wrap_partition(partition1)

        def gen():
            # generate header
            yield ['col1', 'col2']

            # generate first row
            yield [0, 0]

            # generate second row
            yield [1, 1]

        try:
            datafile = MPRowsFile(bundle.build_fs, partition1.cache_key)
            datafile.load_rows(GeneratorSource(SourceSpec('foobar'), gen()))
            partition1._datafile = datafile

            # FIXME: ambry_sources should care about *.mpr permissions. Create an issue with test case.
            # Waiting for https://github.com/CivicKnowledge/ambry_sources/issues/20. When the issue will be
            # resolved, remove permissions setting.
            if datafile.syspath.startswith('/tmp'):
                parts = datafile.syspath.split(os.sep)
                parts[0] = os.sep
                for i, dir_ in enumerate(parts):
                    if dir_ in ('/', 'tmp'):
                        continue
                    path = parts[:i]
                    path.append(dir_)
                    path = os.path.join(*path)
                    if not is_group_readable(path):
                        os.chmod(path, get_perm(path) | stat.S_IRGRP | stat.S_IXGRP)

            library.warehouse.materialize(partition1.vid)

            # assert materialized view created.
            rows = library.warehouse.query('SELECT * FROM {};'.format(partition1.vid))
            self.assertEqual(rows, [(0, 0), (1, 1)])
        finally:
            # FIXME: Use library.warehouse.close() instead.
            library.database.close()


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
