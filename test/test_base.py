"""
Created on Jun 22, 2012

@author: eric
"""

import unittest

from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql.expression import text

from six.moves.urllib.parse import urlparse

from ambry.identity import DatasetNumber
from ambry.orm import Database, Dataset
from ambry.orm.database import POSTGRES_SCHEMA_NAME
from ambry.run import get_runconfig


MISSING_POSTGRES_CONFIG_MSG = 'PostgreSQL is not configured properly. Add postgresql-test '\
    'to the database config of the ambry config.'
SAFETY_POSTFIX = 'ab1kde2'  # Prevents wrong database dropping.


class TestBase(unittest.TestCase):
    def setUp(self):

        super(TestBase, self).setUp()

        self.dsn = 'sqlite://'  # Memory database

        # Make an array of dataset numbers, so we can refer to them with a single integer
        self.dn = [str(DatasetNumber(x, x)) for x in range(1, 10)]

        self.db = None

    def tearDown(self):
        if hasattr(self, 'library'):
            self.library.database.close()

    def ds_params(self, n, source='source'):
        return dict(vid=self.dn[n], source=source, dataset='dataset')

    def get_rc(self, name='ambry.yaml'):
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
        # FIXME: this connection will not be closed properly in a postgres case.
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


class PostgreSQLTestBase(TestBase):
    """ Base class for database tests who requires postgresql database. """

    def setUp(self):
        super(PostgreSQLTestBase, self).setUp()
        # Create database and populate required fields.
        self._create_postgres_test_db()
        self.dsn = self.__class__.postgres_test_db_data['test_db_dsn']
        self.postgres_dsn = self.__class__.postgres_test_db_data['postgres_db_dsn']
        self.postgres_test_db = self.__class__.postgres_test_db_data['test_db_name']

    def tearDown(self):
        super(PostgreSQLTestBase, self).tearDown()
        self._drop_postgres_test_db()

    @classmethod
    def _drop_postgres_test_db(cls):
        # drop test database
        if hasattr(cls, 'postgres_test_db_data'):
            test_db_name = cls.postgres_test_db_data['test_db_name']
            assert test_db_name.endswith(SAFETY_POSTFIX), 'Can not drop database without safety postfix.'

            engine = create_engine(cls.postgres_test_db_data['postgres_db_dsn'])
            connection = engine.connect()
            connection.execute('commit')
            connection.execute('DROP DATABASE {};'.format(test_db_name))
            connection.execute('commit')
            connection.close()
        else:
            # no database were created.
            pass

    @classmethod
    def _create_postgres_test_db(cls, conf=None):
        if not conf:
            conf = get_runconfig()

        # we need valid postgres dsn.
        if not ('database' in conf.dict and 'postgresql-test' in conf.dict['database']):
            # example of the config
            # database:
            #     postgresql-test: postgresql+psycopg2://ambry:secret@127.0.0.1/ambry
            raise unittest.SkipTest(MISSING_POSTGRES_CONFIG_MSG)

        postgres_user = 'ambry'  # FIXME: take it from the conf.
        dsn = conf.dict['database']['postgresql-test']
        parsed_url = urlparse(dsn)
        db_name = parsed_url.path.replace('/', '')
        test_db_name = '{}_test_{}'.format(db_name, SAFETY_POSTFIX)
        postgres_db_dsn = parsed_url._replace(path='postgres').geturl()
        test_db_dsn = parsed_url._replace(path=test_db_name).geturl()

        # connect to postgres database because we need to create database for tests.
        engine = create_engine(postgres_db_dsn, poolclass=NullPool)
        with engine.connect() as connection:
            # we have to close opened transaction.
            connection.execute('commit')

            # drop test database created by previuos run (control + c case).
            if cls.postgres_db_exists(test_db_name, engine):
                assert test_db_name.endswith(SAFETY_POSTFIX), 'Can not drop database without safety postfix.'
                while True:
                    delete_it = raw_input(
                        '\nTest database with {} name already exists. Can I delete it (Yes|No): '.format(test_db_name))
                    if delete_it.lower() == 'yes':
                        try:
                            connection.execute('DROP DATABASE {};'.format(test_db_name))
                            connection.execute('commit')
                        except:
                            connection.execute('rollback')
                        break

                    elif delete_it.lower() == 'no':
                        break

            # check for template with pg_tgrm extension.
            cls.pg_trgm_is_installed = cls.postgres_db_exists('template0_trgm', connection)
            # FIXME: Check for multicorn too.

            if not cls.pg_trgm_is_installed:
                raise unittest.SkipTest(
                    'Can not find template with pg_trgm support. See README.rst for details.')

            query = 'CREATE DATABASE {} OWNER {} TEMPLATE template0_trgm encoding \'UTF8\';'\
                .format(test_db_name, postgres_user)
            connection.execute(query)
            connection.execute('commit')
            connection.close()

        # create db schemas needed by ambry.
        engine = create_engine(test_db_dsn, poolclass=NullPool)
        engine.execute('CREATE SCHEMA IF NOT EXISTS {}'.format(POSTGRES_SCHEMA_NAME))

        cls.postgres_test_db_data = {
            'test_db_name': test_db_name,
            'test_db_dsn': test_db_dsn,
            'postgres_db_dsn': postgres_db_dsn}
        return cls.postgres_test_db_data

    @classmethod
    def postgres_db_exists(self, db_name, conn):
        """ Returns True if database with given name exists in the postgresql. """
        result = conn\
            .execute(
                text('SELECT 1 FROM pg_database WHERE datname=:db_name;'), db_name=db_name)\
            .fetchall()
        return result == [(1,)]
