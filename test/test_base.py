"""
Created on Jun 22, 2012

@author: eric
"""
import os

import unittest

from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql.expression import text

from six.moves.urllib.parse import urlparse
from six.moves import input as six_input

from ambry.identity import DatasetNumber
from ambry.orm import Database, Dataset
from ambry.orm.database import POSTGRES_SCHEMA_NAME, POSTGRES_PARTITION_SCHEMA_NAME
import ambry.run

MISSING_POSTGRES_CONFIG_MSG = 'PostgreSQL is not configured properly. Add test-postgres '\
    'to the database section of the ambry config.'
SAFETY_POSTFIX = 'ab1kde2'  # Prevents wrong database dropping.


class TestBase(unittest.TestCase):

    def setUp(self):
        import uuid

        super(TestBase, self).setUp()

        # WAARNING! This path and dsn is only used if it is explicitly referenced.
        # Otherwise, the get_rc() will use a database specified in the
        # test rc file.
        self.db_path = '/tmp/ambry-test-{}.db'.format(str(uuid.uuid4()))

        self.dsn = 'sqlite:///{}'.format(self.db_path)

        # Make an array of dataset numbers, so we can refer to them with a single integer
        self.dn = [str(DatasetNumber(x, x)) for x in range(1, 10)]

        self.db = None
        self._library = None

    def tearDown(self):

        if self._library:
            self._library.close()

    def ds_params(self, n, source='source'):
        return dict(vid=self.dn[n], source=source, dataset='dataset')

    @staticmethod  # So it can be called from either setUp or setUpClass
    def get_rc(rewrite=True):
        """Create a new config file for test and return the RunConfig.

         This method will start with the user's default Ambry configuration, but will replace the
         library.filesystem_root with the value of filesystem.test, then depending on the value of the AMBRY_TEST_DB
         environmental variable, it will set library.database to the DSN of either database.test-sqlite or
         database.test-postrgres

        """

        from fs.opener import fsopendir

        config = ambry.run.load()  # not cached; get_config is

        dbname = os.environ.get('AMBRY_TEST_DB', 'sqlite')

        orig_root = config.library.filesystem_root
        root_dir = config.filesystem.test.format(root=orig_root)

        dsn = config.get('database', {}).get('test-{}'.format(dbname), 'sqlite:///{root}/library.db')

        config.library.filesystem_root = root_dir
        config.library.database = dsn
        config.accounts = None

        test_root = fsopendir(root_dir, create_dir=True)

        if rewrite:
            with test_root.open('.ambry.yaml', 'w', encoding='utf-8', ) as f:
                config.loaded = None
                config.dump(f)

        return ambry.run.get_runconfig(test_root.getsyspath('.ambry.yaml'))

    @classmethod
    def config(cls):
        return cls.get_rc()

    @classmethod  # So it can be called from either setUp or setUpClass
    def _get_library(cls, config):
        from ambry.library import new_library
        return new_library(config if config else cls.get_rc())

    def library(self, config=None):

        if not self._library:
            self._library = self._get_library(config)

        return self._library

    @staticmethod  # So it can be called from either setUp or setUpClass
    def _import_bundles(library, clean=True, force_import=False):

        from test import bundle_tests
        import os

        if clean:
            library.clean()
            library.create()

        bundles = list(library.bundles)

        if len(bundles) == 0 or force_import:
            library.import_bundles(os.path.dirname(bundle_tests.__file__), detach=True)

    def import_bundles(self, clean=True, force_import=False):
        """
        Import the test bundles into the library, from the test.test_bundles directory
        :param clean: If true, drop the library first.
        :param force_import: If true, force importaing even if the library already has bundles.
        :return:
        """

        library = self.library()

        self._import_bundles(library, clean, force_import)

    def import_single_bundle(self, cache_path, clean=True):
        from test import bundle_tests

        l = self.library()
        print l.database.dsn

        if clean:
            l.clean()
            l.create()

        orig_source = os.path.join(os.path.dirname(bundle_tests.__file__), cache_path)
        l.import_bundles(orig_source, detach=True, force=True)

        b = next(b for b in l.bundles).cast_to_subclass()
        b.clean()
        b.sync_in()
        return b

    def new_dataset(self, n=1, source='source'):
        return Dataset(**self.ds_params(n, source=source))

    def new_db_dataset(self, db, n=1, source='source'):
        return db.new_dataset(**self.ds_params(n, source=source))

    def copy_bundle_files(self, source, dest):
        from ambry.bundle.files import file_info_map
        from fs.errors import ResourceNotFoundError

        for const_name, (path, clz) in list(file_info_map.items()):
            try:
                dest.setcontents(path, source.getcontents(path))
            except ResourceNotFoundError:
                pass

    def dump_database(self, table, db=None):

        if db is None:
            db = self.db

        for row in db.connection.execute('SELECT * FROM {};'.format(table)):
            print(row)

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
        import yaml
        from ambry.util import parse_url_to_dict

        if not library:
            library = self.__class__.library()

        self.db = library._db

        if not source_url:
            source_url = 'mem://source'.format(name)

        if not build_url:
            build_url = 'mem://build'.format(name)

        for fs_url in (source_url, build_url):
            d = parse_url_to_dict(fs_url)

            # For persistent fs types, make sure it is empty before the test.
            if d['scheme'] not in ('temp', 'mem'):
                assert fsopendir(fs_url).isdirempty('/')

        test_source_fs = fsopendir(join(dirname(bundles.__file__), 'example.com', name))

        config = yaml.load(test_source_fs.getcontents('bundle.yaml'))

        b = library.new_from_bundle_config(config)
        b.set_file_system(source_url=source_url, build_url=build_url)

        self.copy_bundle_files(test_source_fs, b.source_fs)

        return b


class ConfigDatabaseTestBase(TestBase):
    """ Always use database engine from config as library database.

    Note:
        This means that subclasses should be ready to work on any engine from file sqlite, memory sqlite,
        postgres set. Do not use that class for test who requires specific version of the engine.
    """

    @staticmethod  # So it can be called from either setUp or setUpClass
    def get_rc(rewrite=True):
        """Create a new config file for test and return the RunConfig.

         This method will start with the user's default Ambry configuration, but will replace the
         library.filesystem_root with the value of filesystem.test, then depending on the value of the AMBRY_TEST_DB
         environmental variable, it will set library.database to the DSN of either database.test-sqlite or
         database.test-postrgres

        """
        rc = TestBase.get_rc()
        if rc.library.database.startswith('postgresql'):
            # create test database and write it to the config.
            test_db = PostgreSQLTestBase._create_postgres_test_db()
            rc.library.database = test_db['test_db_dsn']
        return rc

    def tearDown(self):
        # FIXME: Run for postgres only.
        # PostgreSQLTestBase._drop_postgres_test_db()
        pass


class PostgreSQLTestBase(TestBase):
    """ Base class for database tests who requires postgresql database.

    Note:
        If postgres is not installed all tests of all subclasses will be skipped.

    """

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
        if hasattr(cls, 'postgres_test_db_data'):
            # drop test database
            test_db_name = cls.postgres_test_db_data['test_db_name']
            assert test_db_name.endswith(SAFETY_POSTFIX), 'Can not drop database without safety postfix.'

            engine = create_engine(cls.postgres_test_db_data['postgres_db_dsn'])
            connection = engine.connect()
            connection.execute('COMMIT;')
            connection.execute('DROP DATABASE {};'.format(test_db_name))
            connection.execute('COMMIT;')
            connection.close()
        else:
            # database was not created.
            pass

    @classmethod
    def _create_postgres_test_db(cls, conf=None):
        db = os.environ.get('AMBRY_TEST_DB', 'sqlite')
        if db != 'postgres':
            raise unittest.SkipTest('Postgres tests are disabled.')
        if not conf:
            conf = TestBase.get_rc()  # get_runconfig()

        # we need valid postgres dsn.
        if not ('database' in conf and 'test-postgres' in conf['database']):
            # example of the config
            # database:
            #     test-postgres: postgresql+psycopg2://user:pass@127.0.0.1/ambry
            raise unittest.SkipTest(MISSING_POSTGRES_CONFIG_MSG)
        dsn = conf.database['test-postgres']
        parsed_url = urlparse(dsn)
        postgres_user = parsed_url.username
        db_name = parsed_url.path.replace('/', '')
        test_db_name = '{}_test_{}'.format(db_name, SAFETY_POSTFIX)
        postgres_db_dsn = parsed_url._replace(path='postgres').geturl()
        test_db_dsn = parsed_url._replace(path=test_db_name).geturl()

        # connect to postgres database because we need to create database for tests.
        engine = create_engine(postgres_db_dsn, poolclass=NullPool)
        with engine.connect() as connection:
            # we have to close opened transaction.
            connection.execute('COMMIT;')

            # drop test database created by previuos run (control + c case).
            if cls.postgres_db_exists(test_db_name, engine):
                assert test_db_name.endswith(SAFETY_POSTFIX), 'Can not drop database without safety postfix.'
                while True:
                    delete_it = six_input(
                        '\nTest database with {} name already exists. Can I delete it (Yes|No): '
                        .format(test_db_name))
                    if delete_it.lower() == 'yes':
                        try:
                            connection.execute('DROP DATABASE {};'.format(test_db_name))
                            connection.execute('COMMIT;')
                        except:
                            connection.execute('ROLLBACK;')
                        break

                    elif delete_it.lower() == 'no':
                        break

            #
            # check for test template with required extensions.

            TEMPLATE_NAME = 'template0_ambry_test'
            cls.test_template_exists = cls.postgres_db_exists(TEMPLATE_NAME, connection)

            if not cls.test_template_exists:
                raise unittest.SkipTest(
                    'Tests require custom postgres template db named {}. '
                    'See DEVEL-README.md for details.'.format(TEMPLATE_NAME))

            query = 'CREATE DATABASE {} OWNER {} TEMPLATE {} encoding \'UTF8\';'\
                .format(test_db_name, postgres_user, TEMPLATE_NAME)
            connection.execute(query)
            connection.execute('COMMIT;')
            connection.close()

        # create db schemas needed by ambry.
        engine = create_engine(test_db_dsn, poolclass=NullPool)
        engine.execute('CREATE SCHEMA IF NOT EXISTS {};'.format(POSTGRES_SCHEMA_NAME))
        engine.execute('CREATE SCHEMA IF NOT EXISTS {};'.format(POSTGRES_PARTITION_SCHEMA_NAME))

        # verify all modules needed by tests are installed.
        with engine.connect() as conn:

            if not cls.postgres_extension_installed('pg_trgm', conn):
                raise unittest.SkipTest(
                    'Can not find template with pg_trgm extension. See DEVEL-README.md for details.')

            if not cls.postgres_extension_installed('multicorn', conn):
                raise unittest.SkipTest(
                    'Can not find template with multicorn extension. See DEVEL-README.md for details.')

        cls.postgres_test_db_data = {
            'test_db_name': test_db_name,
            'test_db_dsn': test_db_dsn,
            'postgres_db_dsn': postgres_db_dsn}
        return cls.postgres_test_db_data

    @classmethod
    def get_rc(cls):
        rc = TestBase.get_rc()
        if rc.library.database.startswith('postgresql'):
            # Force library to use test db
            rc.library.database = cls.postgres_test_db_data['test_db_dsn']
        return rc

    @classmethod
    def postgres_db_exists(cls, db_name, conn):
        """ Returns True if database with given name exists in the postgresql. """
        result = conn\
            .execute(
                text('SELECT 1 FROM pg_database WHERE datname=:db_name;'), db_name=db_name)\
            .fetchall()
        return result == [(1,)]

    @classmethod
    def postgres_extension_installed(cls, extension, conn):
        """ Returns True if extension with given name exists in the postgresql. """
        result = conn\
            .execute(
                text('SELECT 1 FROM pg_extension WHERE extname=:extension;'), extension=extension)\
            .fetchall()
        return result == [(1,)]
