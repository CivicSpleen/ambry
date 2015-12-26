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
from ambry.util import parse_url_to_dict, unparse_url_dict

MISSING_POSTGRES_CONFIG_MSG = 'PostgreSQL is not configured properly. Add test-postgres '\
    'to the database section of the ambry config.'
SAFETY_POSTFIX = 'ab1kde2'  # Prevents wrong database dropping.


class TestBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbname = os.environ.get('AMBRY_TEST_DB', 'sqlite')
        config = ambry.run.load()  # not cached; get_config is
        cls.test_dsn_key = 'test-{}'.format(cls.dbname)
        cls.library_test_dsn = config.get('database', {}).get(cls.test_dsn_key)
        cls.library_prod_dsn = config.library.database
        if not cls.library_test_dsn:
            if cls.dbname == 'sqlite':
                d = parse_url_to_dict(config.library.database)
                last_part = d['path'].split('/')[-1]
                if last_part.endswith('.db'):
                    new_last_part = last_part.replace('.db', '_test_1k.db')
                else:
                    new_last_part = last_part + '_test_1k'
                d['path'] = d['path'].rstrip(last_part) + new_last_part
                cls.library_test_dsn = unparse_url_dict(d)
            elif cls.dbname == 'postgres':
                # create test database dsn and write it to the config.
                parsed_url = urlparse(config.library.database)
                db_name = parsed_url.path.replace('/', '')
                test_db_name = '{}_test_{}'.format(db_name, SAFETY_POSTFIX)
                cls.library_test_dsn = parsed_url._replace(path=test_db_name).geturl()
        cls._is_postgres = cls.dbname == 'postgres'
        cls._is_sqlite = cls.dbname == 'sqlite'

    def setUp(self):

        super(TestBase, self).setUp()

        # Make an array of dataset numbers, so we can refer to them with a single integer
        self.dn = [str(DatasetNumber(x, x)) for x in range(1, 10)]

        self._library = None  # Will be populated if someone calls library() method.

        if self.__class__._is_postgres:
            PostgreSQLTestBase._create_postgres_test_db(test_db_dsn=self.__class__.library_test_dsn)

    def tearDown(self):

        if self._library:
            self._library.close()
            if self.__class__._is_sqlite:
                try:
                    # FIXME: Ensure you are dropping test database.
                    os.remove(self._library.database.dsn.replace('sqlite:///', ''))
                except OSError:
                    pass
        if self.__class__._is_postgres:
            PostgreSQLTestBase._drop_postgres_test_db()

    def ds_params(self, n, source='source'):
        return dict(vid=self.dn[n], source=source, dataset='dataset')

    @classmethod  # So it can be called from either setUp or setUpClass
    def get_rc(cls, rewrite=True):
        """Create a new config file for test and return the RunConfig.

        This method will start with the user's default Ambry configuration, but will replace the
        library.filesystem_root with the value of filesystem.test, then depending on the value of the AMBRY_TEST_DB
        environmental variable, it will set library.database to the DSN of either database.test-sqlite or
        database.test-postgres

        """

        from fs.opener import fsopendir

        config = ambry.run.load()  # not cached; get_config is

        orig_root = config.library.filesystem_root
        root_dir = config.filesystem.test.format(root=orig_root)

        if config.library.database == cls.library_test_dsn:
            raise Exception('production database and test database can not be the same.')

        config.library.filesystem_root = root_dir
        config.library.database = cls.library_test_dsn
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

    def dump_database(self, table, db):
        for row in db.connection.execute('SELECT * FROM {};'.format(table)):
            print(row)

    def new_database(self):
        # FIXME: this connection will not be closed properly in a postgres case.
        # FIXME: DEPRECATED. Use self.library.database instead.
        db = Database(self.__class__.library_test_dsn)
        db.open()
        return db

    def setup_bundle(self, name, source_url=None, build_url=None, library=None):
        """Configure a bundle from existing sources"""
        from test import bundles
        from os.path import dirname, join
        from fs.opener import fsopendir
        import yaml

        if not library:
            library = self.library()

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


class PostgreSQLTestBase(TestBase):
    """ Base class for database tests who requires postgresql database.

    Note:
        If postgres is not installed all tests of all subclasses will be skipped.

    """

    def setUp(self):
        super(PostgreSQLTestBase, self).setUp()

        if not self.__class__._is_postgres:
            raise unittest.SkipTest('Postgres tests are disabled.')

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
    def _create_postgres_test_db(cls, prod_db_dsn=None, test_db_dsn=None):
        assert test_db_dsn or prod_db_dsn

        if test_db_dsn:
            parsed_url = urlparse(test_db_dsn)
            postgres_user = parsed_url.username
            db_name = parsed_url.path.replace('/', '')
            test_db_name = db_name
            postgres_db_dsn = parsed_url._replace(path='postgres').geturl()
        else:
            # test db is not given, create test dsn from prod.
            parsed_url = urlparse(prod_db_dsn)
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
