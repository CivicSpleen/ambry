import os

from ambry.dbexceptions import ConfigurationError
from ambry.library import Library
from ambry.library.filesystem import LibraryFilesystem
import ambry.run
from ambry.util import temp_file_name

from test.test_base import TestBase


class TempDirContext(object):
    """Setup and tear down a chroot jail. """

    def __init__(self):
        from tempfile import mkdtemp

        self.tmp_dir = mkdtemp()

    def __enter__(self):
        return self.tmp_dir

    def __exit__(self, type, value, traceback):
        from shutil import rmtree
        rmtree(self.tmp_dir)


class Test(TestBase):

    def tearDown(self):
        super(self.__class__, self).tearDown()
        if hasattr(self, '_temp_file'):
            os.remove(self._temp_file)

    def test_run_config_filesystem(self):
        rc = self.get_rc()
        self.assertEqual('{root}/downloads', rc.filesystem.downloads)
        self.assertEqual('{root}/extracts', rc.filesystem.extracts)

    def test_dsn_config(self):
        from ambry.run import normalize_dsn_or_dict as n

        self.assertEqual('sqlite://', n(dict(driver='sqlite', dbname=''))[1])
        self.assertEqual('sqlite:///foo', n(dict(driver='sqlite', dbname='foo'))[1])
        self.assertEqual('sqlite:////foo', n(dict(driver='sqlite', dbname='/foo'))[1])

        def basic_checks(dsn_list):
            # Check the dsns are idempotent
            for dsn_in in dsn_list:
                config, dsn = n(dsn_in)
                config2, dsn2 = n(dsn)
                self.assertEqual(dsn_in, dsn)
                self.assertEqual(dsn_in, dsn2)

            # Check the configs are idempotent
            for dsn_in in dsn_list:
                config1, dsn1 = n(dsn_in)
                config2, dsn2 = n(config1)
                config3, dsn3 = n(config2)

                self.assertEqual(config1, config2)
                self.assertEqual(config1, config3)
                self.assertEqual(dsn_in, dsn1)
                self.assertEqual(dsn_in, dsn2)
                self.assertEqual(dsn_in, dsn3)

        basic_checks(('sqlite3://', 'sqlite3:///foo', 'sqlite3:////foo'))

        basic_checks(
            ('postgres://host1/dbname', 'postgres://user@host1/dbname', 'postgres://user:pass@host1/dbname',
             'postgres:///dbname'))

        with self.assertRaises(ConfigurationError):
            n('sqlite3:///')

        with self.assertRaises(ConfigurationError):
            n('sqlite3://foobar')

    def test_basic_config(self):

        self._temp_file = temp_file_name()

        with open(self._temp_file, 'w') as f:
            f.write("""
library:
    category: development
    remotes:
        census: s3://test.library.civicknowledge.com/census
        public: s3://test.library.civicknowledge.com/public
        restricted: s3://test.library.civicknowledge.com/restricted
        test: s3://test.library.civicknowledge.com/test
                    """)

        with self.assertRaises(ConfigurationError):
            config = ambry.run.load(self._temp_file)

        if 'AMBRY_DB' in os.environ:
            del os.environ['AMBRY_DB']

        with open(self._temp_file, 'w') as f:
            f.write("""
library:
    category: development
    filesystem_root: /tmp/foo/bar
    database: postgres://foo:bar@baz:5432/ambry
    remotes:
        census: s3://test.library.civicknowledge.com/census
        public: s3://test.library.civicknowledge.com/public
        restricted: s3://test.library.civicknowledge.com/restricted
        test: s3://test.library.civicknowledge.com/test
            """)

        config = ambry.run.load(self._temp_file)
        config.account = None

        self.assertEquals('postgres://foo:bar@baz:5432/ambry', config.library.database)
        self.assertEquals('/tmp/foo/bar', config.library.filesystem_root)

        self.assertEqual(2, len(config.loaded))
        self.assertEqual(self._temp_file, config.loaded[0])

        with open(self._temp_file, 'w') as f:
            f.write("""
library:
    filesystem_root: /foo/root
            """)

        os.environ['AMBRY_DB'] = 'sqlite:////library.db'

        with open(self._temp_file, 'w') as f:
            f.write("""""")

        os.environ['AMBRY_DB'] = 'sqlite:////{root}/library.db'
        os.environ['AMBRY_ROOT'] = '/tmp/foo/bar'

        config = ambry.run.load(self._temp_file)

        lf = LibraryFilesystem(config)

        self.assertEqual('sqlite://///tmp/foo/bar/library.db', lf.database_dsn)
        self.assertEqual('/tmp/foo/bar/downloads/a/b', lf.downloads('a', 'b'))

    def test_library(self):

        db_path = '/tmp/foo/bar/library.db'
        if os.path.exists(db_path):
            os.remove(db_path)

        # Initialize as instance field to allow tearDown to delete file after test.
        self._temp_file = temp_file_name()

        with open(self._temp_file, 'w') as f:
            f.write("""
library:
    category: development
    filesystem_root: /tmp/foo/bar
    remotes:
        census: s3://test.library.civicknowledge.com/census
        public: s3://test.library.civicknowledge.com/public
        restricted: s3://test.library.civicknowledge.com/restricted
        test: s3://test.library.civicknowledge.com/test""")

        config = ambry.run.load(self._temp_file)

        lf = LibraryFilesystem(config)

        self.assertTrue('/tmp/foo/bar', lf.root)

        l = Library(config)
        l.sync_config()

        self.assertEqual(
            sorted(['test', 'restricted', 'census', 'public']),
            sorted([x.short_name for x in l.remotes]))
