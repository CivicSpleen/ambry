import os

from fs.opener import fsopendir

from ambry.dbexceptions import ConfigurationError
from ambry.library import Library
from ambry.library.filesystem import LibraryFilesystem
from ambry.run import load, CONFIG_FILE
from test.proto import TestBase
import unittest


class TestConfigParsing(TestBase):

    def test_run_config_filesystem(self):
        self.assertEqual('{root}/downloads', self.config.filesystem.downloads)
        self.assertEqual('{root}/extracts', self.config.filesystem.extracts)

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
        config_root = fsopendir('temp://')
        config_root.createfile(CONFIG_FILE)
        config_file_syspath = config_root.getsyspath(CONFIG_FILE)

        with open(config_file_syspath, 'w') as f:
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
            config = load(config_root.getsyspath('/'))

        if 'AMBRY_DB' in os.environ:
            del os.environ['AMBRY_DB']

        with open(config_file_syspath, 'w') as f:
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

        config = load(config_root.getsyspath('/'))
        config.account = None

        self.assertEquals('postgres://foo:bar@baz:5432/ambry', config.library.database)
        self.assertEquals('/tmp/foo/bar', config.library.filesystem_root)

        self.assertEqual(2, len(config.loaded))
        self.assertEqual(config_file_syspath, config.loaded[0])

        with open(config_file_syspath, 'w') as f:
            f.write("""
library:
    filesystem_root: /foo/root
            """)

        os.environ['AMBRY_DB'] = 'sqlite:////library.db'

        with open(config_file_syspath, 'w') as f:
            f.write("""""")

        os.environ['AMBRY_DB'] = 'sqlite:////{root}/library.db'
        os.environ['AMBRY_ROOT'] = '/tmp/foo/bar'

        config = load(config_root.getsyspath('/'))

        lf = LibraryFilesystem(config)

        self.assertEqual('sqlite://///tmp/foo/bar/library.db', lf.database_dsn)
        self.assertEqual('/tmp/foo/bar/downloads/a/b', lf.downloads('a', 'b'))

    def test_library(self):

        db_path = '/tmp/foo/bar/library.db'
        if os.path.exists(db_path):
            os.remove(db_path)

        config_root = fsopendir('temp://')
        config_root.createfile(CONFIG_FILE)
        config_file_syspath = config_root.getsyspath(CONFIG_FILE)

        with open(config_file_syspath, 'w') as f:
            f.write("""
library:
    category: development
    filesystem_root: /tmp/foo/bar
    remotes:
        census: s3://test.library.civicknowledge.com/census
        public: s3://test.library.civicknowledge.com/public
        restricted: s3://test.library.civicknowledge.com/restricted
        test: s3://test.library.civicknowledge.com/test""")

        config = load(config_root.getsyspath('/'))

        lf = LibraryFilesystem(config)

        self.assertTrue('/tmp/foo/bar', lf.root)

        l = Library(config)
        l.sync_config()

        self.assertEqual(
            sorted(['test', 'restricted', 'census', 'public']),
            sorted([x.short_name for x in l.remotes]))

from test.proto import TestBase as TestBaseProto


class TestConfigFiles(unittest.TestCase):


    def _test_config_dirs(self, files_dir):
        import test
        from os.path import dirname, join
        import ambry.run

        test_config_dir = join(dirname(test.__file__), 'test_config', files_dir)

        config = ambry.run.load(test_config_dir, load_user=False)

        print config.dump()

        self.assertEqual(1, sum(1 for v in config.accounts.values() if isinstance(v,dict) and 'service' in v))

        self.assertEqual(5, sum(1 for v in config.remotes.values() if 'url' in v))

        for r in ('config1', 'config2', 'remotes1', 'remotes2', 'remotes3'):
            self.assertIn(r, config.remotes)

        self.assertEqual('34765JHTEp13/g8vcK+EIcAj7KJKJHGKKJWEHFW', config.remotes.config1.secret)
        self.assertEqual('34765JHTEp13/g8vcK+EIcAj7KJKJHGKKJWEHFW', config.remotes.remotes1.secret)

        # for r in config.flatten():
        #    print r

    def test_multiple_files(self):
        import test
        from os.path import dirname, join
        import ambry.run

        test_config_dir =  join(dirname(test.__file__), 'test_config','separate_files')

        self.assertEquals(join(test_config_dir,ambry.run.ACCOUNTS_FILE),
                          ambry.run.find_config_file(ambry.run.ACCOUNTS_FILE, test_config_dir))

        self._test_config_dirs('separate_files')

        self._test_config_dirs('single_file')

    def test_remotes_accounts(self):
        import test
        from os.path import dirname, join, exists
        from os import makedirs
        import ambry.run
        from ambry.library import Library

        test_config_dir = join(dirname(test.__file__), 'test_config', 'separate_files')

        config = ambry.run.load(test_config_dir, load_user=False)

        if not exists(config.library.filesystem_root):
            makedirs(config.library.filesystem_root)

        l = Library(config)
        l.sync_config(force=True)

        for r in l.remotes:
            print r.short_name, r.access, r.secret

class MetadataTest(TestBaseProto):

    def test_dump_metadata(self):
        from ambry.util import AttrDict

        l = self.library()

        b = l.bundle('build.example.com-casters')

        v = 'Packaged for [Ambry](http://ambry.io) by {{contact_bundle.creator.org}}'

        self.assertEqual(v, AttrDict(b.metadata.about.items()).processed)
        self.assertEqual(v, b.metadata.about.processed)
        self.assertEqual(v, b.build_source_files.bundle_meta.record.unpacked_contents['about']['processed'])
        self.assertEqual(v, b.build_source_files.bundle_meta.get_object().about.processed)

        b.metadata.about.processed = 'foobar'
        b.commit()

        self.assertEqual('foobar', b.metadata.about.processed)

        self.assertNotEqual(b.metadata.about.processed,
                            b.build_source_files.bundle_meta.get_object().about.processed)


        b.build_source_files.bundle_meta.objects_to_record()

        self.assertEqual(b.metadata.about.processed,
                            b.build_source_files.bundle_meta.get_object().about.processed)


