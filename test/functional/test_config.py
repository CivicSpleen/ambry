
import os

import yaml

from ambry.run import get_runconfig

from test import bundlefiles
from test.test_base import TestBase


class Test(TestBase):


    def test_run_config_filesystem(self):
        rc = self.get_rc()
        self.assertEqual('{root}/downloads', rc.filesystem.downloads)
        self.assertEqual('{root}/extracts', rc.filesystem.extracts)


    def test_dsn_config(self):
        from ambry.dbexceptions import ConfigurationError

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

        from ambry.util import temp_file_name
        import ambry.run
        from ambry.dbexceptions import ConfigurationError
        import os
        from ambry.library.filesystem import LibraryFilesystem

        tf = temp_file_name()

        with open(tf, 'w') as f:
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
            config = ambry.run.load(tf)

        if 'AMBRY_DB' in os.environ:
            del os.environ['AMBRY_DB']

        with open(tf,'w') as f:
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

        config = ambry.run.load(tf)
        config.account = None

        self.assertEquals('postgres://foo:bar@baz:5432/ambry', config.library.database)
        self.assertEquals('/tmp/foo/bar',config.library.filesystem_root)

        self.assertEqual(1, len(config.loaded))
        self.assertEqual(tf, config.loaded[0][0])

        with open(tf, 'w') as f:
            f.write("""
library:
    filesystem_root: /foo/root
            """)

        os.environ['AMBRY_DB'] = 'sqlite:////library.db'

        with open(tf, 'w') as f:
            f.write("""""")

        os.environ['AMBRY_DB'] = 'sqlite:////{root}/library.db'
        os.environ['AMBRY_ROOT'] = '/tmp/foo/bar'

        config = ambry.run.load(tf)

        lf = LibraryFilesystem(config)

        self.assertEqual('sqlite://///tmp/foo/bar/library.db', lf.database_dsn)
        self.assertEqual('/tmp/foo/bar/downloads/a/b',lf.downloads('a','b'))

    def test_library(self):
        from ambry.util import temp_file_name
        import ambry.run
        from ambry.library import Library
        from shutil import rmtree
        from ambry.library.filesystem import LibraryFilesystem

        db_path = '/tmp/foo/bar/library.db'
        import os
        if os.path.exists(db_path):
            os.remove(db_path)

        tf = temp_file_name()

        with open(tf, 'w') as f:
            f.write("""
library:
    category: development
    filesystem_root: /tmp/foo/bar
    remotes:
        census: s3://test.library.civicknowledge.com/census
        public: s3://test.library.civicknowledge.com/public
        restricted: s3://test.library.civicknowledge.com/restricted
        test: s3://test.library.civicknowledge.com/test""")

        config = ambry.run.load(tf)

        lf = LibraryFilesystem(config)

        self.assertTrue('/tmp/foo/bar', lf.root)

        l = Library(config)

        self.assertEqual(['test', 'restricted', 'census', 'public'], l.remotes.keys())


    def test_alt_session_not_reused(self):

        l = self.library()

        db = l.database
        session = l.database.session
        conn, alt_session = db.alt_session()

        self.assertNotEqual(id(session), id(alt_session))

        ds = db.root_dataset

        pc = ds.config.process
        pc.clean()
        pc.commit()

        pc.foo.bar = 'baz'

        self.assertNotIn('process.foo.bar', [c.dotted_key for c in ds.configs])

        ds.session.refresh(ds)
        self.assertNotIn('process.foo.bar', [c.dotted_key for c in ds.configs])

        pc.commit()

        self.assertNotIn('process.foo.bar', [c.dotted_key for c in ds.configs])

        ds.session.refresh(ds)
        self.assertIn('process.foo.bar', [c.dotted_key for c in ds.configs])

        pc.set_activity('foo')

        ds.session.refresh(ds)
        self.assertEqual('foo', [ c.value for c in ds.configs if c.key == 'activity' ][0])

        pc.set_activity('bar')

        ds.session.refresh(ds)
        self.assertEqual('bar', [c.value for c in ds.configs if c.key == 'activity'][0])

