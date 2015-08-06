from test.test_base import TestBase

class Test(TestBase):

    def get_rc(self, name='ambry.yaml'):
        from ambry.run import get_runconfig
        import os
        from test import bundlefiles
        import yaml

        def bf_dir(fn):
            return os.path.join(os.path.dirname(bundlefiles.__file__), fn)

        rc =  get_runconfig(bf_dir(name))

        # RunConfig makes it hard to change where the accounts data comes from, which might actually
        # be good, but it makes testing hard.

        return rc

    def test_run_config_filesystem(self):

        self.rc = self.get_rc()

        self.assertEquals('/tmp/test/downloads', self.rc.filesystem('downloads'))
        self.assertEquals('/tmp/test/extracts', self.rc.filesystem('extracts'))

    def test_run_config_library(self):

        self.rc = self.get_rc()

        print self.rc.library()

    def test_database(self):
        import os
        from test import bundlefiles
        import yaml

        rc = self.get_rc()

        def bf_dir(fn):
            return os.path.join(os.path.dirname(bundlefiles.__file__), fn)

        with open(os.path.join(bf_dir('ambry-accounts.yaml'))) as f:
            rc.accounts = yaml.load(f)['accounts']

        # See the ambry.yaml and ambry-accounts.yaml files in test/bundlefiles

        self.assertEquals({'username': None, 'password': None, 'driver': 'sqlite', 'dbname': '/foo/bar', 'server': None},
                          rc.database('database1'))

        self.assertEquals({'username': 'user', 'password': 'pass', 'driver': 'postgres', 'dbname': 'dbname',
                           'server': 'host'},
                          rc.database('database2'))

        self.assertEquals(rc.database('database2'), rc.database('database3'))

        self.assertEquals('creduser1', rc.database('database4')['user'])
        self.assertEquals('credpass1', rc.database('database4')['password'])

        self.assertEquals({'username': 'user2', '_name': 'host2-user2-dbname', 'password': 'credpass2',
                           'driver': 'postgres', 'dbname': 'dbname', 'server': 'host2'},
                          rc.database('database5'))

    def test_dsn_config(self):
        from ambry.dbexceptions import ConfigurationError

        from ambry.run import normalize_dsn_or_dict as n

        self.assertEquals('sqlite://', n(dict(driver='sqlite', dbname=''))[1])
        self.assertEquals('sqlite:///foo', n(dict(driver='sqlite',dbname='foo'))[1])
        self.assertEquals('sqlite:////foo', n(dict(driver='sqlite', dbname='/foo'))[1])

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

        basic_checks(('postgres://host1/dbname','postgres://user@host1/dbname','postgres://user:pass@host1/dbname',
                     'postgres:///dbname'))

        with self.assertRaises(ConfigurationError):
            n('sqlite3:///')

        with self.assertRaises(ConfigurationError):
            n('sqlite3://foobar')

