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

        with open(os.path.join(bf_dir('ambry-accounts.yaml'))) as f:
            rc.accounts = yaml.load(f)['accounts']

        return rc

    def test_run_config_filesystem(self):

        self.rc = self.get_rc()

        self.assertEquals('/tmp/test/downloads', self.rc.filesystem('downloads'))
        self.assertEquals('/tmp/test/extracts', self.rc.filesystem('extracts'))

    def test_run_config_library(self):

        self.rc = self.get_rc()

        print self.rc.library()

    def test_database(self):

        rc = self.get_rc()

        # See the ambry.yaml and ambry-accounts.yaml files in test/bundlefiles

        self.assertEquals({'username': None, 'password': None, 'driver': 'sqlite', 'dbname': 'foo/bar', 'server': None},
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
