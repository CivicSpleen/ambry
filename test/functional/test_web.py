# -*- coding: utf-8 -*-
import os

from test.test_base import TestBase

# FIXME! These tests need both a running web UI, and to have the ambry_client installed
# neither of which is automatic yet.

# These tests require running the UI with these env vars:
# AMBRY_API_TOKEN=7bd79f83-78c5-4bb3-b93a-dd3057847dfc AMBRY_ACCOUNT_PASSWORD=secret_password


api_token = '7bd79f83-78c5-4bb3-b93a-dd3057847dfc'
secret_password='secret_password'

class Test(TestBase):

    def test_basic(self):
        from ambry_client import Client
        import time

        c = Client('http://localhost:8080')
        l = c.library()

        for d in c.list():
            b = d.bundle
            print b.vname

        remotes = l.remotes

        remotes['time'] = time.time()

        l.remotes = remotes

        print remotes

        for k, v in  b.files.items():
            print k

        print b.files.schema.hash
        print b.files.build_bundle.content

    def test_auth(self):
        """

        """
        from ambry.orm.account import Account
        from ambry_client import Client
        import time
        import yaml

        c = Client('http://localhost:8080')
        c.auth('api',api_token)

        a = Account(account_id='user1', major_type='ambry')
        a.secret_password = secret_password
        a.encrypt_secret('foobar')

        accounts = { 'user1': a.dict }

        c.library.accounts = accounts

        c = Client('http://localhost:8080')
        c.auth('user1', 'foobar')

        self.assertIn('user1', c.library.accounts.keys())

        for k, v in c.library.accounts.items():
            print k, v

    def test_checkin(self):
        """

        """
        from ambry.orm.account import Account
        from ambry_client import Client
        import time
        import yaml

        c = Client('http://localhost:8080')
        c.auth('api', api_token)

        c.library.checkin('/tmp/foo.txt')
