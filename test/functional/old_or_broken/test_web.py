# -*- coding: utf-8 -*-
from unittest import TestCase

# FIXME! These tests need both a running web UI, and to have the ambry_client installed
# neither of which is automatic yet.

# These tests require running the UI with these env vars:
# AMBRY_API_TOKEN=api_secret AMBRY_ACCOUNT_PASSWORD=secret_password


jwt_secret = 'WaeW77qy8yqJM2mP'
api_password = 'qHPaMbgGUXIUobpB2ET0'


class Test(TestCase):

    def test_basic(self):
        from ambry_client import Client

        c = Client('http://localhost:8080')
        l = c.library

        b = None
        for d in c.list():
            b = d.bundle
            print b.vname

        remotes = l.remotes

        l.remotes = remotes

        if b:
            for k, v in b.files.items():
                print k

            print b.files.schema.hash
            print b.files.build_bundle.content

    def test_auth(self):
        """

        """
        from ambry_client import Client

        c = Client('http://localhost:8080')

        print c.authenticate('api', api_password, jwt_secret)

        print c.test()

    def test_checkin(self):
        """

        """
        from ambry_client import Client

        c = Client('http://localhost:8080')
        c.authenticate('api', api_password, jwt_secret)

        c.library.checkin('/tmp/foo.txt')
