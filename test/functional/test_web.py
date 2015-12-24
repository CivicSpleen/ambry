# -*- coding: utf-8 -*-
import os

from test.test_base import TestBase

# FIXME! These tests need both a running web UI, and to have the ambry_client installed
# neither of which is automatic yet.


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