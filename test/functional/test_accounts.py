# -*- coding: utf-8 -*-
import os

from ambry.library import new_library
from ambry.library.config import LibraryConfigSyncProxy
from ambry.orm.account import Account

from test.proto import TestBase


class Test(TestBase):

    def test_encryption(self):

        a = Account(major_type='ambry')
        a.secret_password = 'secret'
        a.encrypt_secret('foobar')
        self.assertIsNotNone(a.decrypt_secret())

        a.encrypt_password('foobaz')
        self.assertTrue(a.test('foobaz'))
        self.assertFalse(a.test('baz'))

    def test_accounts(self):
        """ Tests library, database and environment accounts. """
        l = self.library()
        l.drop()
        l.create()

        lcsp = LibraryConfigSyncProxy(l)
        lcsp.sync()

        # db = self.library().database
        # for v in l.database.root_dataset.config.library:
        #     print v

        l.filesystem.downloads('foo', 'bar')
        l.filesystem.build('foo', 'bar')

        for k, v in l.accounts.items():
            act = l.account(k)
            if k in ('ambry', 'google_spreadsheets',):
                continue

            if act.major_type != 'ambry':
                self.assertTrue(bool(act.decrypt_secret()))
            self.assertTrue(bool(act.account_id))

        for remote in l.remotes:
            self.assertTrue(bool(remote.url))

        os.environ['AMBRY_DB'] = l.database.dsn = 'sqlite://'
        os.environ['AMBRY_ACCOUNT_PASSWORD'] = l._account_password

        self.assertEqual(l.database.dsn, os.getenv('AMBRY_DB'))

        l = new_library()
        try:
            for k, v in l.accounts.items():
                act = l.account(k)
                if k in ('ambry', 'google_spreadsheets',):
                    continue

                if act.major_type != 'ambry':
                    self.assertTrue(bool(act.decrypt_secret()))
                self.assertTrue(bool(act.account_id))

            for remote in l.remotes:
                self.assertTrue(bool(remote.url))
        finally:
            l.close()
