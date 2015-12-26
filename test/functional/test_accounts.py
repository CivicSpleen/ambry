# -*- coding: utf-8 -*-
import os

from ambry.library import new_library
from ambry.library.config import LibraryConfigSyncProxy
from ambry.run import get_runconfig

from test.test_base import TestBase


class Test(TestBase):

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

            self.assertTrue(bool(act.secret))
            self.assertTrue(bool(act.account_id))

        for k, v in l.remotes.items():
            self.assertTrue(bool(k))
            self.assertTrue(bool(v))

        # Delete the config and get the library again, this time from the
        # library DSN.
        rc = self.config()
        # print 'Removing', rc.loaded[0][0]
        os.remove(rc.loaded[0][0])

        self.assertFalse(os.path.exists(rc.loaded[0][0]))
        get_runconfig.clear()  # Clear the LRU cache on the function

        os.environ['AMBRY_DB'] = l.database.dsn
        os.environ['AMBRY_ACCOUNT_PASSWORD'] = l._account_password

        self.assertEqual(l.database.dsn, os.getenv('AMBRY_DB'))

        l = new_library()

        for k, v in l.accounts.items():
            act = l.account(k)
            if k in ('ambry', 'google_spreadsheets',):
                continue
            self.assertTrue(bool(act.secret))
            self.assertTrue(bool(act.account_id))

        for k, v in l.remotes.items():
            self.assertTrue(bool(k))
            self.assertTrue(bool(v))
