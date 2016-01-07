""" Functions for loading configuration into the library.
"""

# Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

from ambry.orm import Account
from ambry.dbexceptions import ConfigurationError

class LibraryConfigSyncProxy(object):

    def __init__(self, library, password=None):
        self.library = library
        self.config = library.config
        self.database = self.library.database

        if password:
            self.password = password
        else:
            try:
                self.password = self.config.accounts.password
            except AttributeError:
                self.password = None


        self.root_dir = None # Set when file systems are synced

        if not self.password:
            import os
            self.password = os.getenv("AMBRY_ACCOUNT_PASSWORD")

    def commit(self):
        return self.library.commit()

    @property
    def accounts(self):
        return self.library.accounts

    def sync(self, force=False):
        import time
        import platform

        change_time = self.config.modtime

        load_time = self.database.root_dataset.config.library.config['load_time']

        self.library._account_password = self.password

        node = self.database.root_dataset.config.library.config['config_node']

        if force or change_time > load_time:
            self.sync_accounts(self.config.accounts)
            self.sync_remotes(self.config.library.remotes)

            if self.config.get('services'):
                self.sync_services(self.config.services)

            self.database.root_dataset.config.library.config['load_time'] = int(time.time())
            self.database.root_dataset.config.library.config['config_node'] = platform.node()
            self.commit()

    def sync_services(self, services):
        root = self.database.root_dataset
        rc = root.config.library.services

        self.commit()

        for name, v in services.items():
            rc[name] = v

        self.commit()

    def sync_remotes(self, remotes, cb=None):
        from ambry.orm.exc import NotFoundError
        from ambry.orm import Remote

        root = self.database.root_dataset

        rc = root.config.library.remotes

        s = self.library.database.session

        for name, r in remotes.items():

            if not isinstance(r, dict):
                r = dict(url=r)

            remote = self.library.find_or_new_remote(name)

            for k, v in r.items():
                setattr(remote, k, v)

            if cb:
                cb("Loaded remote: {}".format(remote.short_name))

        self.commit()


    def sync_accounts(self, accounts_data, clear = False, password=None, cb = None):
        """
        Load all of the accounts from the account section of the config
        into the database.

        :param accounts_data:
        :param password:
        :return:
        """

        # Map common values into the accounts records


        all_accounts = self.accounts

        kmap = Account.prop_map()

        for account_id, values in accounts_data.items():

            if not isinstance(values, dict):
                continue

            d = {}

            a = self.library.find_or_new_account(account_id)
            a.secret_password = password or self.password


            for k, v in values.items():
                try:
                    if kmap[k] == 'secret':
                        a.encrypt_secret(v)
                    else:
                        setattr(a, kmap[k], v)
                except KeyError:
                    d[k] = v

            a.data = d

            if values.get('service') == 's3':
                a.url = 's3://{}'.format(a.account_id)

            if cb:
                cb('Loaded account: {}'.format(a.account_id))

        self.database.session.commit()
