""" Functions for loading configuration into the library.
"""

# Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

from ambry.orm import Account
from ambry.dbexceptions import ConfigurationError

class LibraryConfigSyncProxy(object):

    def __init__(self, library):
        self.library = library
        self.config = library.config
        self.database = self.library.database

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

    def sync(self):
        import time
        import platform

        change_time = self.config.modtime

        load_time = self.database.root_dataset.config.library.config['load_time']

        self.library._account_password = self.password

        node = self.database.root_dataset.config.library.config['config_node']

        if change_time > load_time:

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

    def sync_remotes(self, remotes):
        root = self.database.root_dataset
        rc = root.config.library.remotes

        self.commit()

        for name, url in remotes.items():
            rc[name] = url

        self.commit()

    def sync_accounts(self, accounts_data):
        """
        Load all of the accounts from the account section of the config
        into the database.

        :param accounts_data:
        :param password:
        :return:
        """

        kmap = {
            'service': 'major_type',
            'host': 'url',
            'organization': 'org',
            'apikey': 'secret',

            'access': 'access_key',
            'access_key': 'access_key',
            'secret': 'secret',
            'name': 'name',
            'org': 'org',
            'url': 'url',
            'email': 'email'
        }


        all_accounts = self.accounts

        for account_id, values in accounts_data.items():

            if not isinstance(values, dict):
                continue

            d = {}
            a = Account(account_id=account_id)
            a.password = self.password

            for k, v in values.items():
                try:
                    setattr(a, kmap[k], v)
                except KeyError:
                    d[k] = v

            a.data = d

            if values.get('service') == 's3':
                a.url = 's3://{}'.format(a.account_id)

            if a.account_id in all_accounts:
                a.id = all_accounts[a.account_id]['id']
                self.database.session.merge(a)

            else:
                self.database.session.add(a)

        self.database.session.commit()
