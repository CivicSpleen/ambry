# -*- coding: utf-8 -*-'

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        from sqlalchemy.exc import OperationalError
        try:
            connection.execute('ALTER table remote ADD COLUMN rm_virtual_host VARCHAR')
        except OperationalError:
            # Some of the bundles don't have the remotes table, but that's OK.
            pass


    def _migrate_postgresql(self, connection):
        connection.execute('ALTER table ambrylib.remote ADD COLUMN rm_virtual_host VARCHAR')
        pass