# -*- coding: utf-8 -*-'

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):
    def _migrate_sqlite(self, connection):
        connection.execute('ALTER table remote ADD COLUMN rm_access VARCHAR')
        connection.execute('ALTER table remote ADD COLUMN rm_secret VARCHAR')

    def _migrate_postgresql(self, connection):
        connection.execute('ALTER table ambrylib.remote ADD COLUMN rm_access VARCHAR')
        connection.execute('ALTER table ambrylib.remote ADD COLUMN rm_secret VARCHAR')

