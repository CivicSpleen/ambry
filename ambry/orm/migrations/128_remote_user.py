# -*- coding: utf-8 -*-'

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        connection.execute('ALTER TABLE remote ADD COLUMN rm_username TEXT')

    def _migrate_postgresql(self, connection):
        connection.execute('ALTER TABLE ambrylib.remote ADD COLUMN rm_username TEXT')
