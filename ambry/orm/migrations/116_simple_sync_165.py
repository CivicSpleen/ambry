# -*- coding: utf-8 -*-'

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        connection.execute('ALTER TABLE files ADD COLUMN f_synced_fs REAL ')
        
    def _migrate_postgresql(self, connection):
        connection.execute('ALTER TABLE ambrylib.files ADD COLUMN f_synced_fs REAL ')
