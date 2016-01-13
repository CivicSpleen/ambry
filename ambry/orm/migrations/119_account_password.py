# -*- coding: utf-8 -*-'

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        connection.execute('ALTER TABLE accounts ADD COLUMN ac_password VARCHAR')
        

    def _migrate_postgresql(self, connection):
        connection.execute('ALTER TABLE ambrylib.accounts ADD COLUMN ac_password VARCHAR')
        