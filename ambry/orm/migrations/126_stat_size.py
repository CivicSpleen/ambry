# -*- coding: utf-8 -*-'

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        connection.execute('ALTER TABLE colstats ADD COLUMN cs_width INTEGER')

    def _migrate_postgresql(self, connection):
        connection.execute('ALTER TABLE ambrylib.colstats ADD COLUMN cs_width INTEGER')
