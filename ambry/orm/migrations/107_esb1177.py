# -*- coding: utf-8 -*-',

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        connection.execute('ALTER table colstats ADD COLUMN cs_text_hist VARCHAR')
        pass

    def _migrate_postgresql(self, connection):
        connection.execute('ALTER table ambrylib.colstats ADD COLUMN cs_text_hist VARCHAR')
