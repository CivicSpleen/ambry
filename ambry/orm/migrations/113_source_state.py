# -*- coding: utf-8 -*-',

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        # connection.execute('ALTER table ...')
        pass

    def _migrate_postgresql(self, connection):
        connection.execute('ALTER table ambrylib.datasources DROP COLUMN ds_stage ')
        connection.execute('ALTER table ambrylib.datasources DROP COLUMN ds_order ')
        connection.execute('ALTER table ambrylib.datasources ADD COLUMN ds_stage INTEGER')
