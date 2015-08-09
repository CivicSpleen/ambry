# -*- coding: utf-8 -*-',

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        connection.execute('ALTER table datasources ADD COLUMN ds_stage VARCHAR')

    def _migrate_postgresql(self, connection):
        connection.execute('ALTER table datasources DROP COLUMN ds_phase VARCHAR')
        connection.execute('ALTER table datasources ADD COLUMN ds_stage VARCHAR')
