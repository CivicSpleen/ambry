# -*- coding: utf-8 -*-',

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        # connection.execute('ALTER table ...')
        connection.execute('ALTER table columns ADD COLUMN c_valuetype VARCHAR')

    def _migrate_postgresql(self, connection):
        connection.execute('ALTER table ambrylib.columns ADD COLUMN c_valuetype VARCHAR')
