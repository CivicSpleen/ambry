# -*- coding: utf-8 -*-'

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        connection.execute('ALTER table columns ADD COLUMN c_role VARCHAR')
        connection.execute('ALTER table columns ADD COLUMN c_scale FLOAT')

    def _migrate_postgresql(self, connection):
        connection.execute('ALTER table ambrylib.columns ADD COLUMN c_role VARCHAR(1)')
        connection.execute('ALTER table ambrylib.columns ADD COLUMN c_scale FLOAT')
