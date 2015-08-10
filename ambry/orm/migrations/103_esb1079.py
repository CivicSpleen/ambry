# -*- coding: utf-8 -*-',

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        connection.execute('ALTER table codes ADD COLUMN cd_description VARCHAR')
        connection.execute('ALTER table codes ADD COLUMN cd_source VARCHAR')
        connection.execute('ALTER table codes ADD COLUMN cd_data VARCHAR')

    def _migrate_postgresql(self, connection):
        self._migrate_sqlite(connection)
