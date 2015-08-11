# -*- coding: utf-8 -*-',

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        # connection.execute('ALTER table ...')
        connection.execute('ALTER table config ADD COLUMN co_id VARCHAR')

    def _migrate_postgresql(self, connection):
        self._migrate_sqlite(connection)
