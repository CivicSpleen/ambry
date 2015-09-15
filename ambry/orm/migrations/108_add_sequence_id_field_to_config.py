# -*- coding: utf-8 -*-',

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        connection.execute('ALTER table config ADD COLUMN co_sequence_id INTEGER NOT NULL DEFAULT 0')

    def _migrate_postgresql(self, connection):
        connection.execute('ALTER table ambrylib.config ADD COLUMN co_sequence_id INTEGER NOT NULL DEFAULT 0')
