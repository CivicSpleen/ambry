# -*- coding: utf-8 -*-'

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):
    def _migrate_sqlite(self, connection):
        connection.execute('ALTER TABLE datasets ADD COLUMN d_upstream VARCHAR')

    def _migrate_postgresql(self, connection):
        connection.execute('ALTER TABLE ambrylib.datasets ADD COLUMN d_upstream VARCHAR')