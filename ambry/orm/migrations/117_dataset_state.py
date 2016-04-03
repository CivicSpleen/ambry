# -*- coding: utf-8 -*-'

from ambry.orm.database import BaseMigration

class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        connection.execute('ALTER table datasets ADD COLUMN d_state VARCHAR')
        pass

    def _migrate_postgresql(self, connection):
        connection.execute('ALTER table ambrylib.datasets ADD COLUMN d_state VARCHAR')
        pass