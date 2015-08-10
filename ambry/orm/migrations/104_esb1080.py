# -*- coding: utf-8 -*-',

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        connection.execute('ALTER table datasets ADD COLUMN d_scov VARCHAR')
        connection.execute('ALTER table datasets ADD COLUMN d_tcov VARCHAR')
        connection.execute('ALTER table datasets ADD COLUMN d_gcov VARCHAR')

        connection.execute('ALTER table partitions ADD COLUMN p_scov VARCHAR')
        connection.execute('ALTER table partitions ADD COLUMN p_tcov VARCHAR')
        connection.execute('ALTER table partitions ADD COLUMN p_gcov VARCHAR')

    def _migrate_postgresql(self, connection):
        self._migrate_sqlite(connection)
