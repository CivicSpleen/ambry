# -*- coding: utf-8 -*-'

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        connection.execute('ALTER table partitions ADD COLUMN p_title VARCHAR')
        connection.execute('ALTER table partitions ADD COLUMN p_description VARCHAR')
        connection.execute('ALTER table partitions ADD COLUMN p_notes VARCHAR')
        
        
    def _migrate_postgresql(self, connection):
        connection.execute('ALTER table ambrylib.partitions ADD COLUMN p_title VARCHAR')
        connection.execute('ALTER table ambrylib.partitions ADD COLUMN p_description VARCHAR')
        connection.execute('ALTER table ambrylib.partitions ADD COLUMN p_notes VARCHAR')
