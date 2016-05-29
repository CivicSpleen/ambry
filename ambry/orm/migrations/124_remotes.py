# -*- coding: utf-8 -*-'

from ambry.orm.database import BaseMigration

# This will fail when upgrading database packages ( Sqlite files ) that never got a remote table.
# THat's OK, because the remotes table isn't used and isn't copied over

class Migration(BaseMigration):



    def _migrate_sqlite(self, connection):
        from sqlalchemy.exc import OperationalError
        try:
            connection.execute('ALTER table remotes ADD COLUMN rm_access VARCHAR')
            connection.execute('ALTER table remotes ADD COLUMN rm_secret VARCHAR')
        except OperationalError:
            pass

    def _migrate_postgresql(self, connection):
        from sqlalchemy.exc import OperationalError
        try:
            connection.execute('ALTER table ambrylib.remotes ADD COLUMN rm_access VARCHAR')
            connection.execute('ALTER table ambrylib.remotes ADD COLUMN rm_secret VARCHAR')
        except OperationalError:
            pass


