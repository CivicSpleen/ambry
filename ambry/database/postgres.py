"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from relational import RelationalDatabase



class PostgresDatabase(RelationalDatabase):

    def _create(self):
        """Create the database from the base SQL"""
        from ambry.orm import Config

        if not self.exists():

            tables = [Config]

            for table in tables:
                table.__table__.create(bind=self.engine)

            return True  #signal did create

        return False  # signal didn't create


    def clean(self):
        self.drop()
        self.create()

    def drop(self):
        """Uses DROP ... CASCADE to drop tables"""

        if not self.enable_delete:
            raise Exception("Deleting not enabled")

        for table in reversed(self.metadata.sorted_tables):  # sorted by foreign key dependency

            if table.name not in ['spatial_ref_sys']: # Leave spatial tables alone.
                sql = 'DROP TABLE IF EXISTS  "{}" CASCADE'.format(table.name)

                self.connection.execute(sql)

