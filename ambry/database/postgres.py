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

