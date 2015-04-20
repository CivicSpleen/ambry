"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from relational import RelationalDatabase


class PostgresDatabase(RelationalDatabase):

    @property
    def munged_dsn(self):
        return self.dsn.replace('postgres:', 'postgresql+psycopg2:')

    def _create(self):
        """Create the database from the base SQL"""
        from ambry.orm import Config

        if not self.exists():

            tables = [Config]

            for table in tables:
                table.__table__.create(bind=self.engine)

            return True  # signal did create

        return False  # signal didn't create

    def clean(self):
        self.drop()
        self.create()

    def drop(self):
        """Uses DROP ... CASCADE to drop tables"""

        if not self.enable_delete:
            raise Exception("Deleting not enabled")

        # sorted by foreign key dependency
        for table in reversed(self.metadata.sorted_tables):

            # Leave spatial tables alone.
            if table.name not in ['spatial_ref_sys']:
                sql = 'DROP TABLE IF EXISTS  "{}" CASCADE'.format(table.name)

                self.connection.execute(sql)

    def index_for_search(self, vid, topic, keywords):
        """
        Add a search document to the full-text search index.

        :param vid: Versioned ID for the object. Should be a dataset, partition table or column
        :param topic: A text document or description.
        :param keywords: A list of keywords
        :return:
        """

        # See http://blog.lostpropertyhq.com/postgres-full-text-search-is-good-enough/
        # We probably want to create materialized view.

    def search(self, topic, keywords):
        """

        Search the full text search index.

        :param topic:
        :param keywords:
        :return"""
