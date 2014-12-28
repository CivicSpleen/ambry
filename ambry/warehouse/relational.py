"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..dbexceptions import DependencyError
from . import Warehouse

class RelationalWarehouse(Warehouse):

    ##
    ## Tables
    ##

    def has_table(self, table_name):

        return table_name in self.database.inspector.get_table_names()


    def table_meta(self, identity, table_name):
        '''Get the metadata directly from the database. This requires that
        table_name be the same as the table as it is in stalled in the database'''
        from ..schema import Schema

        assert identity.is_partition

        p_vid = self._to_vid(identity)
        d_vid = self._partition_to_dataset_vid(identity)

        meta, table = Schema.get_table_meta_from_db(self.library.database,
                                                    table_name,
                                                    d_vid=d_vid,
                                                    driver=self.database.driver,
                                                    use_fq_names = True,
                                                    alt_name=self.augmented_table_name(identity, table_name)[0],
                                                    session=self.library.database.session)
        return meta, table

    def create_table(self, partition, table_name):
        '''Create the table in the warehouse, using an augmented table name '''
        from ..schema import Schema

        meta, table = self.table_meta(partition.identity, table_name)

        if not self.has_table(table.name):
            table.create(bind=self.database.engine)
            self.logger.info('create_table {}'.format(table.name))
        else:
            self.logger.info('table_exists {}'.format(table.name))

        return table, meta

    def create_index(self, name, table, columns):

        from sqlalchemy.exc import OperationalError, ProgrammingError

        sql = 'CREATE INDEX {} ON "{}" ({})'.format(name, table, ','.join(columns))

        try:
            self.database.connection.execute(sql)
            self.logger.info('create_index {}'.format(name))
        except (OperationalError, ProgrammingError) as e:

            if 'exists' not in str(e).lower():
                raise

            self.logger.info('index_exists {}'.format(name))
            # Ignore if it already exists.

    def table(self, table_name):
        '''Get table metadata from the database'''
        from sqlalchemy import Table

        table = self._table_meta_cache.get(table_name, False)

        if table is not False:
            r = table
        else:
            metadata = self.metadata # FIXME Will probably fail ..
            table = Table(table_name, metadata, autoload=True)
            self._table_meta_cache[table_name] = table
            r = table

        return r

    def load_insert(self, partition, source_table_name, dest_table_name, where = None):
        from ..database.inserter import ValueInserter
        from sqlalchemy import Table, MetaData
        from sqlalchemy.dialects.postgresql.base import BYTEA
        import psycopg2

        replace = False

        self.logger.info('load_insert {}'.format(partition.identity.vname))

        if self.database.driver == 'mysql':
            cache_size = 5000

        elif self.database.driver == 'postgres' or self.database.driver == 'postgis':
            cache_size = 5000

        else:
            cache_size = 50000

        self.logger.info('populate_table {}'.format(source_table_name))

        dest_metadata = MetaData()
        dest_table = Table(dest_table_name, dest_metadata, autoload=True, autoload_with=self.database.engine)


        insert_statement = dest_table.insert()

        source_metadata = MetaData()
        source_table = Table(source_table_name, source_metadata, autoload=True, autoload_with=partition.database.engine)

        select_statement = source_table.select()

        if replace:
            insert_statement = insert_statement.prefix_with('OR REPLACE')

        if where:
            select_statement += " WHERE "+where

        binary_cols = []
        for c in dest_table.columns:
            if isinstance(c.type, BYTEA ):
                binary_cols.append(c.name)


        # Psycopg executemany function doesn't use the multiple insert syntax of Postgres,
        # so it is fantastically slow. So, we have to do it ourselves.
        # Using multiple row inserts is more than 100 times faster.
        import re

        # For Psycopg's mogrify(), we need %(var)s parameters, not :var
        insert_statement = re.sub(r':([\w_-]+)', r'%(\1)s', str(insert_statement))

        conn = self.database.engine.raw_connection()

        with conn.cursor() as cur:

            def execute_many(insert_statement, values):

                mogd_values = []

                inst, vals = insert_statement.split("VALUES")

                for value in values:

                    mogd = cur.mogrify(insert_statement, value)
                    # Hopefully, including the parens will make it unique enough to not
                    # cause problems. Using just 'VALUES' files when there is a column of the same name.
                    _, vals = mogd.split(") VALUES (", 1)

                    mogd_values.append("("+vals)

                sql = inst+" VALUES "+','.join(mogd_values)


                cur.execute(sql)

            cache = []

            for i, row in enumerate(partition.database.session.execute(select_statement)):
                self.logger.progress('add_row', source_table_name, i)


                if binary_cols:
                    # This is really horrible. To insert a binary column property, it has to be run rhough
                    # function.
                    cache.append({ k: psycopg2.Binary(v) if k in binary_cols else v for k,v in row.items() })

                else:
                    cache.append(dict(row))

                if len(cache) >= cache_size:
                    self.logger.info('committing {} rows'.format(len(cache)))
                    execute_many(insert_statement, cache)
                    cache = []


            if len(cache):
                self.logger.info('committing {} rows'.format(len(cache)))
                execute_many(insert_statement, cache)

        conn.commit()

        self.logger.info('done {}'.format(partition.identity.vname))

        return dest_table_name


    def remove(self, name):
        from ..orm import Dataset
        from ..bundle import LibraryDbBundle
        from ..identity import PartitionNameQuery
        from sqlalchemy.exc import NoSuchTableError, ProgrammingError

        dataset = self.wlibrary.resolve(name)

        if dataset.partition:
            b = LibraryDbBundle(self.library.database, dataset.vid)
            p = b.partitions.find(id_=dataset.partition.vid)
            self.logger.info("Dropping tables in partition {}".format(p.identity.vname))
            for table_name in p.tables:  # Table name without the id prefix

                table_name, alias = self.augmented_table_name(p.identity, table_name)

                try:
                    self.database.drop_table(table_name)
                    self.logger.info("Dropped table: {}".format(table_name))

                except NoSuchTableError:
                    self.logger.info("Table does not exist (a): {}".format(table_name))

                except ProgrammingError:
                    self.logger.info("Table does not exist (b): {}".format(table_name))

            self.library.database.remove_partition(dataset.partition)


        elif dataset:

            b = LibraryDbBundle(self.library.database, dataset.vid)
            for p in b.partitions:
                self.remove(p.identity.vname)

            self.logger.info('Removing bundle {}'.format(dataset.vname))
            self.library.database.remove_bundle(b)
        else:
            self.logger.error("Failed to find partition or bundle by name '{}'".format(name))


    def run_sql(self, sql_text):

        e = self.database.connection.execute

        e(sql_text)

