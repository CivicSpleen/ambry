"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..dbexceptions import DependencyError
from relational import RelationalWarehouse
from ..library.database import LibraryDb
from . import ResolutionError


class SqliteWarehouse(RelationalWarehouse):


    ##
    ## Datasets and Bundles
    ##


    def _ogr_args(self, partition):

        return [
            "-f SQLite ",self.database.path,
            "-gt 65536",
            partition.database.path,
            "-dsco SPATIALITE=no"]


    def load_local(self, partition, table_name, where):
        return self.load_attach(partition, table_name, where)

    def load_attach(self, partition, table_name, where = None):

        self.logger.info('load_attach {}'.format(partition.identity.name))

        p_vid = partition.identity.vid
        d_vid = partition.identity.as_dataset().vid

        source_table_name = table_name
        dest_table_name =  self.augmented_table_name(partition.identity, table_name)

        copy_n = 100 if self.test else None

        with self.database.engine.begin() as conn:
            atch_name = self.database.attach(partition, conn=conn)
            self.logger.info('load_attach {} in {}'.format(table_name, partition.database.path))
            self.database.copy_from_attached( table=(source_table_name, dest_table_name),
                                              on_conflict='REPLACE',
                                              name=atch_name, conn=conn, copy_n = copy_n, where = where)

        self.logger.info('done {}'.format(partition.identity.vname))

        return dest_table_name


    def load_remote(self, partition, table_name, urls):

        import shlex
        from sh import ambry_load_sqlite, ErrorReturnCode_1

        self.logger.info('load_remote {} '.format(partition.identity.vname, table_name))

        d_vid = partition.identity.as_dataset().vid

        a_table_name = self.augmented_table_name(partition.identity, table_name)

        for url in urls:

            self.logger.info("Load Sqlite {} -> {}".format(url, self.database.path))

            try:
                ## Call the external program ambry-load-sqlite to load data into
                ## sqlite
                p = ambry_load_sqlite(url, self.database.path, a_table_name,
                                      _err=self.logger.error, _out=self.logger.info )
                p.wait()
            except Exception as e:
                self.logger.error("Failed to load: {} {}: {}".format(partition.identity.vname, table_name, e.message))

        return a_table_name

    def install_view(self, name, sql):

        self.logger.info('Installing view {}'.format(name))

        sql = """
        DROP VIEW  IF EXISTS {name};
        CREATE VIEW {name} AS {sql}
        """.format(name=name, sql=sql)

        self.database.connection.connection.cursor().executescript(sql)



    def install_material_view(self, name, sql, clean=False):
        from pysqlite2.dbapi2 import  OperationalError
        self.logger.info('Installing materialized view {}'.format(name))

        if clean:
            self.logger.info('mview_remove {}'.format(name))
            self.database.connection.connection.cursor().executescript("DROP TABLE IF EXISTS {}".format(name))

        sql = """
        CREATE TABLE {name} AS {sql}
        """.format(name=name, sql=sql)


        try:
            self.database.connection.connection.cursor().executescript(sql)
        except OperationalError as e:
            if 'exists' not in str(e).lower():
                raise

            self.logger.info('mview_exists {}'.format(name))
            # Ignore if it already exists.

    def run_sql(self, sql_text):

        self.logger.info('Running SQL')

        self.database.connection.executescript(sql_text)

    def installed_table(self, name):
        """Return schema information for tables and views """

        ce = self.database.connection.execute

        out = []
        for row in ce('PRAGMA table_info({})'.format(name)).fetchall():
             out.append(
                dict(
                    name = row['name'],
                    type = row['type'] if row['type'] else 'TEXT',

                 ),
             )

        return out


class SpatialiteWarehouse(SqliteWarehouse):

    def _ogr_args(self, partition):

        return [
            "-f SQLite ", self.database.path,
            "-gt 65536",
            partition.database.path,
            "-dsco SPATIALITE=yes"]


    def install_material_view(self, name, sql, clean=False):

        super(SpatialiteWarehouse, self).install_material_view(name, sql, clean=clean)

        ce = self.database.connection.execute

        if 'geometry' in [ row['name'].lower() for row in ce('PRAGMA table_info({})'.format(name)).fetchall()]:
            types = ce('SELECT count(*) AS count, GeometryType(geometry) AS type,  CoordDimension(geometry) AS cd '
                       'FROM {} GROUP BY type ORDER BY type desc;'.format(name)).fetchall()

            t = types[0][1]
            cd = types[0][2]

            ce("SELECT RecoverGeometryColumn('{}', 'geometry', 4326, '{}', '{}');".format(name, t, cd))
