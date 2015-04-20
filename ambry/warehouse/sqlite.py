"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..dbexceptions import DependencyError
from . import Warehouse
from ..library.database import LibraryDb
from . import ResolutionError


class SqliteWarehouse(Warehouse):

    ##
    ## Datasets and Bundles
    ##

    def load_local(
            self,
            partition,
            source_table_name,
            dest_table_name=None,
            where=None):

        return self.load_attach(
            partition,
            source_table_name,
            dest_table_name,
            where)

    def load_attach(
            self,
            partition,
            source_table_name,
            dest_table_name=None,
            where=None):

        self.logger.info('load_attach {}'.format(partition.identity.name))

        copy_n = 100 if self.test else None

        with self.database.engine.begin() as conn:
            atch_name = self.database.attach(partition, conn=conn)
            self.logger.info(
                'load_attach {} in {}'.format(
                    source_table_name,
                    partition.database.path))
            self.database.copy_from_attached(
                table=(
                    source_table_name,
                    dest_table_name),
                on_conflict='REPLACE',
                name=atch_name,
                conn=conn,
                copy_n = copy_n,
                where = where)

        # Geographic partitions need to be updates to be recognized

        if partition.is_geo and self.database.is_geo:

            from ..geo.util import recover_geometry

            with self.database.engine.begin() as conn:
                table = partition.get_table(source_table_name)

                for column in table.columns:
                    if column.type_is_geo() or column.name.endswith('geometry'):
                        recover_geometry(
                            conn,
                            dest_table_name,
                            column.fq_name,
                            column.datatype)

        self.logger.info('done {}'.format(partition.identity.vname))

        return dest_table_name

    def run_sql(self, sql_text):

        self.logger.info('Running SQL')

        self.database.connection.executescript(sql_text)


class SpatialiteWarehouse(SqliteWarehouse):

    def install_material_view(self, name, sql, clean=False, data=None):
        """
        After installing thematerial view, look for geometry columns and add them to the spatial system
        :param name:
        :param sql:
        :param clean:
        :param data:
        :return:
        """

        if not super(SpatialiteWarehouse, self).install_material_view(name, sql, clean=clean, data=data):
            return False

        # Clean up the geometry

        ce = self.database.connection.execute

        for col in [row['name'].lower() for row in ce('PRAGMA table_info({})'.format(name)).fetchall()]:

            if col.endswith('geometry'):

                types = ce(
                    'SELECT count(*) AS count, GeometryType({}) AS type,  CoordDimension({}) AS cd '
                    'FROM {} GROUP BY type ORDER BY type desc;'.format(
                        col,
                        col,
                        name)).fetchall()

                t = types[0][1]
                cd = types[0][2]

                # connection.execute(
                #    'UPDATE {} SET {} = SetSrid({}, {});'.format(table_name, column_name, column_name, srs))

                ce("SELECT RecoverGeometryColumn('{}', '{}', 4326, '{}', '{}');".format(
                    name, col, t, cd))

        return True
