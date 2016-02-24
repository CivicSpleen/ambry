"""
Provides SQL access to datasets in the library, allowing users to issue SQL queries, either as SQL
or via Sqlalchemy, to return datasets.

Example:
    import ambry
    l = ambry.get_library()
    w = Warehouse(l)
    for row in Warehouse(l).query('SELECT * FROM <partition id or vid> ... '):
        print row
    w.close()
"""

import logging

from sqlalchemy import create_engine

from ambry.identity import ObjectNumber, NotObjectNumberError, TableNumber
from ambry.util import get_logger

logger = get_logger(__name__, level=logging.ERROR)



class Warehouse(object):
    """ Provides SQL access to datasets in the library, allowing users to issue SQL queries, either as SQL
or via SQLAlchemy, to return datasets.
    """

    def __init__(self, library, dsn=None,  logger = None):
        from ambry.library import Library
        assert isinstance(library, Library)

        self._library = library

        if not logger:
            import logging
            self._logger = get_logger(__name__, level=logging.ERROR, propagate=False)
        else:
            self._logger = logger

        if not dsn:
            # Use library database.
            dsn = library.database.dsn

        # Initialize appropriate backend.
        if dsn.startswith('sqlite:'):
            from ambry.mprlib.backends.sqlite import SQLiteBackend
            self._logger.debug('Initializing sqlite warehouse.')
            self._backend = SQLiteBackend(library, dsn)

        elif dsn.startswith('postgres'):
            try:
                from ambry.mprlib.backends.postgresql import PostgreSQLBackend
                self._logger.debug('Initializing postgres warehouse.')
                self._backend = PostgreSQLBackend(library, dsn)
            except ImportError as e:
                from ambry.mprlib.backends.sqlite import SQLiteBackend
                from ambry.util import set_url_part, select_from_url
                dsn = "sqlite:///{}/{}".format(self._library.filesystem.build('warehouses'),
                                               select_from_url(dsn,'path').strip('/')+".db")
                self._logger.error("Failed to import required modules ({})for Postgres warehouse. Using Sqlite dsn={}"
                              .format(e, dsn))
                self._backend = SQLiteBackend(library, dsn)

        else:
            raise Exception('Do not know how to handle {} dsn.'.format(dsn))

        self._warehouse_dsn = dsn

    @property
    def dsn(self):
        return self._warehouse_dsn

    @property
    def connection(self):
        return self._backend._get_connection()

    def clean(self):
        """Remove all of the tables and data from the warehouse"""
        connection = self._backend._get_connection()
        self._backend.clean(connection)

    def list(self):
        """List the tables in the database"""
        connection = self._backend._get_connection()
        return list(self._backend.list(connection))

    @property
    def engine(self):
        """Return A Sqlalchemy engine"""
        return create_engine(self._warehouse_dsn)

    def install(self, ref, table_name=None, index_columns=None,logger=None):
        """ Finds partition by reference and installs it to warehouse db.

        Args:
            ref (str): id, vid (versioned id), name or vname (versioned name) of the partition.

        """


        try:
            obj_number = ObjectNumber.parse(ref)
            if isinstance(obj_number, TableNumber):
                table = self._library.table(ref)
                connection = self._backend._get_connection()
                return self._backend.install_table(connection, table, logger=logger)
            else:
                # assume partition
                raise NotObjectNumberError

        except NotObjectNumberError:
            # assume partition.
            partition = self._library.partition(ref)
            connection = self._backend._get_connection()

            return self._backend.install(
                connection, partition, table_name=table_name, index_columns=index_columns,
                logger=logger)

    def materialize(self, ref, table_name=None, index_columns=None, logger=None):
        """ Creates materialized table for given partition reference.

        Args:
            ref (str): id, vid, name or vname of the partition.

        Returns:
            str: name of the partition table in the database.

        """
        from ambry.library import Library
        assert isinstance(self._library, Library)

        logger.debug('Materializing warehouse partition.\n    partition: {}'.format(ref))
        partition = self._library.partition(ref)

        connection = self._backend._get_connection()

        return self._backend.install(connection, partition, table_name=table_name,
                                     index_columns=index_columns, materialize=True, logger=logger)

    def index(self, ref, columns):
        """ Create an index on the columns.

        Args:
            ref (str): id, vid, name or versioned name of the partition.
            columns (list of str): names of the columns needed indexes.

        """
        from ambry.orm.exc import NotFoundError

        logger.debug('Creating index for partition.\n    ref: {}, columns: {}'.format(ref, columns))

        connection = self._backend._get_connection()

        try:
            table_or_partition = self._library.partition(ref)
        except NotFoundError:
            table_or_partition = ref


        self._backend.index(connection, table_or_partition, columns)

    def parse_sql(self, asql):
        """ Executes all sql statements from asql.

        Args:
            library (library.Library):
            asql (str): ambry sql query - see https://github.com/CivicKnowledge/ambry/issues/140 for details.
        """
        import sqlparse

        statements = sqlparse.parse(sqlparse.format(asql, strip_comments=True))
        parsed_statements = []
        for statement in statements:

            statement_str = statement.to_unicode().strip()

            for preprocessor in self._backend.sql_processors():
                statement_str = preprocessor(statement_str, self._library, self._backend, self.connection)

            parsed_statements.append(statement_str)

        return parsed_statements

    def query(self, asql, logger=None):
        """
        Execute an ASQL file and return the result of the first SELECT statement.

        :param asql:
        :param logger:
        :return:
        """
        import sqlparse
        from ambry.mprlib.exceptions import BadSQLError
        from ambry.bundle.asql_parser import process_sql
        from ambry.orm.exc import NotFoundError

        if not logger:
            logger = self._library.logger

        rec = process_sql(asql, self._library)

        for drop in reversed(rec.drop):

            if drop:
                connection = self._backend._get_connection()
                cursor = self._backend.query(connection, drop, fetch=False)
                cursor.close()

        for vid in rec.materialize:
            logger.debug('Materialize {}'.format(vid))
            self.materialize(vid, logger=logger)

        for vid in rec.install:
            logger.debug('Install {}'.format(vid))

            self.install(vid, logger=logger)

        for table_or_vid, columns in rec.indexes:

            logger.debug('Index {}'.format(vid))

            try:
                self.index(table_or_vid, columns)
            except NotFoundError as e:
                # Comon when the index table in's a VID, so no partition can be found.

                logger.debug('Failed to index {}; {}'.format(vid, e))
            except Exception as e:
                logger.error('Failed to index {}; {}'.format(vid, e))

        for statement in rec.statements:

            statement = statement.strip()

            logger.debug("Process statement: {}".format(statement[:60]))

            if statement.lower().startswith('create'):
                logger.debug('    Create {}'.format(statement))
                connection = self._backend._get_connection()
                cursor = self._backend.query(connection, statement, fetch=False)

                cursor.close()

            elif statement.lower().startswith('select'):
                logger.debug('Run query {}'.format(statement))
                connection = self._backend._get_connection()
                return self._backend.query(connection, statement, fetch=False)

        # A fake cursor that can be closed and iterated
        class closable_iterable(object):
            def close(self):
                pass

            def __iter__(self):
                pass

        return closable_iterable()

    def dataframe(self,asql, logger = None):
        """Like query(), but returns a Pandas dataframe"""
        import pandas as pd
        from ambry.mprlib.exceptions import BadSQLError

        try:
            def yielder(cursor):

                for i, row in enumerate(cursor):
                    if i == 0:
                        yield [ e[0] for e in cursor.getdescription()]

                    yield row

            cursor = self.query(asql, logger)

            yld = yielder(cursor)

            header = next(yld)

            return pd.DataFrame(yld, columns=header)
        except BadSQLError as e:
            import traceback
            self._logger.error("SQL Error: {}".format( e))
            self._logger.debug(traceback.format_exc())

    def geoframe(self, sql, simplify=None, crs=None, epsg=4326):
        """
        Return geopandas dataframe

        :param simplify: Integer or None. Simplify the geometry to a tolerance, in the units of the geometry.
        :param crs: Coordinate reference system information
        :param epsg: Specifiy the CRS as an EPGS number.
        :return: A Geopandas GeoDataFrame
        """
        import geopandas
        from shapely.wkt import loads
        from fiona.crs import from_epsg

        if crs is None:
            try:
                crs = from_epsg(epsg)
            except TypeError:
                raise TypeError('Must set either crs or epsg for output.')

        df = self.dataframe(sql)
        geometry = df['geometry']

        if simplify:
            s = geometry.apply(lambda x: loads(x).simplify(simplify))
        else:
            s = geometry.apply(lambda x: loads(x))

        df['geometry'] = geopandas.GeoSeries(s)

        return geopandas.GeoDataFrame(df, crs=crs, geometry='geometry')

    def shapes(self, simplify=None):
        """
        Return geodata as a list of Shapely shapes

        :param simplify: Integer or None. Simplify the geometry to a tolerance, in the units of the geometry.
        :param predicate: A single-argument function to select which records to include in the output.

        :return: A list of Shapely objects
        """

        from shapely.wkt import loads

        if simplify:
            return [loads(row.geometry).simplify(simplify) for row in self]
        else:
            return [loads(row.geometry) for row in self]

    def patches(self, basemap, simplify=None, predicate=None, args_f=None, **kwargs):
        """
        Return geodata as a list of Matplotlib patches

        :param basemap: A mpl_toolkits.basemap.Basemap
        :param simplify: Integer or None. Simplify the geometry to a tolerance, in the units of the geometry.
        :param predicate: A single-argument function to select which records to include in the output.
        :param args_f: A function that takes a row and returns a dict of additional args for the Patch constructor

        :param kwargs: Additional args to be passed to the descartes Path constructor
        :return: A list of patch objects
        """
        from descartes import PolygonPatch
        from shapely.wkt import loads
        from shapely.ops import transform

        if not predicate:
            predicate = lambda row: True

        def map_xform(x, y, z=None):
            return basemap(x, y)

        def make_patch(shape, row):

            args = dict(kwargs.items())

            if args_f:
                args.update(args_f(row))

            return PolygonPatch(transform(map_xform, shape), **args)

        def yield_patches(row):

            if simplify:
                shape = loads(row.geometry).simplify(simplify)
            else:
                shape = loads(row.geometry)

            if shape.geom_type == 'MultiPolygon':
                for subshape in shape.geoms:
                    yield make_patch(subshape, row)
            else:
                yield make_patch(shape, row)

        return [patch for row in self if predicate(row)
                for patch in yield_patches(row)]


    def close(self):
        """ Closes warehouse database. """
        self._backend.close()
