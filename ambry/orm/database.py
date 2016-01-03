"""Basic Sqlalchemy database initialization for this ORM

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from collections import namedtuple
import pkgutil
import os

from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import create_engine, event, DDL
from ambry.orm.exc import DatabaseError, DatabaseMissingError, NotFoundError, ConflictError

from ambry.util import get_logger, parse_url_to_dict
from . import Column, Partition, Table, Dataset, Config, File,\
    Code, ColumnStat, DataSource, SourceColumn, SourceTable
from ambry.orm.remote import Remote
from ambry.orm.account import Account
from ambry.orm.process import Process

ROOT_CONFIG_NAME = 'd000'
ROOT_CONFIG_NAME_V = 'd000001'

SCHEMA_VERSION = 118

# Note: If you are going to change POSTGRES_SCHEMA_NAME do not forget to change docs:
#   1. README.rst (search for 'Install pg_trgm extension')
#   2. doc/README.testing.md (search for 'Install multicorn and pg_trgm extensions')
POSTGRES_SCHEMA_NAME = 'ambrylib'  # schema for ambry library
POSTGRES_PARTITION_SCHEMA_NAME = 'partitions'  # schema for mpr files

# Database connection information
Dbci = namedtuple('Dbc', 'dsn_template sql')

# Remap the schema
scheme_map = {'postgis': 'postgresql+psycopg2', 'spatialite': 'sqlite'}

MIGRATION_TEMPLATE = '''\
# -*- coding: utf-8 -*-'

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        # connection.execute('ALTER table ...')
        pass

    def _migrate_postgresql(self, connection):
        self._migrate_sqlite(connection)
'''

logger = get_logger(__name__)


class Database(object):
    """ Stores local database of the datasets. """

    def __init__(self, dsn, echo=False, foreign_keys=True, engine_kwargs=None):
        """ Initializes database.

        Args:
            dsn (str): database connect string, 'sqlite://' for example.
            echo (boolean): echo parameter of the create_engine.
            engine_kwargs (dict): parameters to pass to the create_engine method of the Sqlalchemy.

        """

        self.dsn = dsn

        d = parse_url_to_dict(self.dsn)
        self.path = d['path'].replace('//', '/')

        self.driver = d['scheme']
        self.engine_kwargs = engine_kwargs or {}

        self.Session = None
        self._session = None
        self._engine = None
        self._connection = None
        self._echo = echo
        self._foreign_keys = foreign_keys

        self._raise_on_commit = False # For debugging

        if self.driver in ['postgres', 'postgresql', 'postgresql+psycopg2', 'postgis']:
            self.driver = 'postgres'
            self._schema = POSTGRES_SCHEMA_NAME
        else:
            self._schema = None

        self.logger = logger

        self.library = None # Set externally when checking in in

    def create(self):
        """Create the database from the base SQL."""

        if not self.exists():
            self._create_path()
            self.create_tables()
            return True

        return False

    def _create_path(self):
        """Create the path to hold the database, if one wwas specified."""

        if self.driver == 'sqlite' and 'memory' not in self.dsn and self.dsn != 'sqlite://':

            dir_ = os.path.dirname(self.path)

            if dir_ and not os.path.exists(dir_):
                try:
                    # Multiple process may try to make, so it could already
                    # exist
                    os.makedirs(dir_)
                except Exception:
                    pass

                if not os.path.exists(dir_):
                    raise Exception("Couldn't create directory " + dir_)

    #
    # Creation and Existence
    #

    def exists(self):
        """Return True if the database exists, or for Sqlite, which will create the file on the
        first reference, the file has been initialized with the root config """

        if self.driver == 'sqlite' and not os.path.exists(self.path):
            return False

        # init engine
        self.engine

        try:
            # Since we are using the connection, rather than the session, need to
            # explicitly set the search path.

            from sqlalchemy.engine.reflection import Inspector

            inspector = Inspector.from_engine(self.engine)

            if 'config' in inspector.get_table_names(schema=self._schema):
                return True
            else:
                return False
        finally:
            self.close_connection()

    @property
    def engine(self):
        """return the SqlAlchemy engine for this database."""
        from sqlalchemy.pool import NullPool

        if not self._engine:

            if 'postgres' in self.driver:

                if not 'connect_args' in self.engine_kwargs:
                    self.engine_kwargs['connect_args'] = {
                        "application_name": "ambry:{}".format(os.getpid())
                    }

            # For most use, a small pool is good to prevent connection exhaustion, but these settings my be
            # too low for the main public web application


                self._engine = create_engine(self.dsn, echo=self._echo,
                           pool_size = 5, max_overflow = 5, **self.engine_kwargs)

            else:
                self._engine = create_engine(self.dsn, echo=self._echo, **self.engine_kwargs)

            #
            # Disconnect connections that have a different PID from the one they were created in.
            # This protects against re-use in multi-processing.
            #
            @event.listens_for(self._engine, "connect")
            def connect(dbapi_connection, connection_record):
                connection_record.info['pid'] = os.getpid()

            @event.listens_for(self._engine, "checkout")
            def checkout(dbapi_connection, connection_record, connection_proxy):

                from sqlalchemy.exc import DisconnectionError
                pid = os.getpid()
                if connection_record.info['pid'] != pid:

                    connection_record.connection = connection_proxy.connection = None
                    raise DisconnectionError(
                        "Connection record belongs to pid %s, attempting to check out in pid %s" %
                        (connection_record.info['pid'], pid))

            if self.driver == 'sqlite':
                @event.listens_for(self._engine, "connect")
                def pragma_on_connect(dbapi_con, con_record):
                    """ISSUE some Sqlite pragmas when the connection is created."""

                    # dbapi_con.execute('PRAGMA foreign_keys = ON;')
                    # Not clear that there is a performance improvement.

                    dbapi_con.execute('PRAGMA journal_mode = WAL')
                    dbapi_con.execute('PRAGMA synchronous = OFF')
                    dbapi_con.execute('PRAGMA temp_store = MEMORY')
                    dbapi_con.execute('PRAGMA cache_size = 500000')
                    if self._foreign_keys:
                        dbapi_con.execute('PRAGMA foreign_keys=ON')

            with self._engine.connect() as conn:
                _validate_version(conn, self.dsn)

        return self._engine

    @property
    def connection(self):
        """Return an SqlAlchemy connection."""
        if not self._connection:
            self._connection = self.engine.connect()

        return self._connection

    @property
    def session(self):
        """Return a SqlAlchemy session."""
        from sqlalchemy.orm import sessionmaker
        from sqlalchemy.event import listen

        if not self.Session:
            self.Session = sessionmaker(bind=self.engine)

        if not self._session:
            self._session = self.Session()
            # set the search path

            if self._schema:
                def after_begin(session, transaction, connection):
                    #import traceback
                    #print traceback.print_stack()
                    session.execute('SET search_path TO {}'.format(self._schema))

                listen(self._session, 'after_begin', after_begin)

        return self._session

    def set_connection_search_path(self):

        if self._schema:
            self.connection.execute('SET search_path TO {}'.format(self._schema))

    def open(self):
        """ Ensure the database exists and is ready to use. """

        # Creates the session, connection and engine
        self.session

        if not self.exists():
            self.create()

    def close(self):
        self.close_session()
        self.close_connection()

        if self._engine:
            self._engine.dispose() # CLose all of the connections in the pool
            #self._engine = None

    def close_session(self):

        if self._session:
            self._session.close()
            # self._session.bind.dispose()
            self._session = None

    def close_connection(self):

        if self._connection:
            self._connection.close()
            self._connection = None

    def commit(self):

        if self._raise_on_commit: # For debugging
            raise Exception("Committed")

        self.session.commit()
        # self.close_session()

    def rollback(self):
        self.session.rollback()
        # self.close_session()

    def clean(self):

        for ds in self.datasets:
            self.logger.info('Cleaning: {}'.format(ds.name))
            self.remove_dataset(ds)

        # self.remove_dataset(self.root_dataset)

        self.create()

        self.commit()

    def clean_root(self):
        pass

    def drop(self):

        # Should close connection before table drop to avoid hanging in postgres.
        # http://docs.sqlalchemy.org/en/rel_0_8/faq.html#metadata-schema

        if False:
            for ds in self.datasets:
                self.logger.info('Cleaning: {}'.format(ds.name))
                try:
                    self.remove_dataset(ds)
                except:
                    pass

            try:
                self.remove_dataset(self.root_dataset)
            except:
                pass

        self.commit()

        # Delete all of the data
        for tbl in reversed(self.metadata.sorted_tables):
            self.logger.info('Deleting data from  {}'.format(tbl))
            self.engine.execute(tbl.delete())

        if self.dsn.startswith('sqlite:') and self.exists():
            # remove sqlite file.
            os.remove(self.path)
        else:
            self.commit()
            self.close_session()
            self.close_connection()

            # On postgres, this usually just locks up.
            for tbl in reversed(self.metadata.sorted_tables):
                self.logger.info('Droping {}'.format(tbl))
                tbl.drop(self.engine)

    @property
    def metadata(self):
        """Return an SqlAlchemy MetaData object, bound to the engine."""

        from sqlalchemy import MetaData

        metadata = MetaData(bind=self.engine, schema=self._schema)

        metadata.reflect(self.engine)

        return metadata

    @property
    def inspector(self):
        from sqlalchemy.engine.reflection import Inspector

        return Inspector.from_engine(self.engine)

    def clone(self):
        return self.__class__(self.dsn)

    def create_table(self, table):
        pass

    def create_tables(self):

        from sqlalchemy.exc import OperationalError

        tables = [
            Dataset, Config, Table, Column, Partition, File, Code,
            ColumnStat, SourceTable, SourceColumn, DataSource, Account, Process, Remote]

        try:
            self.drop()
        except (OperationalError, ProgrammingError):
            pass

        # Working on the theory that this routine is only ever run once, when the database is created.
        # See http://stackoverflow.com/a/22212214 for how to use events and DDL to create the schema.

        orig_schemas = {}

        for table in tables:
            it = table.__table__
            # These schema shenanigans are almost certainly wrong.
            # But they are expedient. For Postgres, it puts the library
            # tables in the Library schema.
            if self._schema:
                orig_schemas[it] = it.schema
                it.schema = self._schema

            it.create(bind=self.engine)

        # We have to put the schemas back because when installing to a warehouse.
        # the same library classes can be used to access a Sqlite database, which
        # does not handle schemas.
        if self._schema:
            for it, orig_schema in list(orig_schemas.items()):
                it.schema = orig_schema

        self._add_config_root()

    def _add_config_root(self):
        """ Adds the root dataset, which holds configuration values for the database. """

        try:
            self.session.query(Dataset).filter_by(id=ROOT_CONFIG_NAME).one()
            self.close_session()
        except NoResultFound:
            o = Dataset(
                id=ROOT_CONFIG_NAME,
                vid=ROOT_CONFIG_NAME_V,
                name=ROOT_CONFIG_NAME,
                vname=ROOT_CONFIG_NAME_V,
                fqname='datasetroot-0.0.0~' + ROOT_CONFIG_NAME_V,
                cache_key=ROOT_CONFIG_NAME,
                version='0.0.0',
                source=ROOT_CONFIG_NAME,
                dataset=ROOT_CONFIG_NAME,
                revision=1,
            )
            self.session.add(o)
            self.commit()

    #
    # Base Object Access
    #

    def new_dataset(self, *args, **kwargs):
        """ Creates a new dataset

        :param args: Positional args passed to the Dataset constructor.
        :param kwargs:  Keyword args passed to the Dataset constructor.
        :return: :class:`ambry.orm.Dataset`
        :raises: :class:`ambry.orm.ConflictError` if the a Dataset records already exists with the given vid
        """

        ds = Dataset(*args, **kwargs)

        try:
            self.session.add(ds)
            self.session.commit()
            ds._database = self
            return ds
        except IntegrityError as e:
            self.session.rollback()
            raise ConflictError(
                "Can't create dataset '{}'; one probably already exists: {} ".format(str(ds), e))

    @property
    def root_dataset(self):
        """Return the root dataset, which hold configuration values for the library"""
        return self.dataset(ROOT_CONFIG_NAME_V)

    @property
    def package_dataset(self):
        """For sqlite bundle packages, return the first ( and only ) dataset"""

        return self.session.query(Dataset).filter(Dataset.vid != ROOT_CONFIG_NAME_V).one()

    def dataset(self, ref, load_all=False, exception=True):
        """Return a dataset, given a vid or id

        :param ref: Vid or id  for a dataset. If an id is provided, will it will return the one with the
        largest revision number
        :param load_all: Use a query that eagerly loads everything.
        :return: :class:`ambry.orm.Dataset`

        """

        ref = str(ref)

        try:
            ds = self.session.query(Dataset).filter(Dataset.vid == ref).one()
        except NoResultFound:
            ds = None

        if not ds:
            try:
                ds = self.session.query(Dataset).filter(Dataset.id == ref)\
                    .order_by(Dataset.revision.desc()).first()
            except NoResultFound:
                ds = None

        if not ds:
            try:
                ds = self.session.query(Dataset).filter(Dataset.vname == ref).one()
            except NoResultFound:
                ds = None

        if not ds:
            try:
                ds = self.session.query(Dataset).filter(Dataset.name == ref)\
                    .order_by(Dataset.revision.desc()).first()
            except NoResultFound:
                ds = None

        if ds:
            ds._database = self
            return ds
        elif exception:
            raise NotFoundError('No dataset in library for vid : {} '.format(ref))
        else:
            return None

    @property
    def datasets(self):
        """
        Return all datasets

        :return:
        """

        return self.session.query(Dataset).filter(Dataset.vid != ROOT_CONFIG_NAME_V).all()

    def remove_dataset(self, ds):

        if ds:
            self.delete_tables_partitions(ds)
            self.session.delete(ds)
            self.session.commit()

    def delete_tables_partitions(self, ds):
        """Fast delete of all of a datasets codes, columns, partitions and tables"""
        from ambry.orm import Code, Column, Table, Partition, ColumnStat, Process

        ssq = self.session.query

        ssq(Process).filter(Process.d_vid == ds.vid).delete()
        ssq(Code).filter(Code.d_vid == ds.vid).delete()
        ssq(ColumnStat).filter(ColumnStat.d_vid == ds.vid).delete()
        ssq(Column).filter(Column.d_vid == ds.vid).delete()
        ssq(Partition).filter(Partition.d_vid == ds.vid).delete()

        for source in ds.sources:
            source._dest_table = None

        ssq(Table).filter(Table.d_vid == ds.vid).delete()

    def delete_partitions(self, ds):
        """Fast delete of all of a datasets codes, columns, partitions and tables"""
        from ambry.orm import Partition

        ssq = self.session.query

        ssq(Process).filter(Process.d_vid == ds.vid).delete()
        ssq(Code).filter(Code.d_vid == ds.vid).delete()
        ssq(ColumnStat).filter(ColumnStat.d_vid == ds.vid).delete()
        ssq(Partition).filter(Partition.d_vid == ds.vid).delete()


    def copy_dataset(self, ds, incver=False, cb=None):
        """
        Copy a dataset into the database.
        :param ds: The source dataset to copy
        :param cb: A progress callback, taking two parameters: cb(message, num_records)
        :return:
        """
        from ambry.orm import Table, Column, Partition, File, ColumnStat, Code, \
            DataSource, SourceTable, SourceColumn, Dataset, Config

        tables = [ Table, Column, Partition, File, ColumnStat, Code, SourceTable, SourceColumn, DataSource ]

        return self._copy_dataset_copy(ds, tables, incver, cb)

    def copy_dataset_files(self, ds, incver=False, cb=None):
        """
        Copy only files and configs into the database.
        :param ds: The source dataset to copy
        :param cb: A progress callback, taking two parameters: cb(message, num_records)
        :return:
        """
        from ambry.orm import File

        tables = [File]

        return self._copy_dataset_copy(ds, tables, incver, cb)

    def _copy_dataset_copy(self, ds, tables, incver, cb=None):
        from sqlalchemy.orm import noload

        source_session = ds.session
        dest_session = self.session

        ds = source_session.query(Dataset).filter(Dataset.vid == ds.vid).options(noload('*')).first()

        dso = ds.incver() if incver else ds

        dest_session.merge(dso)
        dest_session.commit()

        for table_class in tables:
            self._copy_dataset_merge(ds, source_session, dest_session, table_class, incver, cb=cb)

        self._copy_dataset_configs(ds, source_session, dest_session, incver)

        return self.dataset(dso.vid)

    def _copy_dataset_merge(self, ds, source_session, dest_session, table_class, incver, cb = None):
        from sqlalchemy.orm import noload, undefer

        i = [0]

        options = [noload('*')]
        if table_class == File:
            options.append(undefer('contents'))

        for o in source_session.query(table_class).filter(table_class.d_vid == ds.vid).options(*options).all():

            if incver:
                o = o.incver()

            dest_session.merge(o)

            i[0] += 1

            if i[0] % 1000 == 0:
                if cb:
                    cb('Copy dataset', i[0])
                else:
                    self.logger.info("Copied {} records".format(i[0]))

                dest_session.commit()

    def _copy_dataset_configs(self, ds, source_session, dest_session, incver):
        # FIXME: Oh, this is horrible. Sqlalchemy inserts all of the configs as a group, but they are self-referential,
        # so some with a reference to a parent get inserted before their parent. The topo sort solves this,
        # but there must be a better way to do it.

        from ..util import toposort
        from sqlalchemy.orm import noload

        configs = source_session.query(Config).filter(Config.d_vid == ds.vid).options(noload('*')).all()

        dag = {c.id: set([c.parent_id]) for c in configs}
        refs = {c.id: c for c in configs}
        ordered = []

        for e in toposort(dag):
            for ref in e:
                if ref:
                    config = refs[ref]
                    if incver:
                        config = config.incver()
                    dest_session.merge(config)

        dest_session.commit()


    def next_sequence_id(self, parent_table_class, parent_vid, child_table_class):
        """Get the next sequence id for child objects for a parent object that has a child sequence
        field"""

        from sqlalchemy import text

        # Name of sequence id column in the child
        c_seq_col = child_table_class.sequence_id.property.columns[0].name

        p_seq_col = getattr(parent_table_class, c_seq_col).property.columns[0].name

        p_vid_col = parent_table_class.vid.property.columns[0].name

        try:
            parent_col = child_table_class._parent_col
        except AttributeError:
            parent_col = child_table_class.d_vid.property.columns[0].name

        if self.driver == 'sqlite':
            # The Sqlite version is not atomic, but Sqlite also doesn't support concurrency
            # So, we don't have to open a new connection, but we also can't open a new connection, so
            # this uses the session.
            self.commit()
            sql = text("SELECT  {p_seq_col} FROM {p_table} WHERE {p_vid_col} = '{parent_vid}' "
                       .format(p_table=parent_table_class.__tablename__, p_seq_col=p_seq_col,
                               p_vid_col=p_vid_col, parent_vid=parent_vid))

            v = next(iter(self.session.execute(sql)))[0]
            sql = text("UPDATE {p_table} SET {p_seq_col} = {p_seq_col} + 1 WHERE {p_vid_col} = '{parent_vid}' "
                       .format(p_table=parent_table_class.__tablename__, p_seq_col=p_seq_col,
                               p_vid_col=p_vid_col, parent_vid=parent_vid))

            self.session.execute(sql)
            self.commit()
            return v

        else:
            # Must be postges, or something else that supports "RETURNING"
            sql = text("""
            UPDATE {p_table} SET {p_seq_col} = {p_seq_col} + 1 WHERE {p_vid_col} = '{parent_vid}' RETURNING {p_seq_col}
            """.format(p_table=parent_table_class.__tablename__, p_seq_col=p_seq_col, p_vid_col=p_vid_col,
                       parent_vid=parent_vid))

            self.connection.execute('SET search_path TO {}'.format(self._schema))
            v = next(iter(self.connection.execute(sql)))[0]
            return v-1

    def update_sequence_id(self, parent_table_class, parent_vid, child_table_class):
        from sqlalchemy import text

        # Name of sequence id column in the child
        c_seq_col = child_table_class.sequence_id.property.columns[0].name

        p_seq_col = getattr(parent_table_class, c_seq_col).property.columns[0].name

        try:
            parent_col = child_table_class._parent_col
        except AttributeError:
            parent_col = child_table_class.d_vid.property.columns[0].name

        sql = text("SELECT max({c_seq_col})+1 FROM {table} WHERE {parent_col} = '{parent_vid}'"
                   .format(table=child_table_class.__tablename__, parent_col=parent_col,
                           c_seq_col=c_seq_col, parent_vid=parent_vid))

        max_id, = self.connection.execute(sql).fetchone()

        if not max_id:
            max_id = 1

        return max_id


class BaseMigration(object):
    """ Base class for all migrations. """

    def migrate(self, connection):
        # use transactions
        if connection.engine.name == 'sqlite':
            self._migrate_sqlite(connection)
        elif connection.engine.name == 'postgresql':
            self._migrate_postgresql(connection)
        else:
            raise DatabaseMissingError(
                'Do not know how to migrate {} engine.'.format(self.connection))

    def _migrate_sqlite(self, connection):
        raise NotImplementedError(
            'subclasses of MigrationBase must provide a _migrate_sqlite() method')

    def _migrate_postgresql(self, connection):
        raise NotImplementedError(
            'subclasses of MigrationBase must provide a _migrate_postgresql() method')


class VersionIsNotStored(Exception):
    """ Means that ambry never updated db schema. """
    pass


def migrate(connection, dsn):
    """ Collects all migrations and applies missed.

    Args:
        connection (sqlalchemy connection):

    """
    all_migrations = _get_all_migrations()
    logger.debug('Collected migrations: {}'.format(all_migrations))

    for version, modname in all_migrations:
        if _is_missed(connection, version) and version <= SCHEMA_VERSION:
            logger.info('Missed migration: {} migration is missed. Migrating...'.format(version))
            module = __import__(modname, fromlist='dummy')

            # run each migration under its own transaction. This allows us to apply valid migrations
            # and break on invalid.
            trans = connection.begin()
            try:
                module.Migration().migrate(connection)
                _update_version(connection, version)
                trans.commit()
            except:
                trans.rollback()
                logger.error("Failed to migrate '{}'  on {} ".format(version, dsn))
                raise


def create_migration_template(name):
    """ Creates migration file. Returns created file name.
    Args:
        name (str): name of the migration.

    Returns:
        str: name of the migration file.
    """
    assert name, 'Name of the migration can not be empty.'
    from . import migrations

    # Find next number
    #
    package = migrations
    prefix = package.__name__ + '.'
    all_versions = []
    for importer, modname, ispkg in pkgutil.iter_modules(package.__path__, prefix):
        version = int(modname.split('.')[-1].split('_')[0])
        all_versions.append(version)

    next_number = max(all_versions) + 1

    # Generate next migration name
    #
    next_migration_name = '{}_{}.py'.format(next_number, name)
    migration_fullname = os.path.join(package.__path__[0], next_migration_name)

    # Write next migration file content.
    #
    with open(migration_fullname, 'w') as f:
        f.write(MIGRATION_TEMPLATE)
    return migration_fullname


def get_stored_version(connection):
    """ Returns database version.

    Raises: Assuming user_version pragma (sqlite case) and user_version table (postgresql case)
        exist because they created with the database creation.

    Args:
        connection (sqlalchemy connection):

    Returns:
        int: version of the database.

    """

    if connection.engine.name == 'sqlite':
        version = connection.execute('PRAGMA user_version').fetchone()[0]
        if version == 0:
            raise VersionIsNotStored
        return version
    elif connection.engine.name == 'postgresql':
        try:
            r = connection\
                .execute('SELECT version FROM {}.user_version;'.format(POSTGRES_SCHEMA_NAME))\
                .fetchone()
            if not r:
                raise VersionIsNotStored

            version = r[0]

        except ProgrammingError:
            # This happens when the user_version table doesn't exist
            raise VersionIsNotStored
        return version
    else:
        raise DatabaseError('Do not know how to get version from {} engine.'.format(connection.engine.name))


def _validate_version(connection, dsn):
    """ Performs on-the-fly schema updates based on the models version.

    Raises:
        DatabaseError: if user uses old sqlite database.

    """
    try:
        version = get_stored_version(connection)
    except VersionIsNotStored:
        logger.debug('Version not stored in the db: assuming new database creation.')
        version = SCHEMA_VERSION
        _update_version(connection, version)
    assert isinstance(version, int)

    if version > 10 and version < 100:
        raise DatabaseError('Trying to open an old SQLite database.')

    if _migration_required(connection):
        migrate(connection, dsn)


def _migration_required(connection):
    """ Returns True if ambry models do not match to db tables. Otherwise returns False. """
    stored_version = get_stored_version(connection)
    actual_version = SCHEMA_VERSION
    assert isinstance(stored_version, int)
    assert isinstance(actual_version, int)
    assert stored_version <= actual_version, 'Db version can not be greater than models version. Update your source code.'
    return stored_version < actual_version


def _update_version(connection, version):
    """ Updates version in the db to the given version.

    Args:
        connection (sqlalchemy connection): sqlalchemy session where to update version.
        version (int): version of the migration.

    """
    if connection.engine.name == 'sqlite':
        connection.execute('PRAGMA user_version = {}'.format(version))
    elif connection.engine.name == 'postgresql':

        connection.execute(DDL('CREATE SCHEMA IF NOT EXISTS {};'.format(POSTGRES_SCHEMA_NAME)))
        connection.execute(DDL('CREATE SCHEMA IF NOT EXISTS {};'.format(POSTGRES_PARTITION_SCHEMA_NAME)))

        connection.execute('CREATE TABLE IF NOT EXISTS {}.user_version(version INTEGER NOT NULL);'
                           .format(POSTGRES_SCHEMA_NAME))

        # upsert.
        if connection.execute('SELECT * FROM {}.user_version;'.format(POSTGRES_SCHEMA_NAME)).fetchone():
            # update
            connection.execute('UPDATE {}.user_version SET version = {};'
                               .format(POSTGRES_SCHEMA_NAME, version))
        else:
            # insert
            connection.execute('INSERT INTO {}.user_version (version) VALUES ({})'
                               .format(POSTGRES_SCHEMA_NAME, version))
    else:
        raise DatabaseMissingError('Do not know how to migrate {} engine.'
                                   .format(connection.engine.driver))


def _is_missed(connection, version):
    """ Returns True if migration is not applied. Otherwise returns False.

    Args:
        connection (sqlalchemy connection): sqlalchemy session to check for migration.
        version (int): version of the migration.

    Returns:
        bool: True if migration is missed, False otherwise.
    """
    return get_stored_version(connection) < version


def _get_all_migrations():
    """ Returns sorted list of all migrations.

    Returns:
        list of (int, str) tuples: first elem of the tuple is migration number, second if module name.

    """
    from . import migrations

    package = migrations
    prefix = package.__name__ + '.'
    all_migrations = []
    for importer, modname, ispkg in pkgutil.iter_modules(package.__path__, prefix):
        version = int(modname.split('.')[-1].split('_')[0])
        all_migrations.append((version, modname))

    all_migrations = sorted(all_migrations, key=lambda x: x[0])
    return all_migrations
