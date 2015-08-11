"""Basic Sqlalchemy database initialization for this ORM

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from collections import namedtuple
import pkgutil
import os

from sqlalchemy.exc import IntegrityError, ProgrammingError, OperationalError
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import create_engine, event

from ambry.orm.exc import DatabaseError, DatabaseMissingError, NotFoundError, ConflictError

from ambry.util import get_logger, parse_url_to_dict
from . import Column, Partition, Table, Dataset, Config, File,\
    Code, ColumnStat, DataSource, SourceColumn, SourceTable

ROOT_CONFIG_NAME = 'd000'
ROOT_CONFIG_NAME_V = 'd000001'

SCHEMA_VERSION = 105

# Database connection information
Dbci = namedtuple('Dbc', 'dsn_template sql')

# Remap the schema
scheme_map = {'postgis': 'postgresql+psycopg2', 'spatialite': 'sqlite'}

MIGRATION_TEMPLATE = '''\
# -*- coding: utf-8 -*-',

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

    def __init__(self, dsn, echo=False, engine_kwargs=None):
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
        self._schema = None
        self._echo = echo

        self.logger = logger

    def is_in_memory_db(self):
        """ Returns True if in memory database is used. Otherwise returns False. """
        return self.dsn == 'sqlite://' or (self.dsn.startswith('sqlite') and 'memory' in self.dsn)

    def create(self):
        """Create the database from the base SQL."""

        if not self.exists():
            self._create_path()
            self.create_tables()
            return True



        return False

    def _create_path(self):
        """Create the path to hold the database, if one wwas specified."""

        if self.driver == 'sqlite' and 'memory' not in self.dsn:

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
        rows = []

        try:
            # Since we are using the connection, rather than the session, need to
            # explicitly set the search path.
            if self.driver in ('postgres', 'postgis') and self._schema:
                self.connection.execute('SET search_path TO {}'.format(self._schema))

            from sqlalchemy.engine.reflection import Inspector

            inspector = Inspector.from_engine(self.engine)

            if not 'config' in inspector.get_table_names():
                return False
            else:
                return True

        except ProgrammingError:
            # This happens when the datasets table doesn't exist
            pass
        except OperationalError:
            # TODO Should check that the error is b/c the datasets table is missing, and reraise if it is for
            # some other reason.
            return False

        finally:
            self.close_connection()


    @property
    def engine(self):
        """return the SqlAlchemy engine for this database."""

        if not self._engine:

            # There appears to be a problem related to connection pooling on Linux + Postgres, where
            # multiprocess runs will throw exceptions when the Datasets table record can't be
            # found. It looks like connections are losing the setting for the search path to the
            # library schema.  Disabling connection pooling solves the problem.
            # from sqlalchemy.pool import NullPool
            # self._engine = create_engine(self.dsn, poolclass=NullPool, echo=False)
            # Easier than constructing the pool
            # self._engine.pool._use_threadlocal = True

            self._engine = create_engine(self.dsn, echo=self._echo,  **self.engine_kwargs)

            if self.driver == 'sqlite':
                event.listen(self._engine, 'connect', _pragma_on_connect)
            _validate_version(self.connection)

        return self._engine

    @property
    def connection(self):
        """Return an SqlAlchemy connection."""
        if not self._connection:
            self._connection = self.engine.connect()

            if self.driver in ['postgres', 'postgis']:
                self._connection.execute('SET search_path TO library')

        return self._connection

    @property
    def session(self):
        """Return a SqlAlchemy session."""
        from sqlalchemy.orm import sessionmaker

        if not self.Session:
            self.Session = sessionmaker(bind=self.engine, expire_on_commit=True)

        if not self._session:
            self._session = self.Session()
            # set the search path

        if self.driver in ('postgres', 'postgis') and self._schema:
            self._session.execute('SET search_path TO {}'.format(self._schema))

        return self._session

    def open(self):
        """ Ensure the database exists and is ready to use. """

        # Creates the session
        self.session

        if not self.exists():
            self.create()

    def close(self):

        self.close_session()
        self.close_connection()

        if self._engine:
            self._engine.dispose()

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
        self.session.commit()

    def rollback(self):
        self.session.rollback()
        # self.close_session()

    def clean(self):

        for ds in self.datasets:
            self.logger.info('Cleaning: {}'.format(ds.name))
            self.remove_dataset(ds)

        self.remove_dataset(self.root_dataset)

        self.create()

        self.commit()

    def drop(self):

        # Should close connection before table drop to avoid hanging in postgres.
        # http://docs.sqlalchemy.org/en/rel_0_8/faq.html#metadata-schema

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

        self.metadata.drop_all()

        self.commit()

        self.create()

        self.commit()

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

    def create_tables(self):

        from sqlalchemy.exc import OperationalError

        tables = [
            Dataset, Config, Table, Column, Partition, File, Code,
            ColumnStat, SourceTable, SourceColumn, DataSource]

        try:
            self.drop()
        except (OperationalError, ProgrammingError):
            pass

        orig_schemas = {}

        for table in tables:
            try:
                it = table.__table__
                # stored_partitions, file_link are already tables.
            except AttributeError:
                it = table

            # These schema shenanigans are almost certainly wrong.
            # But they are expedient. For Postgres, it puts the library
            # tables in the Library schema.
            if self._schema:
                orig_schemas[it] = it.schema
                it.schema = self._schema

            it.create(bind=self.engine)
            self.commit()

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

    def dataset(self, ref, load_all=False):
        """Return a dataset, given a vid or id

        :param ref: Vid or id  for a dataset. If an id is provided, will it will return the one with the
        largest revision number
        :param load_all: Use a query that eagerly loads everything.
        :return: :class:`ambry.orm.Dataset`

        """

        try:
            ds = self.session.query(Dataset).filter(Dataset.vid == str(ref)).one()
        except NoResultFound:
            try:
                ds = self.session.query(Dataset).filter(Dataset.id == str(ref))\
                    .order_by(Dataset.revision.desc())\
                    .first()
            except NoResultFound:
                raise NotFoundError('No partition in library for vid : {} '.format(ref))

        if ds:
            ds._database = self

        return ds

    @property
    def datasets(self):
        """
        Return all datasets

        :return:
        """

        return self.session.query(Dataset).filter(Dataset.vid != ROOT_CONFIG_NAME_V).all()

    def remove_dataset(self, ds):

        if ds:
            self.session.delete(ds)
            self.session.commit()

    def copy_dataset(self, ds):
        from ..util import toposort

        ds.commit()

        # Make sure everything we want to copy is loaded
        ds.tables
        ds.partitions
        ds.files
        #ds.configs
        ds.stats
        ds.codes
        ds.source_tables
        ds.source_columns

        # Put the partitions in dependency order so the merge won't throw a Foreign key integrity error
        # The non-segment partitions go first, then the segments.
        ds.partitions = [ p for p in ds.partitions if not p.is_segment ] + [ p for p in ds.partitions if p.is_segment ]

        self.session.merge(ds)

        # FIXME: Oh, this is horrible. Sqlalchemy inserts all of the configs as a group, but they are self-referential,
        # so some with a reference to a parent get inserted before their parent. The topo sort solives this,
        # but there must be a better way to do it.
        
        dag = { c.id:set([c.parent_id]) for c in ds.configs}

        refs = { c.id:c for c in ds.configs }

        for e in toposort(dag):
            for ref in e:
                if ref:
                    self.session.merge(refs[ref])

        self.session.commit()

        return self.dataset(ds.vid)


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


def migrate(connection):
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

    #
    # Find next number
    #
    package = migrations
    prefix = package.__name__ + '.'
    all_versions = []
    for importer, modname, ispkg in pkgutil.iter_modules(package.__path__, prefix):
        version = int(modname.split('.')[-1].split('_')[0])
        all_versions.append(version)

    next_number = max(all_versions) + 1

    #
    # Generate next migration name
    #
    next_migration_name = '{}_{}.py'.format(next_number, name)
    migration_fullname = os.path.join(package.__path__[0], next_migration_name)

    #
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
            version = connection.execute('SELECT version FROM user_version;').fetchone()[0]
        except ProgrammingError:
            # This happens when the user_version table doesn't exist
            raise VersionIsNotStored
        return version
    else:
        raise DatabaseMissingError(
            'Do not know how to get version from {} engine.'.format(connection.engine.name))


def _pragma_on_connect(dbapi_con, con_record):
    """ISSUE some Sqlite pragmas when the connection is created."""

    # dbapi_con.execute('PRAGMA foreign_keys = ON;')
    # Not clear that there is a performance improvement.

    dbapi_con.execute('PRAGMA journal_mode = WAL')
    dbapi_con.execute('PRAGMA synchronous = OFF')
    dbapi_con.execute('PRAGMA temp_store = MEMORY')
    dbapi_con.execute('PRAGMA cache_size = 500000')
    dbapi_con.execute('pragma foreign_keys=ON')


def _on_connect_bundle(dbapi_con, con_record):
    """ISSUE some Sqlite pragmas when the connection is created.

    Bundles have different parameters because they are more likely to be
    accessed concurrently.

    """

    # NOTE ABOUT journal_mode = WAL: it improves concurrency, but has some downsides.
    # See http://sqlite.org/wal.html

    try:
        # Can't change journal mode in a transaction.
        dbapi_con.execute('COMMIT')
    except:
        pass

    try:
        dbapi_con.execute('PRAGMA journal_mode = WAL')
        dbapi_con.execute('PRAGMA page_size = 8192')
        dbapi_con.execute('PRAGMA temp_store = MEMORY')
        dbapi_con.execute('PRAGMA cache_size = 50000')
        dbapi_con.execute('PRAGMA foreign_keys = OFF')
    except Exception:
        raise

    # dbapi_con.execute('PRAGMA busy_timeout = 10000')
    # dbapi_con.execute('PRAGMA synchronous = OFF')


def _validate_version(connection):
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
        migrate(connection)


def _migration_required(connection):
    """ Returns True if ambry models do not match to db tables. Otherwise returns False. """
    stored_version = get_stored_version(connection)
    actual_version = SCHEMA_VERSION
    assert isinstance(stored_version, int)
    assert isinstance(actual_version, int)
    assert stored_version <= actual_version, 'Db version can not be more than models version. Update your source code.'
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
        connection.execute('CREATE TABLE IF NOT EXISTS user_version(version INTEGER NOT NULL);')

        # upsert.
        if connection.execute('SELECT * FROM user_version;').fetchone():
            # update
            connection.execute('UPDATE user_version SET version = {};'.format(version))
        else:
            # insert
            connection.execute('INSERT INTO user_version (version) VALUES ({})'.format(version))
    else:
        raise DatabaseMissingError('Do not know how to migrate {} engine.'.format(connection.engine.driver))


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
