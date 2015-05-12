"""A Library is a local collection of bundles.

It holds a database for the configuration of the bundles that have been
installed into it.

"""

# Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt


import os.path

import ambry
import ambry.util
from ambry.util import temp_file_name
from ..identity import  Identity
from ambry.orm import Column, Partition, Table, Dataset, Config, File,  Code, ColumnStat

from collections import namedtuple
from sqlalchemy.exc import IntegrityError


ROOT_CONFIG_NAME = 'd000'
ROOT_CONFIG_NAME_V = 'd000001'


class LibraryDb(object):

    """Represents the Sqlite database that holds metadata for all installed
    bundles."""

    # Database connection information
    Dbci = namedtuple('Dbc', 'dsn_template sql')

    DBCI = {
        'postgis': Dbci(dsn_template='postgresql+psycopg2://{user}:{password}@{server}{colon_port}/{name}', sql='support/configuration-pg.sql'),
        'postgres': Dbci(dsn_template='postgresql+psycopg2://{user}:{password}@{server}{colon_port}/{name}', sql='support/configuration-pg.sql'),  # Stored in the ambry module.
        'sqlite': Dbci(dsn_template='sqlite:///{name}', sql='support/configuration-sqlite.sql'),
        'spatialite': Dbci(dsn_template='sqlite:///{name}', sql='support/configuration-sqlite.sql'),
        'mysql': Dbci(dsn_template='mysql://{user}:{password}@{server}{colon_port}/{name}', sql='support/configuration-sqlite.sql')
    }

    def __init__(self, driver=None, server=None, dbname=None,
                 username=None, password=None, port=None, **kwargs):
        self.driver = driver
        self.server = server
        self.dbname = dbname
        self.username = username
        self.password = password

        if port:
            self.colon_port = ':' + str(port)
        else:
            self.colon_port = ''

        self.dsn_template = self.DBCI[self.driver].dsn_template
        self.dsn = self.dsn_template.format(
            user=self.username,
            password=self.password,
            server=self.server,
            name=self.dbname,
            colon_port=self.colon_port)

        self.Session = None
        self._session = None
        self._engine = None
        self._connection = None

        self._partition_collection = []

        if self.driver in ['postgres', 'postgis']:
            self._schema = 'library'
        else:
            self._schema = None

        self.logger = ambry.util.get_logger(__name__)
        import logging
        self.logger.setLevel(logging.INFO)

        self.enable_delete = False

    ##
    # Sqlalchemy connection, engine, session, metadata
    ##

    @property
    def engine(self):
        """return the SqlAlchemy engine for this database."""
        from sqlalchemy import create_engine
        from ..database.sqlite import _on_connect_update_sqlite_schema
        from sqlalchemy.pool import AssertionPool
        from sqlalchemy.pool import NullPool

        if not self._engine:

            # There appears to be a problem related to connection pooling on Linux + Postgres, where
            # multiprocess runs will throw exceptions when the Datasets table record can't be
            # found. It looks like connections are losing the setting for the search path to the
            # library schema.
            # Disabling connection pooling solves the problem.
            self._engine = create_engine( self.dsn,poolclass=NullPool, echo=False)

            # Easier than constructing the pool
            self._engine.pool._use_threadlocal = True

            from sqlalchemy import event

            if self.driver == 'sqlite':
                event.listen(self._engine, 'connect', _pragma_on_connect)
                #event.listen(self._engine, 'connect', _on_connect_update_schema)
                _on_connect_update_sqlite_schema(self.connection, None)

        return self._engine

    @property
    def connection(self):
        """Return an SqlAlchemy connection."""
        if not self._connection:
            self._connection = self.engine.connect()

            if self.driver in ['postgres', 'postgis']:
                self._connection.execute("SET search_path TO library")


        return self._connection

    @property
    def session(self):
        """Return a SqlAlchemy session."""
        from sqlalchemy.orm import sessionmaker

        if not self.Session:
            self.Session = sessionmaker(bind=self.engine, expire_on_commit = False)

        if not self._session:
            self._session = self.Session()
            # set the search path

        if self.driver in ('postgres', 'postgis') and self._schema:
            self._session.execute("SET search_path TO {}".format(self._schema))


        return self._session

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
        try:
            self.session.commit()
            self.session.expunge_all() # Clear any cached object in the session.
            self.session.expire_all()
            # self.close_session()
        except Exception as e:
            #self.logger.error("Failed to commit in {}; {}".format(self.dsn, e))
            raise

    def rollback(self):
        self.session.rollback()
        # self.close_session()

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

    ##
    ## Creation and Existence
    ##

    def exists(self):
        from sqlalchemy.exc import ProgrammingError, OperationalError

        if self.driver == 'sqlite' and not os.path.exists(self.dbname):
            return False

        self.engine

        try:
            try:
            # Since we are using the connection, rather than the session, need to
            # explicitly set the search path.
                if self.driver in ('postgres', 'postgis') and self._schema:
                    self.connection.execute("SET search_path TO {}".format( self._schema))

                rows = self.connection.execute(
                    "SELECT * FROM datasets WHERE d_vid = '{}' "
                    .format(ROOT_CONFIG_NAME_V)).fetchone()

            except ProgrammingError as e:
                # This happens when the datasets table doesnt exist
                rows = False

            if not rows:
                return False
            else:
                return True
        except Exception as e:
            # What is the more specific exception here?

            return False
        finally:
            self.close_connection()

    def clean(self, add_config_root=True):
        from sqlalchemy.exc import OperationalError, IntegrityError
        from ..dbexceptions import DatabaseError

        s = self.session

        try:
            s.query(Config).delete()
            s.query(ColumnStat).delete()
            s.query(File).delete()
            s.query(Code).delete()
            s.query(Column).delete()
            s.query(Table).update({Table.p_vid: None}) # Prob should be handled with a cascade on relationship. 
            s.query(Partition).delete()
            s.query(Table).delete()
            s.query(Dataset).delete()
        except (OperationalError, IntegrityError) as e:
            # Tables dont exist?
            raise DatabaseError("Failed to data records from {}: {}".format(self.dsn, str(e)))

        if add_config_root:
            self._add_config_root()

        self.commit()

    def create(self):
        """Create the database from the base SQL."""

        if not self.exists():

            self._create_path()

            self.enable_delete = True

            self.create_tables()
            self._add_config_root()

            return True

        return False

    def _create_path(self):
        """Create the path to hold the database, if one wwas specified."""

        if self.driver == 'sqlite':

            dir_ = os.path.dirname(self.dbname)

            if dir_ and not os.path.exists(dir_):
                try:
                    # MUltiple process may try to make, so it could already
                    # exist
                    os.makedirs(dir_)
                except Exception as e:  # @UnusedVariable
                    pass

                if not os.path.exists(dir_):
                    raise Exception("Couldn't create directory " + dir_)

    def drop(self):
        from sqlalchemy.exc import NoSuchTableError
        if not self.enable_delete:
            raise Exception("Deleting not enabled. Set library.database.enable_delete = True")


        library_tables = [
            Config.__table__,
            ColumnStat.__table__,
            Column.__table__,
            Code.__table__,
            Partition.__table__,
            Table.__table__,
            File.__table__,
            Dataset.__table__,
            ]

        try:
            db_tables = reversed(self.metadata.sorted_tables)
        except NoSuchTableError:
            # Deleted the tables out from under it, so we're done.
            return

        for table in library_tables:
            table.drop(self.engine, checkfirst=True)

        self.commit()

    def __del__(self):
        pass

    def clone(self):
        return self.__class__(self.driver,self.server,self.dbname,self.username,self.password)

    def create_tables(self):
        from sqlalchemy.exc import OperationalError
        tables = [ Dataset,Config,Table,Column,File,Partition,Code,ColumnStat]

        try:
            self.drop()
        except OperationalError:
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

            for it, orig_schema in orig_schemas.items():
                it.schema = orig_schema

    def _add_config_root(self):
        from sqlalchemy.orm.exc import NoResultFound

        try:

            self.session.query(Dataset).filter(
                Dataset.vid == ROOT_CONFIG_NAME).one()
            self.close_session()
        except NoResultFound:
            o = Dataset(
                id=ROOT_CONFIG_NAME,
                name=ROOT_CONFIG_NAME,
                vname=ROOT_CONFIG_NAME_V,
                fqname='datasetroot-0.0.0~' + ROOT_CONFIG_NAME_V,
                cache_key=ROOT_CONFIG_NAME,
                version='0.0.0',
                source=ROOT_CONFIG_NAME,
                dataset=ROOT_CONFIG_NAME,
                creator=ROOT_CONFIG_NAME,
                revision=1,
            )
            self.session.add(o)
            self.commit()

    def _clean_config_root(self):
        """Hack need to clean up some installed databases."""

        ds = self.session.query(Dataset).filter(
            Dataset.id_ == ROOT_CONFIG_NAME).one()

        ds.id_ = ROOT_CONFIG_NAME
        ds.name = ROOT_CONFIG_NAME
        ds.vname = ROOT_CONFIG_NAME_V
        ds.source = ROOT_CONFIG_NAME
        ds.dataset = ROOT_CONFIG_NAME
        ds.creator = ROOT_CONFIG_NAME
        ds.revision = 1

        self.session.merge(ds)
        self.commit()

    def inserter(self, table_name, **kwargs):
        from ..database.inserter import ValueInserter
        from sqlalchemy.schema import Table

        table = Table(
            table_name,
            self.metadata,
            autoload=True,
            autoload_with=self.engine)

        return ValueInserter(self, None, table, **kwargs)

    ##
    # Configuration values
    ##

    def set_config_value(self, group, key, value):
        """Set a configuration value in the database."""
        from ambry.orm import Config as SAConfig
        from sqlalchemy.exc import IntegrityError, ProgrammingError
        from sqlalchemy.orm.exc import NoResultFound

        s = self.session

        try:
            o = s.query(SAConfig).filter(
                SAConfig.group == group,
                SAConfig.key == key,
                SAConfig.d_vid == ROOT_CONFIG_NAME_V).one()

        except NoResultFound:
            o = SAConfig(
                group=group,
                key=key,
                d_vid=ROOT_CONFIG_NAME_V,
                value=value)

        o.value = value
        s.merge(o)

        self.commit()

    def get_config_value(self, group, key):

        from ambry.orm import Config as SAConfig

        s = self.session

        try:

            c = s.query(SAConfig).filter(
                SAConfig.group == group,
                SAConfig.key == key,
                SAConfig.d_vid == ROOT_CONFIG_NAME_V).first()

            return c
        except:
            return None

    def get_config_group(self, group, d_vid=ROOT_CONFIG_NAME_V):

        from ambry.orm import Config as SAConfig

        s = self.session

        try:
            d = {}
            for row in s.query(SAConfig).filter(SAConfig.group == group, SAConfig.d_vid == d_vid).all():
                d[row.key] = row.value
            return d

        except:
            return {}

    def get_config_rows(self, d_vid):
        """Return configuration in a form that can be used to reconstitute a
        Metadataobject Returns all of the rows for a dataset.

        This is distinct from get_config_value, which returns the value
        for the library.

        """
        from ambry.orm import Config as SAConfig
        from sqlalchemy import or_

        rows = []

        for r in self.session.query(SAConfig).filter(or_(SAConfig.group == 'config', SAConfig.group == 'process'),
                                                     SAConfig.d_vid == d_vid).all():

            parts = r.key.split('.', 3)

            if r.group == 'process':
                parts = ['process'] + parts

            cr = ((parts[0] if len(parts) > 0 else None,
                   parts[1] if len(parts) > 1 else None,
                   parts[2] if len(parts) > 2 else None
                   ), r.value)

            rows.append(cr)

        return rows

    def get_bundle_value(self, dvid, group, key):

        from ambry.orm import Config as SAConfig

        s = self.session

        try:
            c = s.query(SAConfig).filter(SAConfig.group == group,
                                         SAConfig.key == key,
                                         SAConfig.d_vid == dvid).first()

            return c.value
        except:
            return None

    def get_bundle_values(self, dvid, group):
        """Get an entire group of bundle values."""

        from ambry.orm import Config as SAConfig

        s = self.session

        try:
            return s.query(SAConfig).filter(
                SAConfig.group == group,
                SAConfig.d_vid == dvid).all()
        except:
            return None

    @property
    def config_values(self):

        from ambry.orm import Config as SAConfig

        s = self.session

        d = {}

        for config in s.query(SAConfig).filter(SAConfig.d_vid == ROOT_CONFIG_NAME_V).all():
            d[(str(config.group), str(config.key))] = config.value

        return d

    def _mark_update(self, o=None, vid=None):

        import datetime

        self.set_config_value('activity','change',datetime.datetime.utcnow().isoformat())

    ##
    # Install and remove bundles and partitions
    ##

    def install_dataset_identity(self, identity, data={}, overwrite=True):
        """Create the record for the dataset.

        Does not add an File objects

        """
        from sqlalchemy.exc import IntegrityError
        from ..dbexceptions import ConflictError

        ds = Dataset(**identity.dict)
        ds.name = identity.sname
        ds.vname = identity.vname
        ds.fqname = identity.fqname
        ds.cache_key = identity.cache_key
        ds.creator = 'N/A'
        ds.data = data

        try:
            self.session.merge(ds)
            self.commit()
        except IntegrityError as e:
            self.session.rollback()

            if not overwrite:
                return

            try:
                self.session.merge(ds)
                self.commit()

            except IntegrityError as e:
                raise ConflictError("Can't install dataset vid={}; \nOne already exists. ('{}');\n {}" .format(
                        identity.vid,e.message,ds.dict))

    def install_bundle_dataset(self, bundle):
        """Install only the dataset record for the bundle"""

        from sqlalchemy.orm import joinedload, noload

        if self.session.query(Dataset).filter(Dataset.vid == str(bundle.identity.vid)).first():
            return False

        dataset = (bundle.database.session.query(Dataset).options(noload('*'), joinedload('configs'))
                   .filter(Dataset.vid == str(bundle.identity.vid)).one() )

        self.session.merge(dataset)

        for cfg in dataset.configs:
            self.session.merge(cfg)

        self.session.commit()

        return dataset


    def install_bundle(self, bundle):
        """Copy the schema and partitions lists into the library database."""

        from sqlalchemy.orm import joinedload, noload

        if self.session.query(Dataset).filter(Dataset.vid == str(bundle.identity.vid) ).first():
            return False

        dataset = self.install_bundle_dataset(bundle)

        d_vid = dataset.vid

        # This is a lot faster than going through the ORM.
        for tbl in [Table, Column, Code, Partition, ColumnStat]:

            rows = [dict(r.items()) for r in bundle.database.session.execute(tbl.__table__.select()) ]

            # There were recent schema updates that add a d_vid to every object, but these will be null
            # in old bundles, so we need to set the value manually.
            if tbl == Column or tbl == ColumnStat or tbl == Code:
                for r in rows:
                    for k,v in r.items():
                        if k.endswith('_d_vid') and not bool(v):
                            r[k] = d_vid
            if rows:
                self.session.execute(tbl.__table__.insert(), rows)

            self.session.commit()

        self._mark_update()

        return dataset

    def mark_table_installed(self, table_or_vid, name=None):
        """Mark a table record as installed."""

        s = self.session
        table = None

        table = s.query(Table).filter(Table.vid == table_or_vid).one()

        if not table:
            table = s.query(Table).filter(Table.name == table.vid).one()

        if not name:
            name = table.name

        table.installed = 'y'

        s.merge(table)
        s.commit()

    def mark_partition_installed(self, p_vid):
        """Mark a table record as installed."""

        s = self.session
        table = None

        p = s.query(Partition).filter(Partition.vid == p_vid).one()

        p.installed = 'y'

        s.merge(p)
        s.commit()

    def remove_bundle(self, bundle):
        """remove a bundle from the database."""
        from ..orm import Dataset
        from ..bundle import LibraryDbBundle

        try:
            dataset, partition = self.get_id( bundle.identity.vid)
        except AttributeError:
            dataset, partition = bundle, None

        if not dataset:
            return False

        dataset =  self.session.query(Dataset).filter( Dataset.vid == dataset.identity.vid).one()

        self.session.delete(dataset)

        self.commit()


    def delete_dataset_colstats(self, dvid):
        """Total hack to deal with not being able to get delete cascades to
        work for colstats.

        :param vid: dataset vid
        :return:

        """
        s = self.session

        # Get the partitions for the dataset
        part_query = s.query(Partition.vid).filter(Partition.d_vid == dvid)

        # Delete those colstats that reference the partitions.
        s.query(ColumnStat).filter(ColumnStat.p_vid.in_(part_query.subquery())).delete(synchronize_session='fetch')


    def remove_dataset(self, vid):
        """Remove all references to a Dataset."""
        from ..orm import Dataset, ColumnStat

        dataset = (self.session.query(Dataset).filter(Dataset.vid == vid).one())

        # Total hack to avoid having to figure out cascades between partitions
        # and colstats
        self.delete_dataset_colstats(dataset.vid)

        # Can't use delete() on the query -- bulk delete queries do not
        # trigger in-python cascades!
        self.session.delete(dataset)


        self.session.commit()

    def remove_partition(self, partition):
        from ..bundle import LibraryDbBundle
        from ..orm import Partition

        try:
            dataset = self.get(partition.identity.vid)  # @UnusedVariable
            p_vid = partition.identity.vid
        except AttributeError:
            # It is actually an identity, we hope
            dataset = partition.as_dataset()
            p_vid = partition.vid

        b = LibraryDbBundle(self, dataset.vid)

        s = self.session

        # TODO: Probably need to manually delete colstats.

        s.query(Partition).filter(Partition.t_vid == p_vid).delete()

        self.commit()

    def remove_partition_record(self, vid):
        from ..orm import ColumnStat

        s = self.session

        # FIXME: The Columstat delete should be cascaded, but I really don't
        # understand cascading.
        s.query(ColumnStat).filter(ColumnStat.p_vid == vid).delete()
        s.query(Partition).filter(Partition.vid == vid).delete()

        s.commit()

    ##
    # Get objects by reference, or resolve a reference
    ##

    def get(self, vid):
        """Get an identity by a vid.

        For partitions, returns a nested Identity

        """
        from ..identity import ObjectNumber, DatasetNumber, PartitionNumber
        from ..orm import Dataset, Partition
        from sqlalchemy.orm.exc import NoResultFound
        from ..dbexceptions import NotFoundError

        try:
            if isinstance(vid, basestring):
                vid = ObjectNumber.parse(vid)

            if isinstance(vid, DatasetNumber):
                d = (self.session.query(Dataset)
                     .filter(Dataset.vid == str(vid)).one())
                did = d.identity

            elif isinstance(vid, PartitionNumber):
                d, p = (self.session.query(Dataset, Partition).join(Partition)
                        .filter(Partition.vid == str(vid)).one())
                did = d.identity
                did.add_partition(p.identity)

            else:
                raise ValueError('vid was wrong type: {}'.format(type(vid)))

            return did
        except NoResultFound:
            raise NotFoundError("No object found for vid {}".format(vid))

    def get_table(self, table_vid):

        s = self.session

        return s.query(Table).filter(Table.vid == table_vid).one()

    def tables(self):

        s = self.session

        out = []

        for t in s.query(Table).all():
            out[t.name] = t.dict

        return out

    def list(self, datasets=None, with_partitions=False, key='vid'):
        """
        :param datasets: If specified, must be a dict, which the internal dataset data will be
        put into.
        :return: vnames of the datasets in the library.
        """

        from ..orm import Dataset, Partition, File
        from .files import Files
        from sqlalchemy.sql import or_

        if datasets is None:
            datasets = {}

        q1 = (self.session.query(Dataset, Partition, File)
              .join(Partition)
              .outerjoin(File, File.ref == Partition.vid)
              .filter(Dataset.vid != ROOT_CONFIG_NAME_V))

        q2 = (self.session.query(Dataset, File)
              .outerjoin(File, File.ref == Dataset.vid)
              .filter(Dataset.vid != ROOT_CONFIG_NAME_V))

        entries = [(d, None, f) for d, f in q2.all()]

        if with_partitions:
            entries += q1.all()

        for d, p, f in entries:

            ck = getattr(d.identity, key)

            if ck not in datasets:
                datasets[ck] = d.identity
                datasets[ck].summary = self.get_bundle_value(
                    d.vid,'config','about.title')

            # Adding the file to the identity gets us the bundle state and
            # modification time.
            if f:
                if not p:

                    datasets[ck].add_file(f)
                    datasets[ck].bundle_state = f.state if (
                        f.state and not datasets[ck].bundle_state) else datasets[ck].bundle_state

                else:
                    p.identity.add_file(f)

            if p and (not datasets[ck].partitions or p.vid not in datasets[ck].partitions):
                datasets[ck].add_partition(p.identity)

        return datasets

    def all_vids(self):

        all = set()

        q = (self.session.query(Dataset,Partition).join(Partition).filter(Dataset.vid != ROOT_CONFIG_NAME_V))

        for row in q.all():
            all.add(row.Dataset.vid)
            all.add(row.Partition.vid)

        return all

    def datasets(self, key='vid'):
        """List only the dataset records."""

        from ..orm import Dataset

        datasets = {}

        for d in (self.session.query(Dataset)
                  .filter(Dataset.location == Dataset.LOCATION.LIBRARY)
                  .filter(Dataset.vid != ROOT_CONFIG_NAME_V).all()):

            ck = getattr(d.identity, key)
            datasets[ck] = d.identity

        return datasets

    @property
    def resolver(self):
        from .query import Resolver
        return Resolver(self.session)





def _pragma_on_connect(dbapi_con, con_record):
    """ISSUE some Sqlite pragmas when the connection is created."""

    #dbapi_con.execute('PRAGMA foreign_keys = ON;')
    # Not clear that there is a performance improvement.

    dbapi_con.execute('PRAGMA journal_mode = WAL')
    dbapi_con.execute('PRAGMA synchronous = OFF')
    dbapi_con.execute('PRAGMA temp_store = MEMORY')
    dbapi_con.execute('PRAGMA cache_size = 500000')
    dbapi_con.execute('pragma foreign_keys=ON')
