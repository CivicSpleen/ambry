"""A Library is a local collection of bundles. It holds a database for the configuration
of the bundles that have been installed into it. 
"""

# Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt


import os.path

import ambry
import ambry.util
from ambry.util import temp_file_name
from ambry.bundle import DbBundle
from ..identity import LocationRef

from collections import namedtuple
from sqlalchemy.exc import IntegrityError, ProgrammingError, OperationalError


ROOT_CONFIG_NAME = 'd000'
ROOT_CONFIG_NAME_V = 'd000001'


class LibraryDb(object):
    '''Represents the Sqlite database that holds metadata for all installed bundles'''

    Dbci = namedtuple('Dbc', 'dsn_template sql') #Database connection information

    DBCI = {
            'postgis':Dbci(dsn_template='postgresql+psycopg2://{user}:{password}@{server}{colon_port}/{name}',sql='support/configuration-pg.sql'),
            'postgres':Dbci(dsn_template='postgresql+psycopg2://{user}:{password}@{server}{colon_port}/{name}',sql='support/configuration-pg.sql'), # Stored in the ambry module.
            'sqlite':Dbci(dsn_template='sqlite:///{name}',sql='support/configuration-sqlite.sql'),
            'mysql':Dbci(dsn_template='mysql://{user}:{password}@{server}{colon_port}/{name}',sql='support/configuration-sqlite.sql')
            }

    def __init__(self,  driver=None, server=None, dbname = None,
                 username=None, password=None, port=None, **kwargs):
        self.driver = driver
        self.server = server
        self.dbname = dbname
        self.username = username
        self.password = password

        if port:
            self.colon_port = ':'+str(port)
        else:
            self.colon_port = ''

        self.dsn_template = self.DBCI[self.driver].dsn_template
        self.dsn = None

        self.Session = None
        self._session = None
        self._engine = None
        self._connection  = None

        if self.driver in ['postgres','postgis']:
            self._schema = 'library'
        else:
            self._schema = None


        self.logger = ambry.util.get_logger(__name__)
        import logging
        self.logger.setLevel(logging.INFO)

        self.enable_delete = False


    ##
    ## Sqlalchemy connection, engine, session, metadata
    ##

    @property
    def engine(self):
        '''return the SqlAlchemy engine for this database'''
        from sqlalchemy import create_engine
        from ..database.sqlite import _on_connect_update_sqlite_schema
        from sqlalchemy.pool import AssertionPool
        from sqlalchemy.pool import NullPool

        if not self._engine:

            self.dsn = self.dsn_template.format(user=self.username, password=self.password,
                            server=self.server, name=self.dbname, colon_port=self.colon_port)

            #print "Create Engine",os.getpid(), self.dsn

            # There appears to be a problem related to connection pooling on Linux + Postgres, where
            # multiprocess runs will throw exceptions when the Datasets table record can't be
            # found. It looks like connections are losing the setting for the search path to the
            # library schema.
            # Disabling connection pooling solves the problem.
            self._engine = create_engine(self.dsn,echo=False, poolclass=NullPool)

            from sqlalchemy import event

            if self.driver == 'sqlite':
                event.listen(self._engine, 'connect', _pragma_on_connect)
                #event.listen(self._engine, 'connect', _on_connect_update_schema)
                _on_connect_update_sqlite_schema(self.connection)

        return self._engine

    @property
    def connection(self):
        '''Return an SqlAlchemy connection'''
        if not self._connection:
            self._connection = self.engine.connect()

        return self._connection

    def close_connection(self):
        if self._connection:
            self._connection.close()
            self._connection = None

    @property
    def session(self):
        '''Return a SqlAlchemy session'''
        from sqlalchemy.orm import sessionmaker

        if not self.Session:
            # expire_on_commit=False prevents DetatchedInstanceErrors when
            # using database object outside the database.
            self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

        if not self._session:
            self._session = self.Session()
            # set the search path
            if self.driver in ('postgres','postgis') and self._schema:
                self._session.execute("SET search_path TO {}".format(self._schema))

        return self._session


    @property
    def metadata(self):
        '''Return an SqlAlchemy MetaData object, bound to the engine'''

        from sqlalchemy import MetaData
        metadata = MetaData(bind=self.engine, schema = self._schema)

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
        from ambry.orm import Dataset
        from sqlalchemy.exc import  ProgrammingError, OperationalError

        if self.driver == 'sqlite' and not os.path.exists(self.dbname):
                return False

        self.engine

        try:
            try:
                # Since we are using the connection, rather than the session, need to
                # explicitly set the search path.
                if self.driver in ('postgres','postgis') and self._schema:
                    self.connection.execute("SET search_path TO {}".format(self._schema))

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
        s = self.session
        from ambry.orm import Column, Partition, Table, Dataset, Config, File

        s.query(Config).delete()
        s.query(File).delete()
        s.query(Column).delete()
        s.query(Partition).delete()
        s.query(Table).delete()
        s.query(Dataset).delete()

        if add_config_root:
            self._add_config_root()

        self.commit()

    def create(self):
        """Create the database from the base SQL"""

        if not self.exists():
            self.enable_delete = True
            self.create_tables()
            self._add_config_root()

            return True

        return False

    def _drop(self, s):

        if not self.enable_delete:
            raise Exception("Deleting not enabled. Set library.database.enable_delete = True")

        for table in reversed(self.metadata.sorted_tables): # sorted by foreign key dependency
            table.drop(self.engine, checkfirst=True)

    def drop(self):
        s = self.session

        self._drop(s)
        s.commit()

    def __del__(self):
        pass # print  'closing LibraryDb'

    def clone(self):
        return self.__class__(self.driver, self.server, self.dbname, self.username, self.password)

    def create_tables(self):
        from ambry.orm import  Dataset, Partition, Table, Column, File, Config

        if self.driver == 'sqlite':

            dir_ = os.path.dirname(self.dbname)
            if not os.path.exists(dir_):
                try:
                    os.makedirs(dir_) # MUltiple process may try to make, so it could already exist
                except Exception as e: #@UnusedVariable
                    pass

                if not os.path.exists(dir_):
                    raise Exception("Couldn't create directory "+dir_)

        tables = [ Dataset, Config, Table, Column, File, Partition]

        self.drop()

        orig_schemas = {}

        for table in tables:
            it = table.__table__

            # These schema shenanigans are almost certainly wrong.
            # But they are expedient
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
        from ambry.orm import Dataset
        from sqlalchemy.orm.exc import NoResultFound

        try:
            self.session.query(Dataset).filter(Dataset.vid==ROOT_CONFIG_NAME).one()
            self.close_session()
        except NoResultFound:
            o = Dataset(
                        id=ROOT_CONFIG_NAME,
                        name=ROOT_CONFIG_NAME,
                        vname=ROOT_CONFIG_NAME_V,
                        fqname='datasetroot-0.0.0~'+ROOT_CONFIG_NAME_V,
                        cache_key=ROOT_CONFIG_NAME,
                        version='0.0.0',
                        source=ROOT_CONFIG_NAME,
                        dataset = ROOT_CONFIG_NAME,
                        creator=ROOT_CONFIG_NAME,
                        revision=1,
                        )
            self.session.add(o)
            self.commit()

    def _clean_config_root(self):
        '''Hack need to clean up some installed databases'''
        from ambry.orm import Dataset

        ds = self.session.query(Dataset).filter(Dataset.id_==ROOT_CONFIG_NAME).one()

        ds.id_=ROOT_CONFIG_NAME
        ds.name=ROOT_CONFIG_NAME
        ds.vname=ROOT_CONFIG_NAME_V
        ds.source=ROOT_CONFIG_NAME
        ds.dataset = ROOT_CONFIG_NAME
        ds.creator=ROOT_CONFIG_NAME
        ds.revision=1

        self.session.merge(ds)
        self.commit()

    def inserter(self,table_name, **kwargs):
        from ..database.inserter import ValueInserter
        from sqlalchemy.schema import Table

        table = Table(table_name, self.metadata, autoload=True, autoload_with=self.engine)

        return ValueInserter(self, None, table , **kwargs)

    ##
    ##
    ##

    def close(self):
        #print "Close Engine",os.getpid(), self.dsn
        self.close_session()
        self.close_connection()

        self.engine.dispose()
        self._engine = None

    def commit(self):
        self.session.commit()
        self.close_session()

    def rollback(self):
        self.session.rollback()
        self.close_session()

    def close_session(self):
        if self._session:
            self._session.close()
            #self._session.bind.dispose()
            self._session = None


    ##
    ## Configuration values
    ##


    def set_config_value(self, group, key, value):
        '''Set a configuration value in the database'''
        from ambry.orm import Config as SAConfig
        from sqlalchemy.exc import IntegrityError, ProgrammingError

        s = self.session

        s.query(SAConfig).filter(SAConfig.group == group,
                                 SAConfig.key == key,
                                 SAConfig.d_vid == ROOT_CONFIG_NAME_V).delete()


        try:
            o = SAConfig(group=group,key=key,d_vid=ROOT_CONFIG_NAME_V,value = value)
            s.add(o)
            self.commit()
        except IntegrityError:
            self.rollback()
            o = s.query(SAConfig).filter(SAConfig.group == group,
                                 SAConfig.key == key,
                                 SAConfig.d_vid == ROOT_CONFIG_NAME_V).one()

            o.value = value
            s.merge(o)
            self.commit()

    def get_config_value(self, group, key):

        from ambry.orm import Config as SAConfig

        s = self.session

        try:
            c = s.query(SAConfig).filter(SAConfig.group == group,
                                     SAConfig.key == key,
                                     SAConfig.d_vid == ROOT_CONFIG_NAME_V).first()

            return c
        except:
            return None

    @property
    def config_values(self):

        from ambry.orm import Config as SAConfig

        s = self.session

        d = {}

        for config in s.query(SAConfig).filter(SAConfig.d_vid == ROOT_CONFIG_NAME_V).all():
            d[(str(config.group),str(config.key))] = config.value

        self.close_session()
        return d

    def _mark_update(self):
        from ambry.orm import Config

        import datetime

        self.set_config_value('activity','change', datetime.datetime.utcnow().isoformat())

    ##
    ## Install and remove bundles and partitions
    ##

    def install_bundle_file(self, identity, bundle_file):
        """Install a bundle in the database, starting from a file that may
        be a partition or a bundle"""
        from ..identity import Identity

        #
        # This is really just used to ignore partitions
        #

        if isinstance(identity , dict):
            identity = Identity.from_dict(identity)

        if identity.is_bundle:

            bundle = DbBundle(bundle_file)
            self.install_bundle(bundle)

    def install_dataset(self, bundle):
        """Install only the most basic parts of the bundle, excluding the
        prtitions and tables. Use install_bundle to install everything.

        This will delete all of the tables and partitions associated with the
        bundle, if they already exist, so callers should check that the dataset does not
        already exist if before installing again.
        """

        from ambry.orm import Dataset, Config, Column, Table, Partition
        from ambry.bundle import Bundle

        # There should be only one dataset record in the
        # bundle
        db = bundle.database
        bdbs = db.unmanaged_session

        s = self.session
        dataset = bdbs.query(Dataset).one()
        s.merge(dataset)

        for config in bdbs.query(Config).all():
            s.merge(config)

        s.query(Partition).filter(Partition.d_vid == dataset.vid).delete()

        for table in dataset.tables:
            s.query(Column).filter(Column.t_vid == table.vid).delete()

        s.query(Table).filter(Table.d_vid == dataset.vid).delete()

        try:
            self.commit()
        except IntegrityError as e:
            self.logger.error("Failed to merge")
            self.rollback()
            raise e

        return dataset

    def install_dataset_identity(self, identity):

        from ambry.orm import Dataset

        ds = Dataset(**identity.dict)
        ds.name = identity.sname
        ds.vname = identity.vname
        ds.fqname = identity.fqname
        ds.cache_key = identity.cache_key
        ds.creator = 'N/A'

        self.session.add(ds)

        self.commit()


    def install_bundle(self, bundle):
        '''Copy the schema and partitions lists into the library database

        '''
        from ambry.orm import Dataset, Config, Column, Table, Partition
        from ambry.bundle import Bundle

        if not isinstance(bundle, Bundle):
            raise ValueError("Can only install a  Bundle object")

            # The Tables only get installed when the dataset is installed,
            # not for the partition

        self._mark_update()


        dataset = self.install_dataset(bundle)

        s = self.session

        for table in dataset.tables:

            s.merge(table)

            for column in table.columns:
                s.merge(column)


        for partition in dataset.partitions:
            s.merge(partition)

        try:
            self.commit()
        except IntegrityError as e:
            self.logger.error("Failed to merge")
            self.rollback()
            raise e

    def install_partition(self, bundle,  p_id):
        """Install a single partition and its tables. This is mostly
        used for installing into warehouses, where it isn't desirable to install
        the whole bundle"""
        from ambry.orm import Table
        from sqlalchemy.orm.exc import NoResultFound

        partition = bundle.partitions.find(p_id)

        s = self.session

        for table_name in partition.tables:
            table = bundle.schema.table(table_name)

            try:
                s.query(Table).filter(Table.vid == table.vid).one()
                # the library already has the table
            except NoResultFound as e:
                s.merge(table)

                for column in table.columns:
                    s.merge(column)

        s.merge(partition.record)

        try:
            self.commit()
        except IntegrityError as e:
            self.logger.error("Failed to merge")
            self.rollback()
            raise e

    def install_table(self, table_or_vid, name=None):
        """Mark a table record as installed"""
        from ambry.orm import Table

        s = self.session
        table = None

        table = s.query(Table).filter(Table.vid == table_or_vid).one()

        if not table:
            table = s.query(Table).filter(Table.vid == table.vid).one()

        if not name:
            name = table.name

        table.installed = name


        s.merge(table)
        s.commit()

    def remove_bundle(self, bundle):
        '''remove a bundle from the database'''
        from ..orm import Dataset
        from ..bundle import LibraryDbBundle

        try:
            dataset, partition = self.get_id(bundle.identity.vid) #@UnusedVariable
        except AttributeError:
            dataset, partition = bundle, None

        if not dataset:
            return False

        if partition:
            self.remove_partition(partition)
        else:
            b = LibraryDbBundle(self, dataset.identity.vid)
            for p in b.partitions:
                self.remove_partition(p)

        dataset = self.session.query(Dataset).filter(Dataset.vid==dataset.identity.vid).one()

        # Can't use delete() on the query -- bulk delete queries do not
        # trigger in-python cascades!
        self.session.delete(dataset)

        self.commit()

    def remove_partition(self, partition):
        from ..bundle import LibraryDbBundle
        from ..orm import Partition

        try:
            dataset = self.get(partition.identity.vid) #@UnusedVariable
        except AttributeError:
            # It is actually an identity, we hope
            dataset = partition.as_dataset

        b = LibraryDbBundle(self, dataset.vid)

        s = self.session

        s.query(Partition).filter(Partition.t_vid  == dataset.partition.vid).delete()

        self.commit()

    ##
    ## Get objects by reference, or resolve a reference
    ##

    def get(self, vid):
        '''Get an identity by a vid. For partitions, returns a nested Identity'''
        from ..identity import ObjectNumber, DatasetNumber, PartitionNumber
        from ..orm import Dataset, Partition

        if isinstance(vid, basestring):
            vid = ObjectNumber.parse(vid)

        if isinstance(vid, DatasetNumber):
            d = self.session.query(Dataset).filter(Dataset.vid == str(vid)).one()
            did = d.identity

        elif isinstance(vid, PartitionNumber):
            d,p = self.session.query(Dataset, Partition).join(Partition).filter(Partition.vid == str(vid)).one()
            did = d.identity
            did.add_partition(p.identity)

        else:
            raise ValueError('vid was wrong type: {}'.format(type(vid)))

        return did

    def get_table(self, table_vid):

        from ambry.orm import Table

        s = self.session

        return  s.query(Table).filter(Table.vid == table_vid).one()

    def list(self, datasets=None, location = LocationRef.LOCATION.LIBRARY, key='vid'):
        """
        :param datasets: If specified, must be a dict, which the internal dataset data will be
        put into.
        :return: vnames of the datasets in the library.
        """

        from ..orm import Dataset, Partition

        if datasets is None:
            datasets = {}

        for d,p in self.session.query(Dataset, Partition).join(Partition).filter(Dataset.vid != ROOT_CONFIG_NAME_V).all():

            ck = getattr(d.identity, key)

            if ck not in datasets:
                dsid = d.identity
                datasets[ck] = dsid
            else:
                dsid = datasets[ck]

            dsid.locations.set(LocationRef.LOCATION.LIBRARY)

            if not (dsid.partitions and p.vid in dsid.partitions):
                datasets[ck].add_partition(p.identity)
                p = dsid.partitions[p.vid]


            p.locations.set(LocationRef.LOCATION.LIBRARY)

        return datasets

    def datasets(self, key='vid'):
        '''List only the dataset records'''


        from ..orm import Dataset

        datasets = {}

        for d in self.session.query(Dataset).filter(Dataset.vid != ROOT_CONFIG_NAME_V).all():

            ck = getattr(d.identity, key)
            datasets[ck] = d.identity

        return datasets


    @property
    def resolver(self):
        from .query import Resolver
        return  Resolver(self.session)

    def find(self, query_command):
        '''Find a bundle or partition record by a QueryCommand or Identity

        Args:
            query_command. QueryCommand or Identity

        returns:
            A list of identities, either Identity, for datasets, or PartitionIdentity
            for partitions.

        '''

        from ambry.orm import Dataset
        from ambry.orm import Partition
        from ambry.identity import Identity
        from ambry.orm import Table, Column

        def like_or_eq(c,v):

            if v and '%' in v:
                return c.like(v)
            else:
                return c == v

        s = self.session

        has_partition = False
        has_where = False

        if isinstance(query_command, Identity):
            raise NotImplementedError()
            out = []
            for d in self.queryByIdentity(query_command).all():
                id_ = d.identity
                d.path = os.path.join(self.cache,id_.cache_key)
                out.append(d)

        tables = [Dataset]

        if len(query_command.partition) > 0:
            tables.append(Partition)

        if len(query_command.table) > 0:
            tables.append(Table)

        if len(query_command.column) > 0:
            tables.append(Column)

        tables.append(Dataset.id_) # Dataset.id_ is included to ensure result is always a tuple)

        query = s.query(*tables) # Dataset.id_ is included to ensure result is always a tuple

        if len(query_command.identity) > 0:
            for k,v in query_command.identity.items():
                if k == 'id':
                    k = 'id_'
                try:
                    query = query.filter( like_or_eq(getattr(Dataset, k),v) )
                except AttributeError as e:
                    # Dataset doesn't have the attribute, so ignore it.
                    pass

        if len(query_command.partition) > 0:
            query = query.join(Partition)

            for k,v in query_command.partition.items():
                if k == 'id':
                    k = 'id_'

                from sqlalchemy.sql import or_

                if k == 'any':
                    continue # Just join the partition
                elif k == 'table':
                    # The 'table" value could be the table id
                    # or a table name
                    query = query.join(Table)
                    query = query.filter( or_(Partition.t_id  == v,
                                              like_or_eq(Table.name,v)))
                elif k == 'space':
                    query = query.filter( or_( like_or_eq(Partition.space,v)))

                else:
                    query = query.filter(  like_or_eq(getattr(Partition, k),v) )


            if not query_command.partition.format:
                # Exclude CSV if not specified
                query = query.filter( Partition.format  != 'csv')

        if len(query_command.table) > 0:
            query = query.join(Table)
            for k,v in query_command.table.items():
                query = query.filter(  like_or_eq(getattr(Table, k),v) )

        if len(query_command.column) > 0:
            query = query.join(Table)
            query = query.join(Column)
            for k,v in query_command.column.items():
                query = query.filter(  like_or_eq(getattr(Column, k),v) )

        query = query.distinct().order_by(Dataset.revision.desc())

        out = []

        try:
            for r in query.all():

                o = {}

                try:
                    o['identity'] = r.Dataset.identity.dict
                    o['partition'] = r.Partition.identity.dict

                except:
                    o['identity'] =  r.Dataset.identity.dict


                try: o['table'] = r.Table.dict
                except: pass

                try:o['column'] = r.Column.dict
                except: pass

                out.append(o)
        except Exception as e:
            self.logger.error("Exception while querrying in {}, schema {}".format(self.dsn, self._schema))
            raise

        self.close_session()
        return out

    def queryByIdentity(self, identity):
        from ..orm import Dataset, Partition
        from ..identity import Identity,PartitionIdentity
        from sqlalchemy import desc

        s = self.database.session

        # If it is a string, it is a name or a dataset id
        if isinstance(identity, str) or isinstance(identity, unicode) :
            query = (s.query(Dataset)
                     .filter( (Dataset.id_==identity) | (Dataset.name==identity)) )
        elif isinstance(identity, PartitionIdentity):

            query = s.query(Dataset, Partition)

            for k,v in identity.to_dict().items():
                d = {}

                if k == 'revision':
                    v = int(v)

                d[k] = v

            query = query.filter_by(**d)

        elif isinstance(identity, Identity):
            query = s.query(Dataset)

            for k,v in identity.to_dict().items():
                d = {}
                d[k] = v

            query = query.filter_by(**d)


        elif isinstance(identity, dict):
            query = s.query(Dataset)

            for k,v in identity.items():
                d = {}
                d[k] = v
                query = query.filter_by(**d)

        else:
            raise ValueError("Invalid type for identity")

        query.order_by(desc(Dataset.revision))

        return query

    ##
    ## Ticket records
    ##

    def add_ticket(self, identity, ticket):
        from ambry.orm import  File
        import time
        path = identity.cache_key
        group = ticket
        ref = identity.id_
        state = 'ticketed'

        s = self.session

        s.query(File).filter(File.path == path and File.state == state).delete()

        file_ = File(path=path,
                     group=group,
                     ref=ref,
                     modified=int(time.time()),
                     state = state,
                     size=0)

        s.add(file_)
        self.commit()

    def get_ticket(self, ticket):
        from ambry.orm import  File
        s = self.session

        return s.query(File).filter(File.group == ticket).one()


    ##
    ## File records
    ##

    def add_remote_file(self, identity):
        self.add_file(identity.cache_key, 'remote', identity.vid, state='remote')


    def add_file(self,
                 path, group, ref,
                 state='new',
                 type_='bundle',
                 data=None,
                 source_url=None,
                 hash=None
                ):
        from ambry.orm import  File

        if os.path.exists(path):
            stat = os.stat(path)
            modified = int(stat.st_mtime)
            size = stat.st_size
        else:
            modified = None
            size = None

        s = self.session

        try: s.query(File).filter(File.path == path).delete()
        except ProgrammingError:
            pass
        except OperationalError:
            pass

        file_ = File(path=path,
                     group=group,
                     ref=ref,
                     modified=modified,
                     state = state,
                     size=size,
                     type_=type_,
                     data=data,
                     source_url=source_url,
                     hash = hash
                     )

        # Sqlalchemy doesn't automatically rollback on exceptions, and you
        # can't re-try the commit until you roll back.
        try:
            s.add(file_)
            self.commit()
        except:
            self.rollback()
            raise

        self._mark_update()


    def get_file_by_state(self, state, type_=None):
        """Return all files in the database with the given state"""
        from ambry.orm import  File
        s = self.session

        # The orderby clause should put bundles before partitions, which is
        # required to install correctly.

        if state == 'all':
            q =  s.query(File).order_by(File.ref)
        else:
            q =  s.query(File).filter(File.state == state).order_by(File.ref)

        if type_:
            q = q.filter(File.type_ == type_)

        return q.all()


    def get_file_by_ref(self, ref, type_=None):
        """Return all files in the database with the given state"""
        from ambry.orm import  File
        from sqlalchemy.orm.exc import NoResultFound
        s = self.session

        try:
            q = s.query(File).filter(File.ref == ref)

            if type_ is not None:
                q = q.filter(File.type_ == type_)

            return q.all()

        except NoResultFound:
            return None


    def get_file_by_type(self, type_=None):
        """Return all files in the database with the given state"""
        from ambry.orm import  File
        from sqlalchemy.orm.exc import NoResultFound
        s = self.session

        try:
            return s.query(File).filter(File.type_ == type_).all()

        except NoResultFound:
            return None


    def get_file_by_path(self, path):
        """Return all files in the database with the given state"""
        from ambry.orm import File
        from sqlalchemy.orm.exc import NoResultFound

        s = self.session

        try:
            return s.query(File).filter(File.path == path).one()

        except NoResultFound:
            return None

    def remove_file(self,path):
        pass


    def get_file(self,path):
        pass

    ##
    ## Database backup and restore. Synchronizes the database with
    ## a remote. This is used when a library is created attached to a remote, and
    ## needs to get the library database from the remote.
    ##

    def _copy_db(self, src, dst):
        from ambry.orm import Dataset
        from sqlalchemy.orm.exc import NoResultFound

        try:
            dst.session.query(Dataset).filter(Dataset.vid=='a0').delete()
        except:
            pass

        for table in self.metadata.sorted_tables: # sorted by foreign key dependency

            rows = src.session.execute(table.select()).fetchall()
            dst.session.execute(table.delete())
            for row in rows:
                dst.session.execute(table.insert(), row)

        dst.session.commit()


    def dump(self, path):
        '''Copy the database to a new Sqlite file, as a backup. '''
        import datetime

        dst = LibraryDb(driver='sqlite', dbname=path)

        dst.create()

        self.set_config_value('activity','dump', datetime.datetime.utcnow().isoformat())

        self._copy_db(self, dst)

    def needs_dump(self):
        '''Return true if the last dump date is after the last change date, and
        the last change date is more than 10s in the past'''
        import datetime
        from dateutil  import parser

        configs = self.config_values

        td = datetime.timedelta(seconds=10)

        changed =  parser.parse(configs.get(('activity','change'),datetime.datetime.fromtimestamp(0).isoformat()))
        dumped = parser.parse(configs.get(('activity','dump'),datetime.datetime.fromtimestamp(0).isoformat()))
        dumped_past = dumped + td
        now = datetime.datetime.utcnow()


        if ( changed > dumped and now > dumped_past):
            return True
        else:
            return False

    def restore(self, path):
        '''Restore a sqlite database dump'''
        import datetime

        self.create()

        src = LibraryDb(driver='sqlite', dbname=path)

        self._copy_db(src, self)

        self.set_config_value('activity','restore', datetime.datetime.utcnow().isoformat())



def _pragma_on_connect(dbapi_con, con_record):
    '''ISSUE some Sqlite pragmas when the connection is created'''

    #dbapi_con.execute('PRAGMA foreign_keys = ON;')
    return # Not clear that there is a performance improvement.
    dbapi_con.execute('PRAGMA journal_mode = MEMORY')
    dbapi_con.execute('PRAGMA synchronous = OFF')
    dbapi_con.execute('PRAGMA temp_store = MEMORY')
    dbapi_con.execute('PRAGMA cache_size = 500000')
    dbapi_con.execute('pragma foreign_keys=ON')
