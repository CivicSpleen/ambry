from __future__ import absolute_import
from ..library import Library
from ..library.database import LibraryDb
from ..cache import new_cache, CacheInterface
from ..database import new_database
import os

class NullCache(CacheInterface):
    def has(self, rel_path, md5=None, use_upstream=True):
        return False


class NullLogger(object):
    def __init__(self):
        pass

    def progress(self, type_, name, n, message=None):
        pass

    def progress(self, o,t):
        pass

    def log(self, message):
        pass

    def error(self, message):
        pass

    def warn(self, message):
        pass

def new_warehouse(config, elibrary):

    assert elibrary is not None

    service = config['service'] if 'service' in config else 'relational'

    if 'database' in config:
        db_config = dict(config['database'].items()) # making a copy so we can alter it.
    else:
        db_config = dict(config.items())

    database = new_database(db_config, class_='warehouse')

    storage = new_cache(config['storage']) if 'storage' in config else None

    library_database = LibraryDb(**config['library']) if 'library' in config else  LibraryDb(**db_config)

    # This library instance is only for the warehouse database.
    wlibrary = Library(
        cache=NullCache(),
        database=library_database)

    if service == 'sqlite':
        from .sqlite import SqliteWarehouse
        return SqliteWarehouse(database=database, wlibrary=wlibrary, elibrary=elibrary)

    if service == 'spatialite':

        from .sqlite import SpatialiteWarehouse

        return SpatialiteWarehouse(database=database, wlibrary=wlibrary, elibrary=elibrary)

    elif service == 'postgres':
        from .postgres import PostgresWarehouse

        return PostgresWarehouse(database=database, wlibrary=wlibrary, elibrary=elibrary)

    elif service == 'postgis':
        from .postgis import PostgisWarehouse

        return PostgisWarehouse(database=database, wlibrary=wlibrary, elibrary=elibrary)

    else:
        raise Exception("Unknown warehouse type: {}".format(service))


class ResolutionError(Exception):
    pass

class WarehouseInterface(object):
    def __init__(self,
                 database,
                 wlibrary=None, # Warehouse library
                 elibrary=None, # external Library
                 logger=None,
                 test=False):

        assert wlibrary is not None
        assert elibrary is not None
        assert database is not None

        self.database = database
        self.wlibrary = wlibrary
        self.elibrary = elibrary
        self.test = test

        self.logger = logger if logger else NullLogger()

    def create(self):
        self.database.create()
        self.wlibrary.database.create()

    def clean(self):
        self.database.clean()

    def delete(self):
        self.database.enable_delete = True
        self.database.drop()
        self.wlibrary.database.enable_delete = True
        self.wlibrary.database.drop()

    def exists(self):
        self.database.exists()

    @property
    def library(self):
        return self.wlibrary

    ##
    ## Installation
    ##

    def install(self, partition, tables=None, prefix=None):
        from ..orm import Partition

        results = dict(
            tables = {},
            partitions = {}
        )

        p_vid = self._to_vid(partition)

        p_orm = self.wlibrary.database.session.query(Partition).filter(Partition.vid == p_vid).first()

        if p_orm and p_orm.installed == 'y':
            self.logger.info("Skipping {}; already installed".format(p_orm.vname))
            return

        bundle, p = self._setup_install(p_vid)

        if p.identity.format not in ('db', 'geo'):
            self.logger.warn("Skipping {}; uninstallable format: {}".format(p.identity.vname, p.identity.format))
            return;

        all_tables = self.install_partition(bundle, p, prefix=prefix)

        if not tables:
            tables = all_tables

        for table_name in tables:


            if isinstance(table_name, (list, tuple)):
                table_name, where = table_name
            else:
                where = None

            try:
                if p.identity.format == 'db':
                    self.elibrary.get(p.vid) # ensure it is local
                    itn = self.load_local(p, table_name, where)
                else:
                    self.elibrary.get(p.vid)  # ensure it is local
                    itn = self.load_ogr(p, table_name, where)

                orm_table = p.get_table(table_name)


                self.library.database.mark_table_installed(orm_table.vid, itn)

            except Exception as e:
                self.logger.error("Failed to install table '{}': {}".format(table_name,e))

        # Install bundle doc, if it doesn't exist.
        if not self.library.files.query.type('text/html').ref(bundle.identity.vid).first:
            self.install_file(path=os.path.join('doc', bundle.identity.vid) + '.html',
                              ref=bundle.identity.vid, type='text/html', content=bundle.html_doc())

        # install the partition documentation
        self.install_file(path=os.path.join('doc', p.vid)+'.html', ref=p.vid, type='text/html',  content = p.html_doc())

        self.library.database.mark_partition_installed(p_vid)


    def install_partition(self, bundle, partition, prefix=None):
        '''Install the records for the partition, the tables referenced by the partition,
        and the bundle, if they aren't already installed'''
        from sqlalchemy.orm.exc import NoResultFound
        from sqlalchemy import inspect

        ld = self.library.database

        pid = self._to_vid(partition)

        ld.install_partition_by_id(bundle, pid)

        p = bundle.partitions.get(pid) # just gets the record

        p = self.elibrary.get(p.vid, cb=self.logger.copy).partition # Gets the database file.

        inspector = inspect(p.database.engine)

        all_tables = [ t.name for t in bundle.schema.tables ]

        tables = [ t for t in inspector.get_table_names() if t != 'config' and t in all_tables ]

        for table_name in tables:
            self.create_table(p, table_name)

        return tables

    def install_manifest(self, manifest):

        from ..dbexceptions import NotFoundError, ConfigurationError
        from ambry.util import init_log_rate

        from . import Logger
        from ..cache import new_cache
        import os


        # Update the manifest with bundle information, since it doesn't normally have access to a library
        manifest.add_bundles(self.elibrary)

        self.logger.info("Working directory: {}".format(manifest.abs_work_dir))

        self.logger = Logger(self.logger, init_log_rate(self.logger.info, N=2000))

        working_cache = new_cache(manifest.abs_work_dir)

        ## First pass
        for line, section in manifest.sorted_sections:

            tag = section.tag

            if tag in ('partitions', 'sql', 'index', 'mview', 'view'):
                self.logger.info("== Processing manifest section {} at line {}".format(section.tag, section.linenumber))

            if tag == 'partitions':
                for pd in section.content['partitions']:
                    try:

                        tables = pd['tables']

                        if pd['where'] and len(pd['tables']) == 1:
                            tables = [(pd['tables'][0], "WHERE (" + pd['where'] + ")")]

                        self.install(pd['partition'], tables)

                    except NotFoundError:
                        self.logger.error("Partition {} not found in external library".format(pd['partition']))

            elif tag == 'sql':
                sql = section.content

                if self.database.driver in sql:
                    self.run_sql(sql[self.database.driver])

            elif tag == 'index':
                c = section.content
                self.create_index(c['name'], c['table'], c['columns'])

            elif tag == 'mview':
                self.install_material_view(section.args, section.content['text'], clean=manifest.force)

            elif tag == 'view':
                self.install_view(section.args, section.content['text'])

            elif tag == 'extract':
                import json

                d = section.content
                doc = manifest.doc_for(section)
                if doc:
                    d['doc'] = doc.content['html']

                self.install_file(path=os.path.join(manifest.work_dir, 'extracts', d['rpath']), ref=d['table'], type='extract', data=d)


        # Manifest documentation
        self.install_file(path=os.path.join(manifest.work_dir, 'doc', 'index.html'), ref=manifest.uid, type='text/html',
                          content=manifest.html_doc())

        # Manifest data
        self.install_file(path=os.path.join('manifests', manifest.uid)+'.ambry', ref=manifest.uid, type='manifest',
                          content=str(manifest))

        if os.path.exists(self.database.path):
            return self.database.path
        else:
            return self.database.dsn

    def install_view(self, name, sql):
        raise NotImplementedError(type(self))

    def install_file(self, path,  ref, content=None, source=None, type=None, data=None):
        raise NotImplementedError(type(self))

    def run_sql(self, sql_text):
        raise NotImplementedError(type(self))


    def load_local(self, partition, table_name, where):
        '''Load data using a network connection to the warehouse and
        INSERT commands'''
        raise NotImplementedError()

    def load_remote(self, partition, table_name, urls):
        '''Load data by streaming from the remote REST interface to a bulk load
        facility of the target warehouse'''
        raise NotImplementedError()

    def load_ogr(self, partition, table_name, where):
        '''Load geo data using the ogr2ogr program'''
        raise NotImplementedError()

    def _setup_install(self, ref):
        '''Perform local and remote resolutions to get the bundle, partition and links
        to CSV parts in the remote REST itnerface '''
        from ..identity import Identity

        if isinstance(ref, Identity):
            ref = ref.vid

        dataset = self.elibrary.resolve(ref)

        if not dataset:
            raise ResolutionError("Library does not have object for reference: {}".format(ref))

        ident = dataset.partition

        if not ident:
            raise ResolutionError(
                "Ref resolves to a bundle, not a partition. Can only install partitions: {}".format(ref))

        # Get just the bundle. We'll install the partition from CSV directly from the
        # library
        b = self.elibrary.get(dataset)
        p = b.partitions.get(ident.id_)

        return b, p


    def extract(self, cache, force=False):
        """Generate the extracts and return a struture listing the extracted files. """

        from .extractors import new_extractor

        extracts = []

        # Generate the file etracts

        for f in self.library.files.query.type('extract').all:

            table = f.data['table']
            format = f.data['format']

            ex = new_extractor(format, self, cache, force=False)

            extracts.append(ex.extract(table, cache, f.path))

        # HTML files.
        for f in self.library.files.query.type('text/html').all:
            content = f.content
            path = f.path


        return extracts

    ##
    ## users
    ##

    def drop_user(self, u):
        pass # Sqlite database don't have users.

    def create_user(self, u):
        pass # Sqlite databases don't have users.

    def users(self):
        return {} # Sqlite databases don't have users.


    def get(self, name_or_id):
        """Return true if the warehouse already has the referenced bundle or partition"""

        return self.library.resolve(name_or_id)


    def has(self, ref):
        r = self.library.resolve(ref)

        if bool(r):
            return True
        else:
            return False

    def has_table(self, table_name):
        raise NotImplementedError()

    def create_table(self, partition, table_name):
        raise NotImplementedError()

    def _to_vid(self, partition):
        from ..partition import PartitionBase
        from ..identity import Identity
        from ..dbexceptions import NotFoundError

        if isinstance(partition, basestring):
            dsid = self.elibrary.resolve(partition)

            if not dsid:
                raise NotFoundError("Didn't find {} in external library".format(partition))

            if not dsid.partition:
                raise ResolutionError("Term referred to a dataset, not a partition: {}".format(partition))

            pid = dsid.partition.vid

        elif isinstance(partition, PartitionBase):
            pid = partition.identity.vid
        elif isinstance(partition, Identity):
            pid = partition.vid
        else:
            pid = partition

        return pid


    def _partition_to_dataset_vid(self, partition):
        from ..partition import PartitionBase
        from ..identity import Identity

        if isinstance(partition, PartitionBase):
            did = partition.identity.as_dataset().vid
        elif isinstance(partition, Identity):
            did = partition.as_dataset().vid
        else:
            from ..identity import ObjectNumber

            did = str(ObjectNumber(str(partition)).dataset)

        return did


    def augmented_table_name(self, identity, table_name):
        """Create a table name that is prefixed with the dataset number and the
        partition grain, if it has one"""

        name = identity.as_dataset().vid.replace('/', '_') + '_' + table_name

        if identity.grain:
            name = name + '_' + identity.grain

        return name

    def is_augmented_name(self, identity, table_name):

        return table_name.startswith(identity.vid.replace('/', '_') + '_')

    def _ogr_args(self, partition):
        '''Return a arguments for ogr2ogr to connect to the database'''
        raise NotImplementedError()

    def list(self):
        from ..orm import Partition
        from ..identity import LocationRef

        orms  = self.wlibrary.database.session.query(Partition).filter(Partition.installed == 'y').all()

        idents  = []

        for p in orms:
            ident = p.identity
            ident.locations.set(LocationRef.LOCATION.WAREHOUSE)
            idents.append(ident)

        return sorted(idents, key = lambda x : x.fqname)


    def info(self):
        config = self.config.to_dict()

        if 'password' in config['database']: del config['database']['password']
        return config


    def table_docs(self):
        from ..orm import Table

        for table in self.wlibrary.database.session.query(Table).all():
            print table.markdown_table



def database_config(db, base_dir=''):
    import urlparse
    import os

    parts = urlparse.urlparse(db)

    if parts.scheme == 'sqlite':
        config = dict(service='sqlite', database=dict(dbname=os.path.join(base_dir,parts.path), driver='sqlite'))

    elif parts.scheme == 'spatialite':
        config = dict(service='spatialite', database=dict(dbname=os.path.join(base_dir,parts.path), driver='spatialite'))

    elif parts.scheme == 'postgres':
        config = dict(service='postgres',
                      database=dict(driver='postgres',
                                    server=parts.hostname,
                                    username=parts.username,
                                    password=parts.password,
                                    dbname=parts.path.strip('/')
                      ))

    elif parts.scheme == 'postgis':
        config = dict(service='postgis',
                      database=dict(driver='postgis',
                                    server=parts.hostname,
                                    username=parts.username,
                                    password=parts.password,
                                    dbname=parts.path.strip('/')
                      ))
    else:
        raise ValueError("Unknown database connection scheme: {}".format(parts.scheme))

    return config

class Logger(object):
    def __init__(self, logger, lr):
        self.lr = lr
        self.logger = logger
        self.lr('Init warehouse logger')

    def progress(self,type_,name, n, message=None):
        self.lr("{} {}: {}".format(type_, name, n))

    def copy(self, o,t):
        self.lr("{} {}".format(o,t))

    def info(self,message):
        self.logger.info(message)

    def log(self,message):
        self.logger.info(message)

    def error(self,message):
        self.logger.error(message)

    def fatal(self,message):
        self.logger.fatal(message)

    def warn(self, message):
        self.logger.warn(message)
