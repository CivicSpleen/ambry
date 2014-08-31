from __future__ import absolute_import
from ..library import Library
from ..library.database import LibraryDb
from ..cache import new_cache, CacheInterface
from ..database import new_database
import os
from ..util import Constant
from ambry.util import init_log_rate

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

    def info(self, message):
        pass

    def error(self, message):
        pass

    def warn(self, message):
        pass


def new_warehouse(config, elibrary, logger=None):

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
        database=library_database
    )

    if service == 'sqlite':
        from .sqlite import SqliteWarehouse
        return SqliteWarehouse(database=database, wlibrary=wlibrary, elibrary=elibrary, logger = logger )

    if service == 'spatialite':

        from .sqlite import SpatialiteWarehouse

        return SpatialiteWarehouse(database=database, wlibrary=wlibrary, elibrary=elibrary, logger = logger )

    elif service == 'postgres':
        from .postgres import PostgresWarehouse

        return PostgresWarehouse(database=database, wlibrary=wlibrary, elibrary=elibrary, logger = logger )

    elif service == 'postgis':
        from .postgis import PostgisWarehouse

        return PostgisWarehouse(database=database, wlibrary=wlibrary, elibrary=elibrary, logger = logger )

    else:
        raise Exception("Unknown warehouse type: {}".format(service))


class ResolutionError(Exception):
    pass

class WarehouseInterface(object):

    FILE_TYPE = Constant()

    FILE_TYPE.MANIFEST = 'manifest'
    FILE_TYPE.HTML = 'text/html'

    FILE_GROUP = Constant()

    FILE_GROUP.MANIFEST = 'manifest'
    FILE_GROUP.DOC = 'doc'
    FILE_GROUP.EXTRACT = 'extract'

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

        logger = logger if logger else NullLogger()

        self.logger =  Logger(logger, init_log_rate(logger.info, N=2000))

    def create(self):
        from datetime import datetime

        self.database.create()
        self.wlibrary.database.create()

        self._meta_set('created', datetime.now().isoformat())


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
    ## Metadata
    ##

    def _meta_set(self, key, value):
        from ..orm import Config
        return self.library.database.set_config_value('warehouse', key, value)

    def _meta_get(self, key):
        from ..orm import Config

        try:
            return self.library.database.get_config_value('warehouse', key).value
        except AttributeError:
            return None

    configurable = ('title','about','local_cache','remote_cache')

    @property
    def title(self):
        """Title of the warehouse"""
        return self._meta_get('title')

    @title.setter
    def title(self, v):
        return  self._meta_set('title', v)

    @property
    def about(self):
        """Short description of the warehouse"""
        return self._meta_get('about')

    @about.setter
    def about(self, v):
        return self._meta_set('about', v)

    @property
    def local_cache(self):
        """Cache name for local publications. Usually a filesystem path"""
        return self._meta_get('local_cache')

    @local_cache.setter
    def local_cache(self, v):
        return self._meta_set('local_cache', v)

    @property
    def remote_cache(self):
        """Cache name for remote publications. Usually S3"""
        return self._meta_get('remote_cache')

    @remote_cache.setter
    def remote_cache(self, v):
        return self._meta_set('remote_cache', v)


    @property
    def manifests(self):
        """Return the parsed manifests that have been installed"""
        from .manifest import Manifest

        manifests = []

        for f in self.library.files.query.type(self.FILE_TYPE.MANIFEST).group(self.FILE_GROUP.MANIFEST).all:
            index = self.library.files.query.type(self.FILE_TYPE.HTML).group(self.FILE_GROUP.MANIFEST).first
            manifests.append((f, Manifest(f.content)))

        return manifests


    @property
    def bundles(self):
        """Metadata for bundles, each with the partitions that are installed here.

        This extracts the bundle information that is in the partitions list, but it requires
        that the add_bundle() method has been run first, because the manifest doesn't usually ahve access to
        a library
        """

        l =  self.library.list(with_partitions=True)

        for k, v in l.items():
            d = { e.key.replace('.','_'):e.value for e in self.library.database.get_bundle_values(k,'config')}
            v.data.update(d)

        return l

    @property
    def tables(self):
        from ..orm import Table

        for table in self.library.database.session.query(Table).all():
            yield table


    def orm_table(self, vid):
        from ..orm import Table

        return self.library.database.session.query(Table).filter(Table.vid == vid).first()

    def partition(self, vid):
        from ..orm import Partition

        return self.library.database.session.query(Partition).filter(Partition.vid == vid).first()

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
            table, meta = self.create_table(p, table_name)

            alias = p.identity.as_dataset().id_ + '_' + table_name

            self.install_table_alias(table, alias)

        return tables

    def install_manifest(self, manifest, force = None, reset=False):
        """Install the partitions and views specified in a manifest file """
        from ..dbexceptions import NotFoundError, ConfigurationError
        from datetime import datetime
        import os


        # Delete everything related to this manifest
        (self.library.files.query.source_url(manifest.uid)).delete()

        # Update the manifest with bundle information, since it doesn't normally have access to a library
        manifest.add_bundles(self.elibrary)

        # If the manifest doesn't have a title or description, get it fro the manifest.

        if reset or not self.title:
            self.title = manifest.title

        if (reset or not self.about) and manifest.summary:
            self.about = manifest.summary['html']

        if reset or not self.local_cache:
            self.local_cache = manifest.local

        if reset or not self.remote_cache:
            self.remote_cache = manifest.remote

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
                self.install_material_view(section.args, section.content['text'], clean= force)

            elif tag == 'view':
                self.install_view(section.args, section.content['text'])

            elif tag == 'extract':
                import json

                d = section.content
                doc = manifest.doc_for(section)
                if doc:
                    d['doc'] = doc.content['html']

                self.install_file(path=os.path.join('extracts', manifest.uid, d['rpath']), ref=d['table'],
                                  type=d['format'], group='extract', source_url = manifest.uid, data=d)

            elif tag == 'include':
                from .manifest import Manifest
                m = Manifest(section.content['path'])
                self.install_manifest(m, force = force)

        # Manifest data

        self.install_file(path=os.path.join('manifests', manifest.uid)+'.ambry', ref=manifest.uid,
                          type=self.FILE_TYPE.MANIFEST, group=self.FILE_GROUP.MANIFEST, source_url = manifest.uid,
                          content=str(manifest))

        self._meta_set(manifest.uid, datetime.now().isoformat())

        if os.path.exists(self.database.path):
            return self.database.path
        else:
            return self.database.dsn

    def install_view(self, name, sql):
        raise NotImplementedError(type(self))

    def install_table_alias(self, table, alias):
        self.install_view(alias, "SELECT * FROM {}".format(table))

    def install_file(self, path,  ref, content=None, source=None, type=None, group=None, source_url= None, data=None):
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

    class extract_entry(object):
        def __init__(self, extracted, rel_path, abs_path, data=None):
            self.extracted = extracted
            self.rel_path = rel_path
            self.abs_path = abs_path
            self.data = data

        def __str__(self):
            return 'extracted={} rel={} abs={} data={}'.format(self.extracted, self.rel_path, self.abs_path, self.data)


    def extract(self, cache, force=False):
        """Generate the extracts and return a struture listing the extracted files. """
        from contextlib import closing

        from .extractors import new_extractor
        from ..text import Tables, BundleDoc, Renderer, WarehouseIndex

        # Get the URL to the root. The public_utl arg only affects S3, and gives a URL without a signature.
        root = cache.path('', missing_ok = True, public_url = True)

        def maybe_render(rel_path, render_lambda, metadata = {}, force=False):

            if rel_path.endswith('.html'):
                metadata['content-type']  = 'text/html'
            elif rel_path.endswith('.css'):
                metadata['content-type'] = 'text/css'

            if not cache.has(rel_path) or force:
                with cache.put_stream(rel_path, metadata=metadata) as s:
                    s.write(render_lambda().encode('utf-8'))
                extracted = True
            else:

                extracted = False

            return WarehouseInterface.extract_entry(extracted, rel_path, cache.path(rel_path))


        extracts = []

        # Generate the file etracts

        for f in self.library.files.query.group('extract').all:

            table = f.data['table']
            format = f.data['format']

            ex = new_extractor(format, self, cache, force=force)

            extracts.append(WarehouseInterface.extract_entry(*ex.extract(table, cache, f.path)))

        # HTML files.
        for f in self.library.files.query.type('text/html').all:
            extracts.append(maybe_render(f.path, lambda: f.content))

        # Manifests
        for f, m in self.manifests:
            from ..text import ManifestDoc

            extracts.append(maybe_render('doc/{}.html'.format(m.uid), lambda: ManifestDoc(root).render(m, self.elibrary), force=force))

            extracts.append(maybe_render(f.path, lambda: f.content, force=force))

        # Bundles
        l = self.elibrary
        for k,b_ident in self.bundles.items():
            b = l.get(b_ident.vid)

            if not b:
                from ..dbexceptions import NotFoundError
                raise NotFoundError("No bundle in library {} for  '{}' ".format(l.database.dsn, b_ident.vid))

            renderer = BundleDoc(root)
            extracts.append(maybe_render('doc/{}.html'.format(b_ident.vid), lambda: renderer.render(self,b), force=force))

        # Partitions

        # Tables
        for t in self.tables:
            renderer = Tables(root)
            extracts.append(maybe_render('doc/{}.html'.format(t.vid), lambda: renderer.render_table(self, t), force=force ))


        extracts.append(maybe_render('index.html', lambda: WarehouseIndex(root).render(self), force=force))

        extracts.append(maybe_render('css/style.css', lambda: Renderer(root).css, force=force))

        extracts.append(maybe_render('toc.html', lambda: WarehouseIndex(root).render_toc(self, extracts), force=force))


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
