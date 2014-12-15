from __future__ import absolute_import
from ..library import Library
from ..library.database import LibraryDb
from ckcache import new_cache, Cache
from ..database import new_database
import os
from ..util import Constant
from ambry.util import init_log_rate
from ..library.files import Files

class NullCache(Cache):
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


class WLibrary(Library):
    """Extends the Library class to remove the Location parameter on identity resolution"""

    def resolve(self, ref, location=None):

        return super(WLibrary, self).resolve(ref, location=location)


def new_warehouse(config, elibrary, logger=None):

    assert elibrary is not None

    service = config['service'] if 'service' in config else 'relational'

    if 'database' in config:
        db_config = dict(config['database'].items()) # making a copy so we can alter it.
    else:
        db_config = dict(config.items())

    database = new_database(db_config, class_='warehouse')

    # If the warehouse specifies a seperate external library, use it, otherwise, use the
    # warehouse datbase for the library
    library_database = LibraryDb(**config['library']) if 'library' in config else  LibraryDb(**db_config)

    # This library instance is only for the warehouse database.
    wlibrary = WLibrary(
        cache=NullCache(),
        database=library_database
    )

    args = dict(database=database, wlibrary=wlibrary, elibrary=elibrary, logger = logger )

    if service == 'sqlite':
        from .sqlite import SqliteWarehouse
        w = SqliteWarehouse(**args)

    elif service == 'spatialite':

        from .sqlite import SpatialiteWarehouse

        w = SpatialiteWarehouse(**args )

    elif service == 'postgres':
        from .postgres import PostgresWarehouse

        w = PostgresWarehouse(**args )

    elif service == 'postgis':
        from .postgis import PostgisWarehouse

        w = PostgisWarehouse(**args )

    else:
        raise Exception("Unknown warehouse type: {}".format(service))

    return w


class ResolutionError(Exception):
    pass

class WarehouseInterface(object):

    FILE_TYPE = Constant()

    FILE_TYPE.MANIFEST = 'manifest'
    FILE_TYPE.HTML = 'text/html'
    FILE_TYPE.EXTRACT = Files.TYPE.EXTRACT

    FILE_GROUP = Constant()

    FILE_GROUP.MANIFEST = Files.TYPE.MANIFEST
    FILE_GROUP.DOC = Files.TYPE.DOC


    def __init__(self,
                 database,
                 wlibrary=None, # Warehouse library
                 elibrary=None, # external Library
                 logger=None,
                 base_dir = None,
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

    def info(self, location, message=None):

        if not message:
            message = location
            location = None

        if location:
            self.logger.info("{}:{} {}", location[0], location[1], message)
        else:
            self.logger.info(message)


    def create(self):
        from datetime import datetime

        self.database.create()
        self.wlibrary.database.create()

        self._meta_set('created', datetime.now().isoformat())

    def clean(self):
        self.database.clean()
        self.wlibrary.clean()

    def delete(self):
        self.database.enable_delete = True
        self.database.drop()
        self.wlibrary.database.enable_delete = True
        self.wlibrary.database.drop()

    def exists(self):
        return self.database.exists()

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

    configurable = ('uid','title','name', 'summary','cache_path', 'url')

    @property
    def uid(self):
        """Title of the warehouse"""
        return self._meta_get('uid')

    @uid.setter
    def uid(self, v):
        return self._meta_set('uid', v)

    @property
    def title(self):
        """Title of the warehouse"""
        return self._meta_get('title')

    @title.setter
    def title(self, v):
        return  self._meta_set('title', v)


    @property
    def summary(self): # Everything else names this property summary
        """Short description of the warehouse"""
        return self._meta_get('summary')

    @summary.setter
    def summary(self, v):
        return self._meta_set('summary', v)

    @property
    def name(self):
        """name of the warehouse"""
        return self._meta_get('name')

    @name.setter
    def name(self, v):
        return self._meta_set('name', v)

    @property
    def cache_path(self):
        """Cache name for local publications. Usually a filesystem path"""

        return self._meta_get('cache_path')

    @cache_path.setter
    def cache_path(self, v):
        return self._meta_set('cache_path', v)


    @property
    def cache(self):

        cp = self.cache_path

        if not cp:
            return None

        if not os.path.isabs(cp) and not '://' in cp:
            cp = self.elibrary.warehouse_cache.path(cp, missing_ok = True)

        return new_cache(cp)

    @property
    def url(self):
        """Url of the management application for the warehouse. """
        return self._meta_get('url')

    @url.setter
    def url(self, v):

        return self._meta_set('url', v)

    @property
    def dict(self):
        """Return information about the warehouse as a dictionary. """
        from ..orm import Config as SAConfig
        from ..library.database import ROOT_CONFIG_NAME_V
        from ambry.warehouse.manifest import Manifest

        d =  {}

        for c in self.library.database.get_config_group('warehouse'):
            if c.key in self.configurable:
                d[c.key] = c.value

        d['dsn'] = self.database.dsn

        d['tables'] =  { t.vid:t.dict for t in self.library.tables }

        d['partitions'] = {p.vid: p.dict for p in self.library.partitions}

        d['manifests'] = {mf.ref: dict(mf.dict.items() + Manifest(mf.content).dict.items()) for mf in self.library.manifests}

        return d

    @property
    def manifests(self):
        """Return the parsed manifests that have been installed"""
        from .manifest import Manifest

        return self.library.files.query.type(self.FILE_TYPE.MANIFEST).group(self.FILE_GROUP.MANIFEST).all


    @property
    def bundles(self):
        """Metadata for bundles, each with the partitions that are installed here.

        This extracts the bundle information that is in the partitions list, but it requires
        that the add_bundle() method has been run first, because the manifest doesn't usually have access to
        a library
        """

        l =  self.library.list(with_partitions=True)

        for k, v in l.items():

            d = { e.key.replace('.','_'):e.value for e in self.library.database.get_bundle_values(k,'config')}
            v.data.update(d)

        return l

    @property
    def extracts(self):
        """Return an array of dicts of the extract files """

        for f in self.library.files.query.group(self.FILE_GROUP.MANIFEST).type(self.FILE_TYPE.EXTRACT).all:
            self.library.database.session.expunge(f)
            f.source_url = self.uid
            f.oid = None
            yield f

    @property
    def tables(self):
        from ..orm import Table

        for table in self.library.database.session.query(Table).all():
            yield table

    def orm_table(self, vid):
        from ..orm import Table

        return self.library.database.session.query(Table).filter(Table.vid == vid).first()

    def orm_table_by_name(self, name):
        from ..orm import Table

        return self.library.database.session.query(Table).filter(Table.name == name).first()

    def expand_table_deps(self):
        """Expand the information about table dependencies so that only leaf tables are included. """

        deps = {}

        for t in self.tables:

            deps[t.name] = [tn for tn in t.data.get('tc_names', []) if tn != t.name] + [t.altname]

            if t.altname:
                deps[t.altname] = [tn for tn in t.data.get('tc_names', []) if tn != t.altname]

        # Get rid of column names, which get into the set because the sqlparser does not distiguish
        # between column names and table names

        for table_name, t_deps in deps.items():
            deps[table_name] = [self.orm_table(tn) for tn in t_deps if tn in deps.keys()]

        return deps

    @property
    def partitions(self):
        from ..orm import Partition

        for p in self.library.database.session.query(Partition).all():
            yield p

    def partition(self, vid):
        from ..orm import Partition

        return self.library.database.session.query(Partition).filter(Partition.vid == vid).first()

    ##
    ## Installation
    ##

    def install(self, partition, tables=None, prefix=None):
        """Install a partition and the talbes in the partition"""
        from ..orm import Partition
        from sqlalchemy.exc import OperationalError

        results = dict(
            tables = {},
            partitions = {}
        )

        p_vid = self._to_vid(partition)

        p_orm = self.wlibrary.database.session.query(Partition).filter(Partition.vid == p_vid).first()

        if p_orm and p_orm.installed == 'y':
            self.logger.info("Skipping {}; already installed".format(p_orm.vname))
            return None, p_orm

        bundle, p = self._setup_install(p_vid)

        if p.identity.format not in ('db', 'geo'):
            self.logger.warn("Skipping {}; uninstallable format: {}".format(p.identity.vname, p.identity.format))
            return None, None;

        all_tables = self.install_partition(bundle, p, prefix=prefix)

        if not tables:
            tables = all_tables

        for source_table_name in tables:

            #
            # Compute the installation name, and an alial that does not have the version number
            dest_table_name, alias = self.augmented_table_name(p.identity, source_table_name)

            if isinstance(source_table_name, (list, tuple)):
                source_table_name, where = source_table_name
            else:
                where = None

            try:
                ##
                ## Copy the data to the destination table

                if p.identity.format == 'db':
                    self.elibrary.get(p.vid) # ensure it is local
                    itn = self.load_local(p, source_table_name, dest_table_name, where)
                else:
                    self.elibrary.get(p.vid)  # ensure it is local
                    itn = self.load_ogr(p, source_table_name, dest_table_name, where)


                t_vid = p.get_table(source_table_name).vid
                w_table = self.library.table(t_vid)

                # Create a table entry for the name of the table with the partition in it,
                # and link it to the main table record.
                proto_vid = w_table.vid
                self.install_table(dest_table_name, alt_name=alias,
                                   data=dict(type='installed', proto_vid=proto_vid))

                # Link the table name and the alias
                self.install_table_alias(dest_table_name, alias, proto_vid=proto_vid)

                self.library.database.mark_table_installed(p.get_table(source_table_name).vid, itn)

                assert self.augmented_table_name(p.identity, source_table_name)[0] == itn


            except OperationalError as e:
                self.logger.error("Failed to install table '{}': {}".format(source_table_name,e))
                raise

        self.library.database.mark_partition_installed(p_vid)

        return tables, p


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

        return tables

    def install_manifest(self, manifest, force = None, reset=False, level = 0):
        """Install the partitions and views specified in a manifest file """
        from ..dbexceptions import NotFoundError, ConfigurationError
        from datetime import datetime
        import os

        errors = []

        # Mark all of the files associated with the manifest, so if they aren't in the manifest
        # we can remove them.
        # TODO Should also do this for tables.
        for f in (self.library.files.query.type(self.library.files.TYPE.EXTRACT).source_url(manifest.uid)).all:
            f.state = 'delatable'
            self.library.files.merge(f)

        # Update the manifest with bundle information, since it doesn't normally have access to a library
        manifest.add_bundles(self.elibrary)

        # If the manifest doesn't have a title or description, get it fro the manifest.

        if reset or not self.title:
            self.title = manifest.title

        if (reset or not self.summary) and manifest.summary:
            self.summary = manifest.summary['summary_text'] # Just the first sentence.

        if (reset or not self._meta_get('cache_path')) and manifest.cache:
            self.cache_path = manifest.cache


        # Manifest data
        mf = self.wlibrary.files.install_manifest(manifest)

        ## First pass
        for line, section in manifest.sorted_sections:

            tag = section.tag

            if tag in ('partitions', 'sql', 'index', 'mview', 'view'):
                self.logger.info("== Processing manifest '{}' section '{}' at line {}"
                                 .format(manifest.path, section.tag, section.linenumber))

            if tag == 'partitions':
                for pd in section.content['partitions']:
                    try:

                        tables = pd['tables'] # Tables that were specified on the parittion line; install only these

                        if pd['where'] and len(tables) == 1:
                            tables = [(pd['tables'][0], "WHERE (" + pd['where'] + ")")]

                        tables, p = self.install(pd['partition'], tables)

                        if p:
                            # Link the partition to the manifest. Have to re-fetch, because p is in the
                            # external library, and the manifest is in the warehouse elibrary
                            p = self.wlibrary.partition(p.vid)
                            mf.link_partition(p)
                            p.link_manifest(mf)

                            if tables:
                                for table in tables:
                                    b = self.wlibrary.bundle(p.identity.as_dataset().vid)
                                    orm_t = b.schema.table(table)

                                    mf.link_table(orm_t)
                                    orm_t.link_manifest(mf)

                    except NotFoundError:
                        self.logger.error("Partition {} not found in external library".format(pd['partition']))

                self.wlibrary.database.session.commit()

            elif tag == 'sql':
                sql = section.content

                if self.database.driver in sql:
                    self.run_sql(sql[self.database.driver])

            elif tag == 'index':
                c = section.content
                self.create_index(c['name'], c['table'], c['columns'])

            elif tag == 'mview':

                self.install_material_view(section.args, section.content['text'], clean= force,
                                           data=dict(
                                               tc_names=section.content['tc_names'],
                                               summary = section.doc.get('summary_text','') if section.doc else '',
                                               doc=section.doc,
                                               manifests = [manifest.uid],
                                               sql_formatted = section.content['html']
                                           ))

            elif tag == 'view':
                try:
                    self.install_view(section.args, section.content['text'],
                                      data = dict(
                                          tc_names = section.content['tc_names'],
                                          summary=section.doc.get('summary_text','') if section.doc else '',
                                          doc=section.doc,
                                          manifests=[manifest.uid],
                                          sql_formatted=section.content['html']
                                      ))
                except Exception as e:
                    errors.append((section, e))
                    self.logger.error("Failed to install view {}: {}".format(section.args, e))
                    raise


            elif tag == 'extract':

                d = section.content
                doc = manifest.doc_for(section)
                if doc:
                    d['doc'] = doc.content['html']

                extract_path = os.path.join('extracts',  d['rpath'])

                self.wlibrary.files.install_extract(extract_path, manifest.uid, d)

            elif tag == 'include':
                from .manifest import Manifest
                m = Manifest(section.content['path'])
                self.install_manifest(m, force = force, level = level + 1)

        self._meta_set(manifest.uid, datetime.now().isoformat())

        # Delete all of the files ( extracts ) that were note in-installed
        (self.library.files.query.type(self.library.files.TYPE.EXTRACT).state('delatable')).delete()

        if errors:
            self.logger.error("")
            self.logger.error("===== Install Errors =====")
            for section, e  in errors:
                self.logger.error("Failed to install view {}: at {}\n{}".format(section.args, section.file_line,  e))
                self.logger.error('----------')

        if level == 0:
            self.post_install()

        if hasattr(self.database, 'path') and os.path.exists(self.database.path):
            return self.database.path
        else:
            return self.database.dsn

    def post_install(self):
        """
        Perform operations after the manifest install, such as creating table views for
        all of the installed tables.

        For each table, it also installs a vid-based view, which replaces all of the column
        names with their vid. This allows for tracing columns through views, linking
        them back to their source

        """
        from ..orm import Column


        # TODO, our use of sqlalchemy is wacked.
        # Some of the install methods commit or flush the session, which invalidated the tables from self.tables,
        # so we have to get just the vid, and look up the object in each iteration.

        for t_vid in [ t.vid for t in self.tables]:

            t= self.orm_table(t_vid)

            if  t.type == 'table' and t.installed: # Get the table definition that columns are linked to

                ## Create table aliases for the vid of the tables.
                installed_tables = [ it for it in self.library.derived_tables(t.vid) if it.type == 'installed' ]

                # col_names = t.vid_select() # Get to this later ...

                col_names = '*'

                if len(installed_tables) == 1:
                    sql = "SELECT {} FROM {} ".format(col_names, installed_tables[0].name)

                else:

                    sql = "SELECT {} FROM ({}) ".format(
                            col_names,
                            ' UNION '.join(' SELECT * FROM {} '.format(table.name)
                                         for table in installed_tables )
                    )



                self.install_view(t_vid, sql, data=dict(type='alias', proto_vid=t_vid ))

                self.install_table(t_vid, data=dict(type='alias', proto_vid=t_vid ))



        s = self.library.database.session

        for t in self.tables:
            if  t.type == 'table' and t.installed:

                for dt in sorted(self.library.derived_tables(t.vid), key=lambda x:x.name):

                    t.add_installed_name(dt.name)
                    s.add(t)

        s.commit()

        for t in [ t for t in self.tables if t.type in ('view','mview') ]:

            sql = "SELECT * FROM {} LIMIT 1".format(t.name)

            for row in self.database.connection.execute(sql):
                pass
                #print [ (k,Column.convert_python_type(type(v))) for k,v in row.items()]

    def install_material_view(self, name, sql, clean = False, data=None):
        raise NotImplementedError(type(self))


    def _install_material_view(self, name, sql, clean=False, data=None):

        import time

        if not (clean or self.mview_needs_update(name, sql)):
            self.logger.info('Skipping materialized view {}: update not required'.format(name))
            return False, False
        else:
            self.logger.info('Installing materialized view {}'.format(name))

            if not self.orm_table_by_name(name):
                self.logger.info('mview_remove {}'.format(name))
                drop = True
            else:
                drop = False


        data = data if data else {}

        data['sql'] = sql
        data['type'] = 'mview'
        data['updated'] = time.time()

        return drop, data


    def mview_needs_update(self, name, sql):
        """Return True if an mview needs to be regnerated, because it's SQL changed,
         or one of its predecessors was re-generated

         NOTE. This probably only works property when the MVIEWS are listed in the manifest in an order
         where dependent views are listed after depenencies.
         """

        t = self.orm_table_by_name(name)

        if not t:
            return True

        if t.data.get('sql') != sql:
            return True

        update_time = int(t.data.get('updated', None))

        if not update_time:
            return True

        if t:
            for tc_name in t.data.get('tc_names'):
                tc = self.orm_table_by_name(tc_name)

                if (tc and tc.dict.get('updated',False)
                    and ( int(tc.dict.get('updated')) > int(t.data.get('updated')))):
                    return True

        return  False

    def install_view(self, name, sql, data=None):
        raise NotImplementedError(type(self))


    def install_table_alias(self, table_name, alias, proto_vid = None):
        """Install a view that allows referencing a table by another name """
        self.install_view(alias, "SELECT * FROM \"{}\" ".format(table_name),
                          data = dict(type='alias',proto_vid=proto_vid))

    def install_table(self, name, alt_name = None, data = None ):
        """Install a view, mview or alias as a Table record. Real tables are copied """

        from ..orm import Table, Config, Dataset
        from ..library.database import ROOT_CONFIG_NAME_V
        from sqlalchemy import func
        from sqlalchemy.orm.exc import NoResultFound

        s = self.library.database.session

        try:
            from sqlalchemy.orm import lazyload

            q = (s.query(Table).filter(Table.d_vid == ROOT_CONFIG_NAME_V, Table.name == name )
                 .options(lazyload('columns')))

            t = q.one()

        except NoResultFound:

            ds = s.query(Dataset).filter(Dataset.vid == ROOT_CONFIG_NAME_V).one()

            q = ( s.query(func.max(Table.sequence_id)) .filter(Table.d_vid == ROOT_CONFIG_NAME_V))

            seq = q.one()[0]

            seq = 0 if not seq else seq

            seq += 1

            t = Table(ds,name=name, sequence_id = seq, preserve_case = True)

        if alt_name is not None:
            t.altname = str(alt_name)

        if data and 'type' in data:
            t.type = data['type']
            del data['type']

        if data and 'summary' in data:
            if not t.description:
                t.description = data['summary']
            del data['summary']

        if data and 'proto_vid' in data:
            if not t.proto_vid:
                t.proto_vid = data['proto_vid']
            del data['proto_vid']


        if t.data:
            d = dict(t.data.items())
            d.update(data if data else {})
            t.data = d
        else:
            t.data = data

        t.installed = 'y'

        s.merge(t)
        s.commit()


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

    def load_ogr(self, partition, source_table_name, dest_table_name, where):
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


    def extract(self, force=False):
        """Generate the extracts and return a struture listing the extracted files. """
        from contextlib import closing

        from .extractors import new_extractor
        import time
        from ..util import md5_for_file

        # Get the URL to the root. The public_utl arg only affects S3, and gives a URL without a signature.
        root = self.cache.path('', missing_ok = True, public_url = True)

        extracts = []

        # Generate the file etracts

        for f in self.library.files.query.group('manifest').type('extract').all:

            t = self.orm_table_by_name(f.data['table'])

            if (t and t.data.get('updated') and
                f.modified and
                int(t.data.get('updated')) > f.modified) or (not f.modified):
                force = True


            ex = new_extractor(f.data.get('format'), self, self.cache, force=force)

            e = ex.extract(f.data['table'], self.cache, f.path )

            extracts.append(e)

            if e.time:
                f.modified = e.time

                if os.path.exists(e.abs_path):
                    f.hash = md5_for_file(e.abs_path)
                    f.size = os.path.getsize(e.abs_path)


                self.library.files.merge(f)



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

        name = identity.vid.replace('/', '_') + '_' + table_name

        if identity.grain:
            name = name + '_' + identity.grain

        alias = identity.id_.replace('/', '_') + '_' + table_name

        if identity.grain:
            alias = alias + '_' + identity.grain


        return name, alias

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
    from ..dbexceptions import ConfigurationError


    parts = urlparse.urlparse(db)

    path = parts.path

    scheme = parts.scheme

    if '+' in scheme:
        scheme, _ = scheme.split('+',1)

    if scheme in ('sqlite', "spatialite"):
        # Sqlalchemy expects 4 slashes for absolute paths, 3 for relative,
        # which is hard to manage reliably. So, fixcommon problems.

        if parts.netloc or path[0] != '/':
            raise ConfigurationError('DSN Parse error. For Sqlite and Sptialite, the DSN should have 3 or 4 slashes')

        path = path[1:]

        if path[0] != '/':
            path = os.path.join(base_dir, path)


    if scheme == 'sqlite':
        config = dict(service='sqlite', database=dict(dbname=os.path.join(base_dir,path), driver='sqlite'))

    elif scheme == 'spatialite':

        config = dict(service='spatialite', database=dict(dbname=os.path.join(base_dir,path), driver='spatialite'))

    elif scheme == 'postgres' or scheme == 'postgresql':
        config = dict(service='postgres',
                      database=dict(driver='postgres',
                                    server=parts.hostname,
                                    username=parts.username,
                                    password=parts.password,
                                    dbname=path.strip('/')
                      ))

    elif scheme == 'postgis':
        config = dict(service='postgis',
                      database=dict(driver='postgis',
                                    server=parts.hostname,
                                    username=parts.username,
                                    password=parts.password,
                                    dbname=parts.path.strip('/')
                      ))
    else:
        raise ValueError("Unknown database connection scheme for  {}".format(db))

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
