from __future__ import absolute_import
from ..library import Library
from ..library.database import LibraryDb
from ..cache import new_cache, CacheInterface
from ..database import new_database


class NullCache(CacheInterface):
    def has(self, rel_path, md5=None, use_upstream=True):
        return False


class NullLogger(object):
    def __init__(self):
        pass

    def progress(self, type_, name, n, message=None):
        pass

    def log(self, message):
        pass

    def error(self, message):
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
        database=library_database,
        upstream=None)


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
                 logger=None):

        assert wlibrary is not None
        assert elibrary is not None
        assert database is not None

        self.database = database
        self.wlibrary = wlibrary
        self.elibrary = elibrary

        self.logger = logger if logger else NullLogger()

    def create(self):
        self.database.create()
        self.wlibrary.database.create()

    def clean(self):
        self.database.clean()

    def delete(self):
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

    def install(self, partition):

        p_vid = self._to_vid(partition)

        bundle, p, tables = self._setup_install(p_vid)

        if p.identity.format == 'db':
            self.install_partition(bundle, p)
            for table_name, urls in tables.items():

                if urls:
                    self.load_remote(p, table_name, urls)
                else:
                    self.load_local(p, table_name)

        elif p.identity.format == 'geo':
            self.install_partition(bundle, p)
            for table_name, urls in tables.items():
                self.load_ogr(p, table_name)
        else:
            self.logger.warn("Skipping {}; uninstallable format: {}".format(p.identity.vname, p.identity.format))


    def install_partition(self, bundle, partition):
        '''Install the records for the partition, the tables referenced by the partition,
        and the bundle, if they aren't already installed'''
        from sqlalchemy.orm.exc import NoResultFound

        ld = self.library.database

        pid = self._to_vid(partition)

        ld.install_partition(bundle, pid)

        p = bundle.partitions.get(pid)

        for table_name in p.tables:
            self.create_table(p, table_name)


    def load_local(self, partition, table_name):
        '''Load data using a network connection to the warehouse and
        INSERT commands'''
        raise NotImplementedError()

    def load_remote(self, partition, table_name, urls):
        '''Load data by streaming from the remote REST interface to a bulk load
        facility of the target warehouse'''
        raise NotImplementedError()

    def load_ogr(self, partition, table_name):
        '''Load geo data using the ogr2ogr program'''
        raise NotImplementedError()


    def _setup_install(self, ref):
        '''Perform local and remote resolutions to get the bundle, partition and links
        to CSV parts in the remote REST itnerface '''
        from ..identity import Identity

        ri = RestInterface()

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

        rident = self.elibrary.remote_resolver.resolve(ident)

        table_urls = {}

        for table_name in p.tables:
            t = b.schema.table(table_name)

            if rident:
                import requests
                from ..client.exceptions import BadRequest
                # If we got an rident, the remotes were defined, and we can get the CSV urls
                # to load the table.

                try:
                    table_urls[table_name] = ri.get(rident.data['csv']['tables'][t.id_]['parts'])
                except BadRequest:
                    table_urls[table_name] = None



            else:
                table_urls[table_name] = None

        return b, p, table_urls


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


    def augmented_table_name(self, d_vid, table_name):
        return d_vid.replace('/', '_') + '_' + table_name

    def is_augmented_name(self, d_vid, table_name):

        return table_name.startswith(d_vid.replace('/', '_') + '_')

    def _ogr_args(self, partition):
        '''Return a arguments for ogr2ogr to connect to the database'''
        raise NotImplementedError()


    def info(self):
        config = self.config.to_dict()

        if 'password' in config['database']: del config['database']['password']
        return config




class RestInterface(object):


    def _handle_status(self, r):
        import exceptions

        if r.status_code >= 300:

            try:
                o = r.json()
            except:
                o = None

            if isinstance(o, dict) and 'exception' in o:
                e = self._handle_exception(o)
                raise e

            if 400 <= r.status_code < 500:
                raise exceptions.NotFound("Failed to find resource for URL: {}".format(r.url))

            r.raise_for_status()


    def _handle_return(self, r):
        if r.headers.get('content-type', False) == 'application/json':
            self.last_response = r
            return r.json()
        else:
            return r

    def _handle_exception(self, object):
        '''If self.object has an exception, re-construct the exception and
        return it, to be raised later'''

        import types, sys

        field = object['exception']['class']

        pre_message = ''
        try:
            class_ = getattr(sys.modules['ambry.client.exceptions'], field)
        except AttributeError:
            pre_message = "(Class: {}.) ".format(field)
            class_ = Exception

        if not isinstance(class_, (types.ClassType, types.TypeType)):
            pre_message = "(Class: {},) ".format(field)
            class_ = Exception

        args = object['exception']['args']

        # Add the pre-message, if the real exception type is not known.
        if isinstance(args, list) and len(args) > 0:
            args[0] = pre_message + str(args[0])

        # Add the trace
        try:
            if args:
                args[0] = args[0] + "\n---- Server Trace --- \n" + str('\n'.join(object['exception']['trace']))
            else:
                args.append("\n---- Server Trace --- \n" + str('\n'.join(object['exception']['trace'])))
        except Exception as e:
            print "Failed to augment exception. {}, {}".format(args, object)

        return class_(*args)


    def get(self, url, params={}):
        import requests

        r = requests.get(url, params=params)

        self._handle_status(r)

        return self._handle_return(r)
