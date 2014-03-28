'''
REST Server For DataBundle Libraries. 
'''


from bottle import  error, hook, get, put, post, request, response, redirect
from bottle import HTTPResponse, static_file, install, url
from bottle import ServerAdapter, server_names, Bottle
from bottle import run, debug

from decorator import  decorator
from  ambry.library import new_library
import ambry.util
from ambry.bundle import DbBundle
import logging
import os
import json
from sqlalchemy.orm.exc import NoResultFound

import ambry.client.exceptions as exc

logger = ambry.util.get_logger(__name__)
logger.setLevel(logging.DEBUG)

#
# The LibraryPlugin allows the library to be inserted into a request handler with a
# 'library' argument. 
class LibraryPlugin(object):
    
    def __init__(self, library_creator, keyword='library'):

        self.library_creator = library_creator
        self.keyword = keyword

    def setup(self, app):
        pass

    def apply(self, callback, context):
        import inspect

        # Override global configuration with route-specific values.
        conf = context['config'].get('library') or {}
        
        #library = conf.get('library', self.library_creator())

        keyword = conf.get('keyword', self.keyword)
        
        # Test if the original callback accepts a 'library' keyword.
        # Ignore it if it does not need a database handle.
        args = inspect.getargspec(context['callback'])[0]
        if keyword not in args:
            return callback

        def wrapper(*args, **kwargs):

            #
            # NOTE! Creating the library every call. This is bacuase the Sqlite driver
            # isn't multi-threaded. 
            #
            kwargs[keyword] = self.library_creator()

            rv = callback(*args, **kwargs)

            return rv

        # Replace the route callback with the wrapped one.
        return wrapper
 
def capture_return_exception(e):
    
    import sys
    import traceback
    
    (exc_type, exc_value, exc_traceback) = sys.exc_info() #@UnusedVariable
    
    tb_list = traceback.format_list(traceback.extract_tb(sys.exc_info()[2]))
    
    return {'exception':
     {'class':e.__class__.__name__, 
      'args':e.args,
      'trace': tb_list
     }
    }   

def _CaptureException(f, *args, **kwargs):
    '''Decorator implementation for capturing exceptions '''

    try:
        r =  f(*args, **kwargs)
    except HTTPResponse:
        raise # redirect() uses exceptions
    except Exception as e:
        r = capture_return_exception(e)
        if hasattr(e, 'code'):
            response.status = e.code
        else:
            response.status = 500

    return r

def CaptureException(f, *args, **kwargs):
    '''Decorator to capture exceptions and convert them
    to a dict that can be returned as JSON ''' 

    return decorator(_CaptureException, f) # Preserves signature

class AllJSONPlugin(object):
    '''A copy of the bottle JSONPlugin, but this one tries to convert
    all objects to json ''' 
    
    from json import dumps as json_dumps
    
    name = 'json'
    remote  = 2

    def __init__(self, json_dumps=json_dumps):
        self.json_dumps = json_dumps

    def apply(self, callback, context):
      
        dumps = self.json_dumps
        if not dumps: return callback
        def wrapper(*a, **ka):
            rv = callback(*a, **ka)

            if isinstance(rv, HTTPResponse ):
                return rv

            if isinstance(rv, basestring ):
                return rv

            #Attempt to serialize, raises exception on failure
            try:
                json_response = dumps(rv)
            except Exception as e:
                r =  capture_return_exception(e)
                json_response = dumps(r)
                
            #Set content type only if serialization succesful
            response.content_type = 'application/json'
            return json_response
        return wrapper

install(AllJSONPlugin())


@error(404)
@CaptureException
def error404(error):
    raise exc.NotFound("For url: {}".format(repr(request.url)))

@error(500)
def error500(error):
    raise exc.InternalError("For Url: {}".format(repr(request.url)))

@hook('after_request')
def close_library_db():
    pass

@hook('after_request')
def enable_cors():
    response.headers['Access-Control-Allow-Origin'] = '*'
    
def _host_port(library):

    urlh = library.urlhost

    if not urlh.startswith('http'):
        urlh = "http://"+urlh

    return '{}'.format(urlh)
    #return  'http://{}{}'.format(library.host, ':'+str(library.port) if library.port != 80 else '')


def process_did(did, library):
    from ..identity import ObjectNumber, DatasetNumber

    try:
        d_on = ObjectNumber.parse(did)
    except ValueError:
        raise exc.BadRequest("Could not parse dataset id".format(did))

    if not isinstance(d_on, DatasetNumber):
        raise exc.BadRequest("Not a valid dataset number {}".format(did))

    b =  library.get(did)

    if not b:
        raise exc.BadRequest("Didn't get bundle for {}".format(did))


    return did, d_on, b

def process_pid(did, pid, library):
    from ..identity import ObjectNumber, PartitionNumber


    try:
        p_on = ObjectNumber.parse(pid)
    except ValueError:
        raise exc.BadRequest("Could not parse dataset id".format(did))

    if not isinstance(p_on, PartitionNumber):
        raise exc.BadRequest("Not a valid partition number {}".format(did))

    b =  library.get(did)

    if not b:
        raise exc.BadRequest("Didn't get bundle for {}".format(did))

    p_orm = b.partitions.find_id(pid)

    if not p_orm:
        raise exc.BadRequest("Partition reference {} not found in bundle {}".format(pid, did))

    return pid, p_on, p_orm

def _get_ct(typ):
    ct = ({'application/json':'json',
          'application/x-yaml':'yaml',
          'text/x-yaml':'yaml',
          'text/csv':'csv'}
          .get(request.headers.get("Content-Type"), None))

    if ct is None:
        try:
            _, ct = typ.split('.',2)
        except:
            ct = 'json'

    return ct

def _table_csv_parts(library,b,pid,table=None):
    # This partition does not have CSV parts, so we'll have to make them.
    parts = []

    TARGET_ROW_COUNT = 50000

    # For large partitions, this could be really slow, and
    # can cause the server to run out of disk space.
    p = library.get(pid).partition

    if not table:
        table = p.table # Use the default table


    count = p.query("SELECT count(*) FROM {}".format(table.name)).fetchone()

    if count:
        count = count[0]
    else:
        raise exc.BadRequest("Failed to get count of number of rows")

    part_count, rem = divmod(count, TARGET_ROW_COUNT)

    template = ("{}/datasets/{}/partitions/{}/tables/{}/csv"
                .format(_host_port(library),b.identity.vid,
                        p.identity.vid, table.id_))

    if part_count == 0:
        parts.append(template)
    else:
        for i in range(1, part_count+1):
            parts.append(template +"?i={}&n={}".format(i,part_count))

    return parts

def _read_body(request):
    '''Read the body of a request and decompress it if required '''
    # Really important to only call request.body once! The property method isn't
    # idempotent!
    import zlib
    import uuid # For a random filename.
    import tempfile

    tmp_dir = tempfile.gettempdir()
    #tmp_dir = '/tmp'

    file_ = os.path.join(tmp_dir,'rest-downloads',str(uuid.uuid4())+".db")
    if not os.path.exists(os.path.dirname(file_)):
        os.makedirs(os.path.dirname(file_))

    body = request.body # Property acessor

    # This method can recieve data as compressed or not, and determines which
    # from the magic number in the head of the data.
    data_type = ambry.util.bundle_file_type(body)
    decomp = zlib.decompressobj(16+zlib.MAX_WBITS) # http://stackoverflow.com/a/2424549/1144479

    if not data_type:
        raise Exception("Bad data type: not compressed nor sqlite")

    # Read the file directly from the network, writing it to the temp file,
    # and uncompressing it if it is compressesed.
    with open(file_,'w') as f:

        chunksize = 8192
        chunk =  body.read(chunksize) #@UndefinedVariable
        while chunk:
            if data_type == 'gzip':
                f.write(decomp.decompress(chunk))
            else:
                f.write(chunk)
            chunk =  body.read(chunksize) #@UndefinedVariable

    return file_

def _download_redirect(identity, library):
    '''This is very similar to get_key'''
    from ambry.cache import RemoteMarker
    from ambry.dbexceptions import NotFoundError

    if library.upstream:
        remote = library.upstream.get_upstream(RemoteMarker)
        if not remote:
            logger.error("Library remote does not have a proper upstream")
    else:
        remote = None

    try:
        return remote.path(identity.cache_key)
    except NotFoundError:
        logger.warn("Object not found in upstream. Return local URL; {}".format(identity.fqname))


    return redirect("{}/files/{}".format(_host_port(library), identity.cache_key))


def _send_csv_if(did, pid, table, library):
    '''Send csv function, with a web-processing interface '''
    did, _, _ = process_did(did, library)
    pid, _, _  = process_pid(did, pid, library)

    p = library.get(pid).partition # p_orm is a database entry, not a partition

    i = int(request.query.get('i',1))
    n = int(request.query.get('n',1))
    sep = request.query.get('sep',',')

    where = request.query.get('where',None)

    return _send_csv(library, did, pid, table, i, n, where, sep)

def _send_csv(library, did, pid, table, i, n, where, sep=',' ):
    '''Send a CSV file to the client. '''
    import unicodecsv
    import csv
    from StringIO import StringIO
    from sqlalchemy import text

    p = library.get(pid).partition # p_orm is a database entry, not a partition


    if not table:
        table = p.table

    if i > n:
        raise exc.BadRequest("Segment number must be less than or equal to the number of segments")

    if i < 1:
        raise exc.BadRequest("Segment number starts at 1")


    count = p.query("SELECT count(*) FROM {}".format(table.name)).fetchone()

    if count:
        count = count[0]
    else:
        raise exc.BadRequest("Failed to get count of number of rows")

    base_seg_size, rem = divmod(count, int(n))

    if i == n:
        seg_size = base_seg_size + rem
    else:
        seg_size = base_seg_size

    out = StringIO()
    #writer = unicodecsv.writer(out, delimiter=sep, escapechar='\\',quoting=csv.QUOTE_NONNUMERIC)
    writer = unicodecsv.writer(out, delimiter=sep)

    if request.query.header:
        writer.writerow(tuple([c.name for c in p.table.columns]))

    if where:
        q = "SELECT * FROM {} WHERE {} LIMIT {} OFFSET {} ".format(table.name, where, seg_size, base_seg_size*(i-1))

        params = dict(request.query.items())
    else:
        q = "SELECT * FROM {} LIMIT {} OFFSET {} ".format(table.name, seg_size, base_seg_size*(i-1))
        params = {}

    for row in p.query(text(q), params):
        writer.writerow(tuple(row))

    response.content_type = 'text/csv'

    response.headers["content-disposition"] = "attachment; filename='{}-{}-{}-{}.csv'".format(p.identity.vname,table.name,i,n)

    return out.getvalue()


@get('/')
def get_root(library):
    
    hp = _host_port(library)

    return {
           'datasets' : "{}/datasets".format(hp),
           'find': "{}/datasets/find".format(hp),
           'info': library.dict,
           'upstream': dict(
               options = library.upstream.last_upstream().options,
               bucket = library.upstream.last_upstream().bucket_name,
               prefix = library.upstream.last_upstream().prefix)
           }

def _resolve(library, ref):
    from ambry.orm import Dataset
    return library.resolver.resolve_ref_one(ref, location=[Dataset.LOCATION.LIBRARY,Dataset.LOCATION.UPSTREAM ])


@get('/resolve/<ref:path>')
@CaptureException
def get_resolve(library, ref):
    '''Resolve a name or other reference into an identity'''

    ip, dataset = _resolve(library, ref)

    if not dataset:
        return None

    if dataset.partition:
        return dataset.partition.dict

    return dataset.dict

@get('/info/<ref:path>')
@CaptureException
def get_info(ref, library):
    '''Resolve the reference, and redirect to the dataset or partition page'''
    from ambry.cache import RemoteMarker

    ip, dataset = _resolve(library, ref)

    if not dataset:
        raise exc.NotFound("No file for reference: {} ".format(ref))


    if dataset.partition:
        url = '{}/datasets/{}/partitions/{}'.format(_host_port(library),
                                                    dataset.vid,
                                                    dataset.partition.vid)
    else:
        url = '{}/datasets/{}'.format(_host_port(library), dataset.vid)

    return redirect(url)

@get('/files/<key:path>')
@CaptureException
def get_file(key, library):
    '''Download a file based on a cache key '''

    path = library.cache.get(key)
    metadata = library.cache.metadata(key)

    if not path:
        raise exc.NotFound("No file for key: {} ".format(key))

    if 'etag' in metadata:
        del metadata['etag']

    return static_file(path, root='/', download=key.replace('/','-'))

@get('/key/<key:path>')
@CaptureException
def get_key(key, library):
    from ambry.cache import RemoteMarker

    if library.upstream:

        remote = library.upstream.get_upstream(RemoteMarker)

        if not remote:
            raise exc.InternalError("Library remote does not have a proper upstream")

        url =  remote.path(key)

    else:
        url = "{}/files/{}".format(_host_port(library), key)


    return redirect(url)


@get('/datasets')
def get_datasets(library):
    '''Return all of the dataset identities, as a dict, 
    indexed by id'''

    from ..orm import Dataset

    return { dsid.cache_key : {
                 'identity': dsid.dict ,
                 'refs': {
                    'path': dsid.path,
                    'cache_key': dsid.cache_key
                 }, 
                 'urls': {
                          'partitions': "{}/datasets/{}".format(_host_port(library), dsid.vid),
                          'db': "{}/datasets/{}/db".format(_host_port(library), dsid.vid)
                          },
                  'schema':{
                    'json':'{}/datasets/{}/schema.json'.format(_host_port(library), dsid.vid),
                    'yaml':'{}/datasets/{}/schema.yaml'.format(_host_port(library), dsid.vid),
                    'csv':'{}/datasets/{}/schema.csv'.format(_host_port(library),dsid.vid),
                  }
                 } 
            for dsid in library.list(locations=[Dataset.LOCATION.LIBRARY,Dataset.LOCATION.UPSTREAM]).values()}

@post('/datasets/find')
def post_datasets_find(library):
    '''Post a QueryCommand to search the library. '''
    from ambry.library.query import QueryCommand
   
    q = request.json
   
    bq = QueryCommand(q)
    results = library.find(bq)

    out = []
    for r in results:
        out.append(r)
        
    return out

@post('/datasets/<did>')
@CaptureException
def post_dataset(did,library): 
    '''Accept a payload that describes a bundle in the remote. Download the
    bundle from the remote and install it. '''

    from ambry.identity import  Identity

    identity = Identity.from_dict(request.json)

    if not identity.md5:
        raise exc.BadRequest("The identity must have the md5 value set")

    if not did in set([identity.id_, identity.vid]):
        raise exc.Conflict("Dataset address '{}' doesn't match payload id '{}'".format(did, identity.vid))

    # need to go directly to remote, not library.get() because the
    # dataset hasn't been loaded yet. 
    db_path = library.load(identity.cache_key, identity.md5)

    if not db_path:
        logger.error("Failed to get {} from cache while posting dataset".format(identity.cache_key))
        logger.error("  cache =  {}".format(library.cache))
        logger.error("  remote = {}".format(library.upstream))
        raise exc.NotFound("Didn't  get bundle file for cache key {} ".format(identity.cache_key))

    logger.debug("Loading {} for identity {} ".format(db_path, identity))

    b = library.load(identity.cache_key, identity.md5)

    return b.identity.dict


@get('/datasets/<did>') 
@CaptureException   
def get_dataset(did, library, pid=None):
    '''Return the complete record for a dataset, including
    the schema and all partitions. '''

    gr =  library.get(did)
 
    if not gr:
        raise exc.NotFound("Failed to find dataset for {}".format(did))
    
    # Construct the response
    d = {'identity' : gr.identity.dict, 'partitions' : {}}


    files = library.files.query.installed.ref(gr.identity.vid).all
    
    # Get direct access to the cache that implements the remote, so
    # we can get a URL with path()
    #remote = library.remote.get_upstream(RemoteMarker)

    d['urls'] = dict(db = "{}/datasets/{}/db".format(_host_port(library), gr.identity.vid))
    
    if files and len(files) > 0:
        d['file'] = dict(
            ref = files[0].dict,
            config = gr.db_config.dict
        )

    if pid:
        partitions = [gr.partitions.partition(pid)]
    else:
        partitions = gr.partitions.all_nocsv

    for partition in  partitions:

        d['partitions'][partition.identity.id_] = dict()

        d['partitions'][partition.identity.id_]['identity'] = partition.identity.dict

        files = library.files.query.installed.ref(partition.identity.vid).all
        
        if len(files) > 0:
            file_ = files.pop(0)

            fd = file_.dict
            d['partitions'][partition.identity.id_]['file']  = { k:v for k,v in fd.items() if k in ['state'] }

        tables = {}
        for table_name in partition.tables:
            table = partition.bundle.schema.table(table_name)
  
            args = (_host_port(library), gr.identity.vid, 
                    partition.identity.vid, table.id_, table.name)
            whole_link = "{}/datasets/{}/partitions/{}/tables/{}/csv#{}".format(*args)
            parts_link = "{}/datasets/{}/partitions/{}/tables/{}/csv/parts#{}".format(*args)
            
            tables[table.id_] = {'whole': whole_link,'parts': parts_link}

        whole_link = "{}/datasets/{}/partitions/{}/csv#{}".format(_host_port(library), 
                        gr.identity.vid, partition.identity.vid, partition.table.name)

        if partition.identity.name.format == 'csv':
            db_link = whole_link
            parts_link = None
        else:
            db_link = "{}/datasets/{}/partitions/{}/db".format(_host_port(library), 
                            gr.identity.vid, partition.identity.vid)
            parts_link = "{}/datasets/{}/partitions/{}/csv/parts#{}".format(
                        _host_port(library), gr.identity.vid,
                        partition.identity.vid, partition.table.name)

        tables_link = "{}/datasets/{}/partitions/{}/tables".format(
                        _host_port(library), gr.identity.vid, partition.identity.vid)


        d['partitions'][partition.identity.id_]['urls'] ={
         'db': db_link,
         'tables': tables_link,
         'csv': {
            'whole':whole_link,
            'parts': parts_link,
            'tables': tables
          }
        }

    d['response'] = 'dataset'

    return d

@get('/datasets/<did>/csv') 
@CaptureException   
def get_dataset_csv(did, library, pid=None):
    '''List the CSV partitions for the dataset'''
    pass

@post('/datasets/<did>/partitions/<pid>') 
@CaptureException   
def post_partition(did, pid, library):
    from ambry.identity import  Identity
    from ambry.util import md5_for_file

    b =  library.get(did)

    if not b:
        raise exc.NotFound("No bundle found for id {}".format(did))

    payload = request.json
    identity = Identity.from_dict(payload['identity'])

    p = b.partitions.get(pid)
    
    if not p:
        raise exc.NotFound("No partition for {} in dataset {}".format(pid, did))

    if not pid in set([identity.id_, identity.vid]):
        raise exc.Conflict("Partition address '{}' doesn't match payload id '{}'".format(pid, identity.vid))

    library.database.add_remote_file(identity)

    return identity.dict



@get('/datasets/<did>/db') 
@CaptureException   
def get_dataset_file(did, library):
    from ambry.cache import RemoteMarker

    ident = library.resolve(did)
    
    if not ident:
        raise exc.NotFound("No dataset found for identifier '{}' ".format(did))

    return redirect(_download_redirect(ident, library))

@get('/datasets/<did>/<typ:re:schema\\.?.*>')
@CaptureException
def get_dataset_schema(did, typ, library):
    from ambry.cache import RemoteMarker

    ct = _get_ct(typ)

    b =  library.get(did)

    if not b:
        raise exc.NotFound("No bundle found for id {}".format(did))


    if ct == 'csv':
        from StringIO import StringIO
        output = StringIO()
        response.content_type = 'text/csv'
        b.schema.as_csv(output)
        static_file(output)
    elif ct == 'json':
        import json
        s = b.schema.as_struct()
        return s
    elif ct == 'yaml':
        import yaml
        s = b.schema.as_struct()
        response.content_type = 'application/x-yaml'
        return  yaml.dump(s)
    else:
        raise Exception("Unknown format" )

@get('/datasets/<did>/partitions/<pid>')
@CaptureException
def get_partition(did, pid, library):
    from ambry.cache import RemoteMarker

    d =  get_dataset(did, library, pid)
    d['response'] = 'partition'
    return d



@get('/datasets/<did>/partitions/<pid>/db')
@CaptureException
def get_partition_file(did, pid, library):
    from ambry.cache import RemoteMarker
    from ambry.identity import Identity

    b =  library.get(did)

    if not b:
        raise exc.NotFound("No bundle found for id {}".format(did))

    payload = request.json

    p = b.partitions.get(pid)

    if not p:
        raise exc.NotFound("No partition found for identifier '{}' ".format(pid))

    return redirect(_download_redirect(p.identity, library))

@get('/datasets/<did>/partitions/<pid>/tables')
@CaptureException
def get_partition_tables(did, pid, library):
    '''
    '''


    did, _, _ = process_did(did, library)
    pid, _, _  = process_pid(did, pid, library)

    p = library.get(pid).partition # p_orm is a database entry, not a partition

    o = {}

    for table_name in p.tables:
        table = p.bundle.schema.table(table_name)
        d = table.dict

        args = (_host_port(library), did, pid, table.id_)
        csv_link = "{}/datasets/{}/partitions/{}/tables/{}/csv".format(*args)
        parts_link = "{}/datasets/{}/partitions/{}/tables/{}/csv/parts".format(*args)

        d['urls'] = {
            'csv':{
                'csv': csv_link,
                'parts': parts_link
            }
        }

        o[table.name] = d
    return o

@get('/datasets/<did>/partitions/<pid>/tables/<tid>/csv')
@CaptureException
def get_partition_table_csv(did, pid, tid, library):
    '''
    '''

    did, _, _ = process_did(did, library)
    pid, _, _  = process_pid(did, pid, library)

    p = library.get(pid).partition # p_orm is a database entry, not a partition

    table = p.bundle.schema.table(tid)

    if table == p.table:
        pass

    return _send_csv_if(did, pid, table, library)

@get('/datasets/<did>/partitions/<pid>/tables/<tid>/csv/parts')
@CaptureException
def get_partition_table_csv_parts(did, pid, tid, library):
    '''
    '''
    from ..partition.csv import CsvPartitionName
    from ..partition.geo import GeoPartitionName
    from ..partition.sqlite import SqlitePartitionName

    did, _, _ = process_did(did, library)
    pid, _, _  = process_pid(did, pid, library)

    p = library.get(pid).partition # p_orm is a database entry, not a partition

    if p.identity.format == CsvPartitionName.FORMAT:
        return ['/datasets/{}/partitions/{}/db'.format(_host_port(library), pid, pid)]
    elif p.identity.format not in  (SqlitePartitionName.FORMAT, GeoPartitionName.FORMAT):
        raise exc.BadRequest("Can only get CSV parts for csv, geo or sqlite partitions. Got: {}".format(p.identity.format))


    table = p.bundle.schema.table(tid)

    if table == p.table:
        pass

    return _table_csv_parts(library,p.bundle,pid, table)

@get('/datasets/<did>/partitions/<pid>/csv')
@CaptureException
def get_partition_csv(did, pid, library):
    '''Stream as CSV, a  segment of the main table of a partition

    Query
        n: The total number of segments to break the CSV into
        i: Which segment to retrieve
        header:If existent and not 'F', include the header on the first line.

    '''

    return _send_csv_if(did, pid, None, library)

@get('/datasets/<did>/partitions/<pid>/csv/parts')
@CaptureException
def get_partition_csv_parts(did, pid, library):
    '''Return a set of URLS for optimal CSV parts of a partition'''

    did, d_on, b = process_did(did, library)
    pid, p_on, p_orm  = process_pid(did, pid, library)

    b = library.get(did)

    p = b.partitions.partition(p_orm)

    csv_parts = p.get_csv_parts()

    if len(csv_parts):
        # This partition has defined CSV parts, so we should link to those.

        parts = []

        for csv_p in sorted(csv_parts, key=lambda x: x.identity.segment):
            parts.append("{}/datasets/{}/partitions/{}/db#{}"
                         .format(_host_port(library), b.identity.vid,
                                 csv_p.identity.vid, csv_p.identity.segment))

        pass
    else:
        return _table_csv_parts(library,b,pid)

    return parts



#### Test Code

@get('/test/echo/<arg>')
def get_test_echo(arg):
    '''just echo the argument'''
    return  (arg, dict(request.query.items()))

@put('/test/echo')
def put_test_echo():
    '''just echo the argument'''

    return  (request.json, dict(request.query.items()))

@get('/test/exception')
@CaptureException
def get_test_exception():
    '''Throw an exception'''
    raise Exception("throws exception")

@put('/test/exception')
@CaptureException
def put_test_exception():
    '''Throw an exception'''
    raise Exception("throws exception")

@get('/test/isdebug')
def get_test_isdebug():
    '''eturn true if the server is open and is in debug mode'''
    try:
        global stoppable_wsgi_server_run
        if stoppable_wsgi_server_run is True:
            return True
        else:
            return False
    except NameError:
        return False

@post('/test/close')
@CaptureException
def get_test_close():
    '''Close the server'''
    global stoppable_wsgi_server_run
    if stoppable_wsgi_server_run is not None:
        logger.debug("SERVER CLOSING")
        stoppable_wsgi_server_run = False
        return True

    else:
        raise exc.NotAuthorized("Not in debug mode, won't close")

class StoppableWSGIRefServer(ServerAdapter):
    '''A server that can be stopped by setting the module variable
    stoppable_wsgi_server_run to false. It is primarily used for testing. '''

    def run(self, handler): # pragma: no cover
        global stoppable_wsgi_server_run
        stoppable_wsgi_server_run = True

        from wsgiref.simple_server import make_server, WSGIRequestHandler
        if self.quiet:
            class QuietHandler(WSGIRequestHandler):
                def log_request(*args, **kw): pass #@NoSelf
            self.options['handler_class'] = QuietHandler
        srv = make_server(self.host, self.port, handler, **self.options)
        while stoppable_wsgi_server_run:
            srv.handle_request()

server_names['stoppable'] = StoppableWSGIRefServer

def test_run(config):
    '''Run method to be called from unit tests'''
    from bottle import run, debug #@UnresolvedImport

    debug()

    port = config['port'] if config['port'] else 7979
    host = config['host'] if config['host'] else 'localhost'

    lf = lambda: new_library(config, True)

    l = lf()
    l.database.create()

    logger.info("Starting test server on http://{}:{}".format(host, port))
    logger.info("Library at: {}".format(l.database.dsn))

    install(LibraryPlugin(lf))

    return run(host=host, port=port, reloader=False, server='stoppable')

def local_run(config, reloader=False):

    global stoppable_wsgi_server_run
    stoppable_wsgi_server_run = None

    debug()

    lf = lambda:  new_library(config, True)

    l = lf()
    l.database.create()

    logger.info("starting local server for library '{}' on http://{}:{}".format(l.name, l.host, l.port))

    install(LibraryPlugin(lf))
    return run(host=l.host, port=l.port, reloader=reloader)

def local_debug_run(config):

    debug()

    port = config['port'] if config['port'] else 7979
    host = config['host'] if config['host'] else 'localhost'

    logger.info("starting debug server on http://{}:{}".format(host, port))

    lf = lambda: new_library(config, True)

    l = lf()
    l.database.create()

    install(LibraryPlugin(lf))

    return run(host=host, port=port, reloader=True, server='stoppable')

def production_run(config, reloader=False):

    lf = lambda:  new_library(config, True)


    l = lf()
    l.database.create()

    logger.info("starting production server for library '{}' on http://{}:{}".format(l.name, l.host, l.port))

    install(LibraryPlugin(lf))

    return run(host=l.host, port=l.port, reloader=reloader, server='paste')

if __name__ == '__main__':
    local_debug_run()


