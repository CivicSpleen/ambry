"""


Copyright (c) 2014 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


from bottle import  error, hook, get, put, post, request, response, redirect
from bottle import HTTPResponse, static_file, install, url, local
from bottle import  Bottle
from bottle import run, debug #@UnresolvedImport

from decorator import decorator #@UnresolvedImport
import logging

import ambry.client.exceptions as exc
import ambry.util
from  ambry.library import new_library

from ..text import Renderer

global_logger = ambry.util.get_logger(__name__)
global_logger.setLevel(logging.DEBUG)

# Alternative number spaces, mostly for manifests and databases
# The main number space for datasets is 'd'
NUMBER_SPACES = ('m','x','b')

def capture_return_exception(e):
    
    import sys
    import traceback
    
    (exc_type, exc_value, exc_traceback) = sys.exc_info() #@UnusedVariable
    
    tb_list = traceback.format_list(traceback.extract_tb(sys.exc_info()[2]))

    return "Exception: "+str(e)

    return {'exception':
     {'class':e.__class__.__name__, 
      'args':e.args,
      'trace': "\n".join(tb_list)
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


class RendererPlugin(object):

    keyword = 'renderer'

    def __init__(self, cache, library_cb):

        self.cache = cache

        self.library_cb = library_cb

    @property
    def library(self):
        """Return a new library. COnstructed for every request because Sqlite idsn't multi-threaded
        Should be able to cache for other databases """
        return self.library_cb()

    @property
    def renderer(self):
        return Renderer(self.cache, self.library, root_path = '')

    def setup(self, app):
        pass

    def apply(self, callback, context):
        import inspect

        # Test if the original callback accepts a 'library' keyword.
        # Ignore it otherwise
        args = inspect.getargspec(context['callback'])[0]
        if self.keyword not in args:
            return callback

        def wrapper(*args, **kwargs):
            kwargs[self.keyword] = self

            return callback(*args, **kwargs)

        # Replace the route callback with the wrapped one.
        return wrapper


@error(404)
@CaptureException
def error404(error):
    raise exc.NotFound("For url: {}".format(repr(request.url)))

@error(500)
def error500(error):
    raise exc.InternalError("For Url: {}".format(repr(request.url)))

@hook('after_request')
def enable_cors():
    response.headers['Access-Control-Allow-Origin'] = '*'


@get('/css/<name>')
def css_file(name, renderer):
    return static_file(name, root=renderer.renderer.css_dir)

@get('/')
def get_root(renderer):
    return renderer.renderer.index()

@get('/bundles/<vid>.html')
def get_bundle(vid, renderer):

    b = renderer.library.get(vid)

    return renderer.renderer._bundle_main(b)

@get('/bundles')
def get_bundles(renderer):

    return renderer.renderer.bundles_index()


@get('/bundles/<bvid>/tables/<tvid>.html')
def get_table(bvid, tvid, renderer):

    b = renderer.library.get(bvid)

    t = b.schema.table(tvid)

    return renderer.renderer.table(b,t)

@get('/bundles/<bvid>/partitions/<pvid>.html')
def get_partitions(bvid, pvid, renderer):

    b = renderer.library.get(bvid)

    p = b.partitions.get(pvid)

    return renderer.renderer.partition(b,p)

def _run( host, port,  reloader=False, **kwargs):

    return run( host=host, port=port, reloader=reloader, server='paste')
    
if __name__ == '__main__':
    import argparse
    from ambry.run import  get_runconfig
    from ambry.cache import new_cache, parse_cache_string

    rc = get_runconfig()

    d = rc.servers('numbers', {'host': 'localhost', 'port': 8080, 'cache': '/tmp/ambry-doc-cache'})

    try:
        d = d.to_dict()
    except:
        pass


    parser = argparse.ArgumentParser(prog='python -mambry.server.documentation',
                                     description='Run an Ambry documentation server')

    parser.add_argument('-H', '--host', default=None,
                        help="Server host. Defaults to configured value: {}".format(d['host']))
    parser.add_argument('-p', '--port', default=None,
                        help="Server port. Defaults to configured value: {}".format(d['port']))
    parser.add_argument('-c', '--cache', default=None,
                        help="Generated file cache. Defaults to configured value: {}".format(d['cache']))

    args = parser.parse_args()

    if args.port:
        d['port'] = args.port

    if args.host:
        d['host'] = args.host

    if args.cache:
        d['cache'] = args.cache

    lf = lambda: new_library(rc.library('default'), True)
    l = lf()
    l.database.create()

    global_logger.info("Library at: {}".format(l.database.dsn))

    config = parse_cache_string(d['cache'])
    cache = new_cache(config, run_config=rc)




    _run( **d)
    

