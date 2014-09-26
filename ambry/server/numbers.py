"""

Server application for assigning dataset numbers. Requires a redis instance for
data storage.

Run with: python -mambry.server.numbers

Requires a run_config configuration item:

numbers:
    host: gala
    port: 7977
    redis:
        host: redis
        port: 6379

For Clients:

numbers:
    key: this-is-a-long-uid-key


The key is a secret key that the client will use to assign an assignment class.
The two classes are 'authoritative' and 'registered' Only central authority operators
( like Clarinova ) should use the authoritative class. Other users can use the
'registered' class. Without a key and class assignment, the callers us the 'unregistered' class.

Set the assignment class with the redis-cli:

    set assignment_class:this-is-a-long-uid-key authoritative

There is only one uri to call:

    /next

It returns a JSON dict, with the 'number' key mapping to the number.

Copyright (c) 2014 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


from bottle import  error, hook, get, put, post, request, response, redirect
from bottle import HTTPResponse, static_file, install, url
from bottle import  Bottle
from bottle import run, debug #@UnresolvedImport

from decorator import decorator #@UnresolvedImport
import logging

import ambry.client.exceptions as exc
import ambry.util


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
    
    return {'exception':
     {'class':e.__class__.__name__, 
      'args':e.args,
      'trace': "\n".join(tb_list)
     }
    }   

class RedisPlugin(object):

    def __init__(self, pool, keyword='redis'):

        self.pool = pool
        self.keyword = keyword

    def setup(self, app):
        pass

    def apply(self, callback, context):
        import inspect
        import redis as rds

        # Override global configuration with route-specific values.
        conf = context['config'].get('redis') or {}

        keyword = conf.get('keyword', self.keyword)

        # Test if the original callback accepts a 'library' keyword.
        # Ignore it if it does not need a database handle.
        args = inspect.getargspec(context['callback'])[0]
        if keyword not in args:
            return callback

        def wrapper(*args, **kwargs):

            kwargs[keyword] = rds.Redis(connection_pool=self.pool)

            rv = callback(*args, **kwargs)

            return rv

        # Replace the route callback with the wrapped one.
        return wrapper

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
    


@get('/')
def get_root(redis):

    return []


def request_delay(nxt,delay,delay_factor):
    """
    Calculate how long this client should be delayed before next
    request.
    :rtype : object
    """

    import time

    now = time.time()

    try:
        delay = float(delay)
    except:
        delay = 1.0

    nxt = float(nxt) if nxt else now-1

    since = None
    if now <= nxt:
        # next is in the future, so the
        # request is rate limited

        ok = False

    else:
        # next is in the past, so the request can proceed
        since = now - nxt

        if since > 2*delay:

            delay = int(delay / delay_factor )

            if delay < 1:
                delay = 1

        else:

            delay = int(delay * delay_factor)

        if nxt < now:
            nxt = now

        nxt = nxt + delay

        ok = True

    return ok, since, nxt,  delay, nxt-now, (nxt+4*delay)-now

@get('/next')
@CaptureException
def get_next(redis, assignment_class=None, space=''):
    from time import time
    from ambry.identity import DatasetNumber, TopNumber

    delay_factor = 2

    ip = str(request.remote_addr)
    now = time()

    next_key = "next:"+ip
    delay_key = "delay:"+ip

    if space and space in NUMBER_SPACES:
        spacestr = space + ':'
    else:
        spacestr = ''

    #
    # The assignment class determine how long the resulting number will be
    # which namespace the number is drawn from, and whether the user is rate limited
    # The assignment_class: key is assigned and set externally
    #
    access_key = request.query.access_key

    if access_key:
        assignment_class_key = "assignment_class:"+access_key
        assignment_class = redis.get(assignment_class_key )

    if not assignment_class:
        raise exc.NotAuthorized('Use an access key to gain access to this service')

    #
    # These are the keys that store values, so they need to be augmented with the numebr space.
    # For backwards compatiility, the 'd' space is empty, but the other spaces have strings.
    #
    number_key = "dataset_number:"+spacestr+assignment_class # The number space depends on the assignment class.
    authallocated_key = "allocated:"+spacestr+assignment_class
    ipallocated_key = "allocated:" + spacestr+ip # Keep track of allocatiosn by IP

    nxt = redis.get(next_key)
    delay = redis.get(delay_key)

    # Adjust rate limiting based on assignment class
    if assignment_class == 'authoritative':
        since, nxt, delay, wait, safe  = (0,now-1,0,0,0)

    elif assignment_class == 'registered':
        delay_factor = 1.1

    ok, since, nxt, delay, wait, safe = request_delay(nxt,delay,delay_factor)

    with redis.pipeline() as pipe:
        redis.set(next_key, nxt)
        redis.set(delay_key, delay)

    global_logger.info("ip={} ok={} since={} nxt={} delay={} wait={} safe={}"
                    .format(ip, ok, since, nxt, delay, wait, safe))

    if ok:
        number = redis.incr(number_key)

        if not space:
            dn = DatasetNumber(number, None, assignment_class)
        else:
            dn = TopNumber(space, number, None, assignment_class)

        redis.sadd(ipallocated_key, dn)
        redis.sadd(authallocated_key, dn)

    else:
        number = None
        raise exc.TooManyRequests(" Access will resume in {} seconds".format(wait))

    return dict(ok=ok,
                number=str(dn),
                assignment_class=assignment_class,
                wait=wait,
                safe_wait=safe,
                nxt=nxt,
                delay=delay)


@get('/next/<space>')
@CaptureException
def get_next_space(redis, assignment_class=None, space=''):

    if not space in NUMBER_SPACES:
        raise exc.NotFound('Invalid number space: {}'.format(space))

    return get_next(redis,assignment_class=assignment_class, space=space)

@get('/find/<name>')
def get_echo_term(name, redis):
    '''Return an existing number for a bundle name, or return a new one. '''

    nk = 'name:'+name

    # This code has a race condition. It can be fixed with pipe lines, but
    # that requires re-working get_next

    v = redis.get(nk)

    if v:
        d = dict(
            ok=True,
            number=v,
            assignment_class=None,
            wait=None,
            safe_wait=None,
            nxt=None,
            delay=0
        )

        return d

    else:
        d = get_next(redis)

        # get_next captures exceptions, so we'll have to deal with it as a return value.
        if 'exception' not in d:
            redis.set(nk, d['number'])

        return d


@get('/echo/<term>')
def get_echo_term(term, redis):
    '''Test function to see if the server is working '''

    return [term]

def _run(host, port, redis, unregistered_key,  reloader=False, **kwargs):
    import redis as rds
    pool = rds.ConnectionPool(host=redis['host'],port=redis['port'], db=0)

    rds = rds.Redis(connection_pool=pool)

    # This is the key that can be distributed publically. It is only to
    # keep bots and spiders from sucking up a bunch of numbers.
    rds.set("assignment_class:"+unregistered_key,'unregistered')

    install(RedisPlugin(pool))

    print host, port

    return run( host=host, port=port, reloader=reloader, server='paste')
    
if __name__ == '__main__':
    import argparse
    from ambry.run import  get_runconfig
    from ..util import print_yaml
    import uuid
    rc = get_runconfig()

    d = rc.servers('numbers',{'host' : 'localhost', 'port': 8080, 'unregistered_key': str(uuid.uuid4()) })

    try:
        d = d.to_dict()
    except:
        pass

    d['redis'] = d.get('redis',{'host':'localhost','port': 6379})

    parser = argparse.ArgumentParser(prog='python -mambry.server.numbers',
                                     description='Run an Ambry numbers server')

    parser.add_argument('-H','--server-host', default=None, help="Server host. Defaults to configured value: {}".format(d['host']))
    parser.add_argument('-p','--server-port', default=None, help="Server port. Defaults to configured value: {}".format(d['port']))

    parser.add_argument('-R','--redis-host', default=None, help="Redis host. Defaults to configured value: {}".format(d['redis']['host']))
    parser.add_argument('-r','--redis-port', default=None, help="Redis port. Defaults to configured value: {}".format(d['redis']['port']))
    parser.add_argument('-u', '--unregistered-key', default=None,help="access_key value for unregistered access")

    args = parser.parse_args()

    if args.server_port:
        d['port'] = args.server_port

    if args.server_host:
        d['host'] = args.server_host

    if args.redis_port:
        d['redis']['port'] = args.redis_port

    if args.redis_host:
        d['redis']['host'] = args.redis_host

    if args.unregistered_key:
        d['unregistered_key'] = args.unregistered_key

    print_yaml(d)

    _run(**d)
    

