"""Server application for assigning dataset numbers. Requires a redis instance
for data storage.

Run with something like: python -m ambry.server.numbers  -p 80 -H 162.243.194.227



The access key is a secret key that the client will use to assign an assignment class.
The two classes are 'authority' and 'registered' Only central authority
operators ( like Clarinova ) should use the authoritative class. Other users can
use the 'registered' class. Without a key and class assignment, the callers us
the 'unregistered' class.

Set the key for the authority class with the redis-cli:

    set assignment_class:this-is-a-long-uid-key authoritative

For 'registered' users, use:

    set assignment_class:this-is-a-long-uid-key registered

There is only one uri to call:

    /next

It returns a JSON dict, with the 'number' key mapping to the number.

Running a redis server in docker
--------------------------------

Run the server, from https://hub.docker.com/_/redis/

    docker run --name ambry-redis -d redis redis-server --appendonly yes


Connect from a CLI:

    docker run -it --link ambry-redis:redis --rm redis sh -c 'exec redis-cli -h "$REDIS_PORT_6379_TCP_ADDR" -p "$REDIS_PORT_6379_TCP_PORT"'

Proxy
-----

You probably also want to run a web proxy, like Hipache:

    docker run --name hipache --link ambry-redis:redis -p 80:8080 -p 443:4430 hipache

Copyright (c) 2014 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt

"""

from six import string_types

from bottle import error, hook, get, request, response  # , redirect, put, post
from bottle import HTTPResponse, install  # , static_file, url
# from bottle import Bottle
from bottle import run  # , debug  # @UnresolvedImport

from decorator import decorator  # @UnresolvedImport
import logging

import ambry.util

global_logger = ambry.util.get_logger(__name__)
global_logger.setLevel(logging.DEBUG)

# Alternative number spaces, mostly for manifests and databases
# The main number space for datasets is 'd'
NUMBER_SPACES = ('m', 'x', 'b')


class NotFound(Exception):
    pass

class InternalError(Exception):
    pass

class NotAuthorized(Exception):
    pass

class TooManyRequests(Exception):
    pass

def capture_return_exception(e):

    import sys
    import traceback

    # (exc_type, exc_value, exc_traceback) = sys.exc_info()  # @UnusedVariable

    tb_list = traceback.format_list(traceback.extract_tb(sys.exc_info()[2]))

    return {'exception': {
        'class': e.__class__.__name__,
        'args': e.args,
        'trace': "\n".join(tb_list)
    }}


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
    """Decorator implementation for capturing exceptions."""

    try:
        r = f(*args, **kwargs)
    except HTTPResponse:
        raise  # redirect() uses exceptions
    except Exception as e:
        r = capture_return_exception(e)
        if hasattr(e, 'code'):
            response.status = e.code

    return r


def CaptureException(f, *args, **kwargs):
    """Decorator to capture exceptions and convert them to a dict that can be
    returned as JSON."""

    return decorator(_CaptureException, f)  # Preserves signature


class AllJSONPlugin(object):

    """A copy of the bottle JSONPlugin, but this one tries to convert all
    objects to json."""

    from json import dumps as json_dumps

    name = 'json'
    remote = 2

    def __init__(self, json_dumps=json_dumps):
        self.json_dumps = json_dumps

    def apply(self, callback, context):

        dumps = self.json_dumps
        if not dumps:
            return callback

        def wrapper(*a, **ka):
            rv = callback(*a, **ka)

            if isinstance(rv, HTTPResponse):
                return rv

            if isinstance(rv, string_types):
                return rv

            # Attempt to serialize, raises exception on failure
            try:
                json_response = dumps(rv)
            except Exception as e:
                r = capture_return_exception(e)
                json_response = dumps(r)

            # Set content type only if serialization succesful
            response.content_type = 'application/json'
            return json_response
        return wrapper

install(AllJSONPlugin())


@error(404)
@CaptureException
def error404(error):
    raise NotFound("For url: {}".format(repr(request.url)))


@error(500)
def error500(error):
    raise InternalError("For Url: {}".format(repr(request.url)))


@hook('after_request')
def enable_cors():
    response.headers['Access-Control-Allow-Origin'] = '*'


@get('/')
def get_root(redis):

    return []


def request_delay(nxt, delay, delay_factor):
    """Calculate how long this client should be delayed before next request.

    :rtype : object

    """

    import time

    now = time.time()

    try:
        delay = float(delay)
    except (ValueError, TypeError):
        delay = 1.0

    nxt = float(nxt) if nxt else now - 1

    since = None
    if now <= nxt:
        # next is in the future, so the
        # request is rate limited

        ok = False

    else:
        # next is in the past, so the request can proceed
        since = now - nxt

        if since > 2 * delay:

            delay = int(delay / delay_factor)

            if delay < 1:
                delay = 1

        else:

            delay = int(delay * delay_factor)

        if nxt < now:
            nxt = now

        nxt = nxt + delay

        ok = True

    return ok, since, nxt, delay, nxt - now, (nxt + 4 * delay) - now


@get('/next')
@CaptureException
def get_next(redis, assignment_class=None, space=''):
    from time import time
    from ambry.identity import DatasetNumber, TopNumber

    delay_factor = 2

    ip = str(request.remote_addr)
    now = time()

    next_key = "next:" + ip
    delay_key = "delay:" + ip

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
        assignment_class_key = "assignment_class:" + access_key
        assignment_class = redis.get(assignment_class_key)

    if not assignment_class:
        raise NotAuthorized('Use an access key to gain access to this service')

    #
    # These are the keys that store values, so they need to be augmented with the numebr space.
    # For backwards compatiility, the 'd' space is empty, but the other spaces have strings.
    #
    # The number space depends on the assignment class.
    number_key = "dataset_number:" + spacestr + assignment_class
    authallocated_key = "allocated:" + spacestr + assignment_class
    # Keep track of allocatiosn by IP
    ipallocated_key = "allocated:" + spacestr + ip

    nxt = redis.get(next_key)
    delay = redis.get(delay_key)

    # Adjust rate limiting based on assignment class
    if assignment_class == 'authoritative':
        since, nxt, delay, wait, safe = (0, now - 1, 0, 0, 0)

    elif assignment_class == 'registered':
        delay_factor = 1.1

    ok, since, nxt, delay, wait, safe = request_delay(nxt, delay, delay_factor)

    # with redis.pipeline() as pipe:
    with redis.pipeline():
        redis.set(next_key, nxt)
        redis.set(delay_key, delay)

    log_msg = 'ip={} ok={} since={} nxt={} delay={} wait={} safe={}'\
        .format(ip, ok, since, nxt, delay, wait, safe)
    global_logger.info(log_msg)

    if ok:
        number = redis.incr(number_key)

        if not space:
            dn = DatasetNumber(number, None, assignment_class)
        else:
            dn = TopNumber(space, number, None, assignment_class)

        redis.sadd(ipallocated_key, dn)
        redis.sadd(authallocated_key, dn)

    else:
        raise TooManyRequests(' Access will resume in {} seconds'.format(wait))

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

    if space not in NUMBER_SPACES:
        raise NotFound('Invalid number space: {}'.format(space))

    return get_next(redis, assignment_class=assignment_class, space=space)


@get('/find/<name>')
def get_echo_term(name, redis):
    """Return an existing number for a bundle name, or return a new one."""

    nk = 'name:' + name

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

        # get_next captures exceptions, so we'll have to deal with it as a
        # return value.
        if 'exception' not in d:
            redis.set(nk, d['number'])

        return d


@get('/echo/<term>')
def get_echo_term(term, redis):
    """Test function to see if the server is working."""
    # FIXME: Why twice? See previous function.

    return [term]


def _run(host, port, redis, unregistered_key, reloader=False, **kwargs):
    import redis as rds
    pool = rds.ConnectionPool(host=redis['host'], port=redis['port'], db=0)

    rds = rds.Redis(connection_pool=pool)

    # This is the key that can be distributed publically. It is only to
    # keep bots and spiders from sucking up a bunch of numbers.
    rds.set('assignment_class:' + unregistered_key, 'unregistered')

    install(RedisPlugin(pool))

    print('{} {}'.format(host, port))

    return run(host=host, port=port, reloader=reloader, server='paste')

if __name__ == '__main__':
    import argparse
    import os

    from ..util import print_yaml

    docker_host = os.getenv('REDIS_PORT_6379_TCP_ADDR')
    docker_port = os.getenv('REDIS_PORT_6379_TCP_PORT', 6379)

    d = {
        'host': '0.0.0.0',
        'port': 80,
        'redis': {
            'host': docker_host,
            'port': docker_port

        },
        'unregistered_key': 'fe78d179-8e61-4cc5-ba7b-263d8d3602b9'
    }

    parser = argparse.ArgumentParser(prog='python -mambry.server.numbers',
                                     description='Run an Ambry numbers server')

    parser.add_argument('-H','--server-host',default=None,help="Server host. ")

    parser.add_argument('-p','--server-port',default=80,help="Server port.")
    parser.add_argument('-R','--redis-host',default=docker_host,help="Redis host.")
    parser.add_argument('-r','--redis-port',default=docker_port,help="Redis port.")
    parser.add_argument('-u','--unregistered-key',default=None,help="access_key value for unregistered access")

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
