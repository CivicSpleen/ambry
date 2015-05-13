"""Documentation, file and login server for Ambry warehouses.

Copyright 2014, Civic Knowledge. All Rights Reserved

"""

import os
import functools
from flask import Flask
from flask.ext.session import Session
from ambry.util import memoize

root_config = '/etc/ambrydoc/config.yaml'
user_config = '~/.ambrydoc/config.yaml'

config_paths = [root_config, os.path.expanduser(user_config)]

# From https://wiki.python.org/moin/PythonDecoratorLibrary#Memoize




# Default configuration
app_config = {'host': os.getenv('AMBRYDOC_HOST', 'localhost'),
              'port': os.getenv('AMBRYDOC_PORT', 8081),
              'cache': os.getenv('AMBRYDOC_CACHE', '/data/cache/jdoc'),
              'use_proxy': bool(os.getenv('AMBRYDOC_USE_PROXY', False)),
              'debug': bool(os.getenv('AMBRYDOC_HOST', False)),
              'SESSION_TYPE': 'filesystem',
              'SESSION_FILE_DIR' : '/tmp/ambrydoc/sessions/'

              }




def expiring_memoize(obj):
    """Like memoize, but forgets after 10 seconds."""
    from collections import defaultdict
    cache = obj.cache = {}
    last_access = obj.last_access = defaultdict(int)

    @functools.wraps(obj)
    def memoizer(*args, **kwargs):
        import time

        key = str(args) + str(kwargs)

        if last_access[key] and last_access[key] + 10 < time.time():
            if key in cache:
                del cache[key]

        last_access[key] = time.time()

        if key not in cache:
            cache[key] = obj(*args, **kwargs)
        return cache[key]

    return memoizer


@memoize
def fscache():
    from ckcache import parse_cache_string, new_cache

    cache_config = parse_cache_string(app_config['cache'])
    return new_cache(cache_config)


def renderer(content_type='html', session = None):

    session = session if session else {}

    from render import Renderer
    return Renderer(content_type=content_type, session=session)


def setup_logging():
    import logging

    path = fscache().path('ambrydoc.log', missing_ok=True)

    print "Logging to: ", path

    logging.basicConfig(filename=path, level=logging.DEBUG)



app = Flask(__name__)

if False:  # How to use a proxy
    from werkzeug.contrib.fixers import ProxyFix

    app.wsgi_app = ProxyFix(app.wsgi_app)


app.config.update(app_config)
Session(app)


import ambry.ui.views



