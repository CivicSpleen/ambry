"""Documentation, file and login server for Ambry warehouses.

Copyright 2014, Civic Knowledge. All Rights Reserved

"""

import os
import functools
from flask import Flask, g
from flask.ext.session import Session


# Default configuration
app_config = {
    'host': os.getenv('AMBRYDOC_HOST', 'localhost'),
    'port': os.getenv('AMBRYDOC_PORT', 8081),
    'use_proxy': bool(os.getenv('AMBRYDOC_USE_PROXY', False)),
    'debug': bool(os.getenv('AMBRYDOC_HOST', False)),
    'SESSION_TYPE': 'filesystem',
    'SESSION_FILE_DIR': '/tmp/ambrydoc/sessions/',
    'website_title': os.getenv('AMBRY_UI_TITLE', 'Civic Knowledge Data Search'),
}

class AmbryAppContext(object):
    """Ambry specific objects for the application context"""

    def __init__(self):
        from ambry.library import Library
        from render import Renderer
        from ambry.run import get_runconfig

        rc = get_runconfig()
        self.library = Library(rc, read_only=True, echo = False)
        self.renderer = Renderer(self.library)

        #import logging
        #path = self.library.filesystem.logs()
        #logging.basicConfig(filename=path, level=logging.DEBUG)

    def render(self, template, *args, **kwargs):
        return self.renderer.render(template, *args, **kwargs)

def aac(): # Ambry Application Context
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'aac'):
        g.acc = AmbryAppContext()

    return g.acc


app = Flask(__name__)

app.config.update(app_config)
Session(app)

# Flask Magic. The views have to be imported for Flask to use them.
import public_search.ui.views
