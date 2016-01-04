"""Documentation, file and login server for Ambry warehouses.

Copyright 2014, Civic Knowledge. All Rights Reserved

"""

import os
import functools
from flask import Flask, g
from uuid import uuid4
from flask_wtf.csrf import CsrfProtect
from session import ItsdangerousSessionInterface
from flask_login import LoginManager



import logging

# Default configuration
app_config = {
    'host': os.getenv('AMBRY_UI_HOST', 'localhost'),
    'port': os.getenv('AMBRY_UI_PORT', 8081),
    'use_proxy': bool(os.getenv('AMBRY_UI_USE_PROXY', False)),
    'debug': bool(os.getenv('AMBRY_UI_DEBUG', False)),
    'website_title': os.getenv('AMBRY_UI_TITLE', 'Civic Knowledge Data Search'),
    'JWT_SECRET': os.getenv('AMBRY_JWT_SECRET', os.getenv('AMBRY_API_TOKEN',str(uuid4()))),
    'MAX_CONTENT_LENGTH': 1024*1024*512,
    "WTF_CSRF_SECRET_KEY": str(uuid4()),
    "SECRET_KEY": str(uuid4()),
    'SESSION_TYPE': 'filesystem',
    'WTF_CSRF_CHECK_DEFAULT': False # Turn off CSRF by default. Must enable on specific views.
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


    def bundle(self, ref):
        from flask import abort
        from ambry.orm.exc import NotFoundError

        try:
            return self.library.bundle(ref)
        except NotFoundError:
            abort(404)


    def json(self,*args, **kwargs):
        return self.renderer.json(*args, **kwargs)

    def close(self):
        self.library.close()

def get_aac(): # Ambry Application Context
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'aac'):
        g.aac = AmbryAppContext()

    return g.aac


app = Flask(__name__)

app.config.update(app_config)

csrf = CsrfProtect()
csrf.init_app(app)
#app.secret_key = app.config['SECRET_KEY']
app.session_interface = ItsdangerousSessionInterface()

login_manager = LoginManager()
login_manager.init_app(app)

@app.teardown_appcontext
def close_connection(exception):
    aac = getattr(g,'aac', None)
    if aac is not None:
        aac.close()

# Flask Magic. The views have to be imported for Flask to use them.
import ambry.ui.views
import ambry.ui.jsonviews
import ambry.ui.api
import ambry.ui.user