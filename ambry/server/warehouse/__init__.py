"""Documentation, file and login server for Ambry warehouses
"""

from flask import Flask, current_app
import logging, os
from ambry.util import get_logger
from ambry.run import get_runconfig
from ambry.text import Renderer
from ambry.library import new_library
from ambry.warehouse import new_warehouse, database_config
from ambry.cache import new_cache, parse_cache_string
from ambry.util import memoize

global_logger = get_logger(__name__)
global_logger.setLevel(logging.DEBUG)

app = Flask(__name__)

def configure_application(command_args = None):

    # Load the application configuration
    rc = get_runconfig()

    app_config = {'host': 'localhost',
                'port': 8080,
                'cache': '/tmp/ambry-doc-cache',
                'library': 'default'
    }

    app_config.update(rc.servers('documentation', {} ).to_dict())


    if command_args:
        app_config.update(command_args)

    with app.app_context():

        current_app.app_config = app_config
        current_app.run_config =  rc

    return app_config

@memoize # Remove the Memoize if there are concurrency problems with Sqlite.
def library():

    if current_app.app_config.get('warehouse', False):
        return warehouse().library
    else:
        l = new_library(current_app.run_config.library(current_app.app_config['library']), True)
        l.logger = global_logger
        return l

@memoize
def warehouse():
    from ambry.dbexceptions import ConfigurationError

    if current_app.app_config.get('warehouse',False):

        try: # The warehouse value is a name
            warehouse_config = current_app.run_config.warehouse(current_app.app_config['warehouse'])
        except ConfigurationError:
            from ambry.warehouse import database_config
            # The warehouse value is a database string
            warehouse_config = database_config(current_app.app_config['warehouse'])


        l = new_library(current_app.run_config.library(current_app.app_config['library']), True)

        return new_warehouse(warehouse_config,l, logger=global_logger)
    else:
        return None

@memoize
def cache():
    cache_config = parse_cache_string(current_app.app_config['cache'])
    return new_cache(cache_config, run_config=current_app.run_config)

@memoize
def renderer():
    return Renderer(cache(), library=library(), warehouse = warehouse(), root_path = '/')


import views
configure_application() # May get run again in __main__, with running in develop mode.
