"""
REST API for Warehouse servers, to download extracts and upload manifests.


Run with gunicorn:

    gunicorn ambry.warehouse.server:app -b 104.236.53.117:80 \
        -e AMBRY_WAREHOUSE=postgres://root:Jx8bf3HDkN5Fuz@aegea.do.cnshost.net/health_demo

"""

from flask import Flask, current_app
from flask import g, send_from_directory, request, jsonify, abort
from flask.ext.compress import Compress
from ambry.util import memoize
import os

app = Flask(__name__)
Compress(app)

import logging

print app.config['COMPRESS_MIMETYPES'].append('text/csv')


stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
app.logger.addHandler(stream_handler)

_dsn = os.getenv("AMBRY_WAREHOUSE", None)  # Global DSN to database.

@app.route('/')
def get_root():

    w = warehouse()

    d = w.dict

    del d['dsn']

    return jsonify(d)

@app.route('/extracts/<tid>.<ct>')
def get_extract(tid, ct):
    """Return an extract for a table """
    from extractors import new_extractor
    from os.path import basename, dirname

    w = warehouse()

    t = w.orm_table(tid)

    if not t:
        abort(404)


    e = new_extractor(ct, w, cache())

    ref = t.name if t.type in ('view','mview') else t.vid

    ee = e.extract(ref,'{}.{}'.format(tid,ct))

    return send_from_directory(directory=dirname(ee.abs_path),
                               filename=basename(ee.abs_path),
                               as_attachment = True,
                               attachment_filename="{}_{}.{}".format(t.vid,t.name,ct))

@app.route('/extractors/<tid>')
def get_extractors(tid):
    from extractors import get_extractors

    w = warehouse()

    t = w.orm_table(tid)

    return jsonify(results=get_extractors(t))


def warehouse():

    from ambry.warehouse import new_warehouse, database_config
    from ambry.library import new_library
    from ambry.run import get_runconfig
    from ambry.util import get_logger

    rc = get_runconfig()

    library = new_library(rc.library('default'))

    base_dir = os.path.join(rc.filesystem('warehouse')['dir'], 'reset_server')

    # db_url is a module variable.
    config = database_config(_dsn, base_dir=base_dir)

    return new_warehouse(config, library, logger=get_logger(__name__))

@memoize
def cache():
    return warehouse().cache.subcache('extracts')

def run(config):

    global _dsn # Global so warehouse() can get it it.

    _dsn = config['dsn']

    print "Serving ", warehouse().database.dsn
    print "Cache:  ", str(cache())

    app.run(host=app_config['host'], port=int(app_config['port']), debug=app_config['debug'])

if __name__ == "__main__":
    import argparse

    app_config = dict(
        host='localhost',
        port='7978',
        debug=False,
        dsn = None,
    )

    parser = argparse.ArgumentParser(prog='python -mambry.server.documentation',
                                     description='Run an Ambry documentation server')

    parser.add_argument('-H', '--host', help="Server host.")
    parser.add_argument('-p', '--port', help="Server port")
    parser.add_argument('-d', '--dsn',  help="URL to the database")
    parser.add_argument('-D', '--debug', action='store_true', help="Set debugging mode")

    args = parser.parse_args()

    app_config.update( {k:v for k,v in vars(args).items() if v})

    run(app_config)