"""
REST API for Warehouse servers, to download extracts and upload manifests.
"""

from flask import Flask, current_app
from flask import g, send_from_directory, request, jsonify
from ambry.util import memoize
import os

app = Flask(__name__)

db_url = os.getenv("AMBRY_WAREHOUSE", None)

app_config = dict(
    host = 'localhost',
    port = '7978',
    debug = True
)

@app.route('/')
def get_root(tid, ct):

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

@memoize
def warehouse():

    from ambry.warehouse import new_warehouse, database_config
    from ambry.library import new_library
    from ambry.run import get_runconfig
    from ambry.util import get_logger

    rc = get_runconfig()

    library = new_library(rc.library('default'))

    base_dir = os.path.join(rc.filesystem('warehouse')['dir'], 'reset_server')

    # db_url is a module variable.
    config = database_config(db_url, base_dir=base_dir)

    return new_warehouse(config, library, logger=get_logger(__name__))

@memoize
def cache():
    return warehouse().cache.subcache('extracts')

def run(url=None):

    if url:
        global db_url
        db_url = url

    print "Serving ", warehouse().database.dsn

    app.run(host=app_config['host'], port=int(app_config['port']), debug=app_config['debug'])

if __name__ == "__main__":

    run()