"""
REST API for Warehouse servers, to download extracts and upload manifests.


Run with gunicorn:

    gunicorn ambry.warehouse.server:app -b 0.0.0.0:81s


"""

from flask import Flask, current_app,url_for
from flask import g, send_from_directory, request, jsonify, abort
from flask.ext.compress import Compress
from ambry.util import memoize
import os

app = Flask(__name__)
Compress(app)



import logging

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
app.logger.addHandler(stream_handler)

@memoize
def get_warehouse_data():

    l = library()

    d = {}

    for sf in l.stores:
        s = l.store(sf.ref)

        w = l.warehouse(s.ref)

        d[sf.ref] = w.dict

        del d[sf.ref]['dsn']

    return d

@app.route('/')
def root():

    return jsonify(get_warehouse_data())


@app.route('/warehouses/<wid>/extracts/<tid>.<ct>')
def get_extract(wid, tid, ct):
    """Return an extract for a table """
    from extractors import new_extractor
    from os.path import basename, dirname

    w = warehouse(wid)

    t = w.orm_table(tid)

    if not t:
        abort(404)

    e = new_extractor(ct, w, w.cache.subcache('extracts'))

    ref = t.name if t.type in ('view','mview') else t.vid

    ee = e.extract(ref,'{}.{}'.format(tid,ct))

    return send_from_directory(directory=dirname(ee.abs_path),
                               filename=basename(ee.abs_path),
                               as_attachment = True,
                               attachment_filename="{}_{}.{}".format(t.vid,t.name,ct))

@app.route('/warehouses/<wid>/extractors/<tid>')
def get_extractors(wid, tid):
    from extractors import get_extractors

    w = warehouse(wid)

    t = w.orm_table(tid)

    return jsonify(results=get_extractors(t))


def library():
    from ambry.library import new_library
    from ambry.run import get_runconfig

    rc = get_runconfig()
    library = new_library(rc.library('default'))

    return library

def warehouse(uid):
    from werkzeug.exceptions import NotFound

    w =  library().warehouse(uid)

    return w


@app.before_first_request
def init_warehouses():

    l = library()

    for sf in l.stores:
        s = l.store(sf.ref)

        w = l.warehouse(s.ref)
        w.url = url_for('root', _external=True)
        print 'Registering', s.ref, w.dsn, w.url
        w.close()
        l.sync_warehouse(w)


@app.teardown_appcontext
def shutdown_application(exception=None):
    pass


def exit_handler():
    l = library()

    for sf in l.stores:
        s = l.store(sf.ref)

        w = l.warehouse(s.ref)
        w.url = None
        print "Unregistering ", s.ref, w.dsn, w.url
        w.close()
        l.sync_warehouse(w)

import atexit
atexit.register(exit_handler)

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

    parser.add_argument('-d', '--debug', action='store_true', help="Set debugging mode")

    args = parser.parse_args()

    app_config.update( {k:v for k,v in vars(args).items() if v})

    app.run(host=app_config['host'], port=int(app_config['port']), debug=app_config['debug'])