"""
REST API for Warehouse servers, to download extracts and upload manifests.


Run with gunicorn:

    gunicorn ambry.warehouse.server:app -b 0.0.0.0:81s

"""

from flask import Flask, current_app,url_for
from flask import send_from_directory, request, jsonify, abort

from flask import Blueprint

exracts_blueprint = Blueprint('extract_tables', __name__)

@exracts_blueprint.route('/')
def get_root():
    return __name__

@exracts_blueprint.route('/<wid>/extracts/<tid>.<ct>')
def get_extract(wid, tid, ct):
    """Return an extract for a table """

    from os.path import basename, dirname
    from ambry.dbexceptions import NotFoundError

    try:

        path, attach_filename = warehouse(wid).extract_table(tid, content_type = ct)

        return send_from_directory(directory=dirname(path),
                                   filename=basename(path),
                                   as_attachment = True,
                                   attachment_filename=attach_filename)
    except NotFoundError:
        abort(404)

@exracts_blueprint.route('/<wid>/sample/<tid>')
def get_sample(wid, tid, ct):
    """Return an extract for a table """

    from os.path import basename, dirname
    from ambry.dbexceptions import NotFoundError

    try:

        path, attach_filename = warehouse(wid).extract_table(tid, content_type =  'json')


    except NotFoundError:
        abort(404)

@exracts_blueprint.route('/<wid>/extractors/<tid>')
def get_extractors(wid, tid):
    from ambry.warehouse.extractors import get_extractors

    return jsonify(results=get_extractors(warehouse(wid).orm_table(tid)))



def library():
    from ambry.library import new_library
    from ambry.run import get_runconfig

    rc = get_runconfig()
    library = new_library(rc.library('default'))

    return library

def warehouse(uid):
    return library().warehouse(uid)


if __name__ == "__main__":
    import argparse
    from flask.ext.compress import Compress

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

    app = Flask(__name__)
    Compress(app)

    app.register_blueprint(exracts_blueprint)

    app.run(host=app_config['host'], port=int(app_config['port']), debug=app_config['debug'])


