"""
API Views, return javascript rendietions of objects and allowing modification of the database
"""

import os
from . import app, get_aac


from flask import Flask, g, current_app, send_from_directory, send_file, request, abort, url_for
from flask.json import jsonify
from flask_jwt import JWT, jwt_required, current_identity
from werkzeug.security import safe_str_cmp

from werkzeug.local import LocalProxy


aac = LocalProxy(get_aac)


class User(object):
    def __init__(self, id, username, password):
        self.id = id
        self.username = username
        self.password = password

    def __str__(self):
        return "User(id='%s')" % self.id


def authenticate(username, password):
    from ambry.orm.exc import NotFoundError

    if username == 'api':
        if safe_str_cmp(password.encode('utf-8'), app.config['API_TOKEN'].encode('utf-8')):
            return User(username,username, password)


    # Try one of the accounts from the accounts file.
    try:
        account = aac.library.account(username)

        if account.major_type == 'ambry' and account.test(password):
            return User(username, username, password)
    except (KeyError,NotFoundError):

        pass

    return None


def identity(payload):
    user_id = payload['identity']
    return user_id


jwt = JWT(app, authenticate, identity)


#
# Administration Interfaces
#

@app.route('/config/remotes', methods = ['GET'])
@jwt_required()
def config_remotes_put():
    """Return remotes configured on the Library"""
    r = aac.renderer

    return r.json(
        remotes=r.library.remotes
    )

@app.route('/config/remotes', methods = ['PUT'])
@jwt_required()
def config_remotes_get():
    """Replace all of the remotes in the library with new ones"""
    r = aac.renderer

    from ambry.library.config import LibraryConfigSyncProxy

    lsp = LibraryConfigSyncProxy(r.library)

    lsp.sync_remotes(request.get_json(), clear = True)

    return r.json(
        remotes=r.library.remotes
    )

@app.route('/config/accounts', methods = ['GET'])
@jwt_required()
def config_accounts_get():

    r = aac.renderer

    def proc_account(a):
        if 'encrypted_secret' in a:
            del a['encrypted_secret']
        if 'secret' in a:
            del a['secret']
        return  a

    return r.json(
        accounts={ k:proc_account(a) for k, a in r.library.accounts.items()}
    )


@app.route('/config/accounts', methods = ['PUT'])
@jwt_required()
def config_accounts_put():
    from ambry.orm.account import AccountDecryptionError, MissingPasswordError
    r = aac.renderer


    from ambry.library.config import LibraryConfigSyncProxy

    l = r.library

    lsp = LibraryConfigSyncProxy(r.library)

    try:

        lsp.sync_accounts(request.get_json())
    except MissingPasswordError:
        print 'Missing Password'
        abort(400)
    except AccountDecryptionError:
        print "Decryption Failed"
        abort(400)


    return config_accounts_get()

@app.route('/config/services', methods = ['PUT'])
@jwt_required()
def config_services_put():
    pass

@app.route('/config/services', methods = ['GET'])
@jwt_required()
def config_services_get():
    pass

@app.route('/bundles/<ref>', methods = ['DELETE'])
@jwt_required()
def bundle_delete(ref):
    """Returns the file records, excluding the content"""
    from ambry.orm.exc import NotFoundError

    try:
        aac.library.remove(aac.bundle(ref))
    except NotFoundError:
        abort(404)

    return aac.json(
        ok=True
    )

@app.route('/bundles/<vid>/build/files', methods = ['GET'])
@jwt_required()
def bundle_build_files(vid):
    """Returns the file records, excluding the content"""

    r = aac.renderer

    b = r.library.bundle(vid)

    def make_dict(f):
        d = f.record.dict
        del d['modified_datetime']
        return d

    return r.json(
        files=[ make_dict(f) for f in b.build_source_files ]
    )

@app.route('/bundles/<vid>/build/files/<name>', methods = ['GET'])
@jwt_required()
def bundle_build_file(vid, name):
    """Returns the file records, excluding the content"""

    r = aac.renderer

    b = r.library.bundle(vid)

    try:
        fs = b.build_source_files.file(name)
    except KeyError:
        abort(404)

    def make_dict(f):
        d = f.record.dict
        del d['modified_datetime']
        return d

    return r.json(file= make_dict(fs))


@app.route('/bundles/<vid>/build/files/<name>/content', methods = ['GET'])
@jwt_required()
def bundle_build_files_get(vid, name):
    from flask import Response

    r = aac.renderer
    b = r.library.bundle(vid)

    try:
        fs = b.build_source_files.file(name)
    except KeyError:
        abort(404)

    mt = 'text/csv' if 'csv' in fs.record.path else 'text/plain'

    return Response(fs.getcontent(), mimetype=mt)

@app.route('/bundles/<vid>/build/files/<name>/content', methods = ['PUT'])
@jwt_required()
def bundle_build_files_put(vid, name):
    r = aac.renderer
    b = r.library.bundle(vid)

    try:
        fs = b.build_source_files.file(name)
    except KeyError:
        abort(404)

    mt = 'text/csv' if 'csv' in fs.record.path else 'text/plain'

    fs.setcontent(request.content)

    return r.json(
        file=fs.record.dict
    )

@app.route('/bundles/<vid>/checkin', methods = ['POST'])
@jwt_required()
def bundle_build_checkin_post(vid):
    """Checkin a bundle to this library, as a sqlite file"""
    from ambry.util.flo import copy_file_or_flo
    import tempfile
    r = aac.renderer

    fh, path = tempfile.mkstemp()
    os.fdopen(fh).close()

    with open(path, 'wb') as f:
        copy_file_or_flo(request.stream, f)

    r.library.checkin_bundle(path)

    return r.json(
        result='ok'
    )

@app.route('/bundles/<vid>/checkout', methods = ['GET'])
@jwt_required()
def bundle_build_checkout_get(vid):
    """Checkout a bundle from this library, as a Sqlite file"""
    pass

@app.route('/bundles/sync/<ref>', methods = ['POST'])
@jwt_required()
def bundle_build_sync_post(ref):
    """Command the library to install a bundle. Optionally send information about the remote to use.

    If a payload is included, it is an dict of an Account record, with the encrypted secret encrypted with the
    api token.

    """
    pass