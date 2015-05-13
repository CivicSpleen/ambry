
import os
from . import app, renderer


from flask import g, current_app, send_from_directory, request, abort
from flask.json import jsonify

from flask import  session

@app.teardown_appcontext
def close_connection(exception):
    pass


@app.errorhandler(500)
def page_not_found(e):
    return renderer().error500(e)


# Really should be  serving this from a static directory, but this
# is easier for now.
@app.route('/css/<name>')
def css_file(name):
    return send_from_directory(renderer().css_dir, name)


@app.route('/js/<path:path>')
def js_file(path):
    import os.path

    return send_from_directory(*os.path.split(os.path.join(renderer().js_dir,path)))


@app.route('/')
@app.route('/index')
def index():
    import time

    session['time_time'] = time.time() # Just for testing

    return renderer(session=session).index()


@app.route('/index.<ct>')
def index_ct(ct):
    return renderer(content_type=ct,session=session).index()


@app.route('/databases.<ct>')
def databases_ct(ct):
    return renderer(content_type=ct).databases()



@app.route('/search/place')
def place_search():
    """Search for a place, using a single term."""

    return renderer().place_search(term=request.args.get('term'))


@app.route('/search/bundle')
def bundle_search():
    """Search for a datasets and partitions, using a structured JSON term."""

    return renderer(session=session).bundle_search(terms=request.args['terms'])


@app.route('/bundles/<vid>.<ct>')
def get_bundle(vid, ct):
    return renderer(content_type=ct).bundle(vid)


@app.route('/bundles/summary/<vid>.<ct>')
def get_bundle_summary(vid, ct):
    return renderer(content_type=ct).bundle_summary(vid)


@app.route('/bundles/<vid>/schema.<ct>')
def get_schema(vid, ct):

    if ct == 'csv':
        return renderer().schemacsv(vid)
    else:
        return renderer(content_type=ct).schema(vid)


@app.route('/bundles.<ct>')
def get_bundles(ct):
    return renderer(content_type=ct).bundles_index()


@app.route('/tables.<ct>')
def get_tables(ct):

    return renderer(content_type=ct).tables_index()


@app.route('/bundles/<bvid>/tables/<tvid>.<ct>')
def get_table(bvid, tvid, ct):

    return renderer(content_type=ct).table(bvid, tvid)


@app.route('/bundles/<bvid>/partitions/<pvid>.<ct>')
def get_bundle_partitions(bvid, pvid, ct):

    return renderer(content_type=ct).partition(pvid)


@app.route('/collections.<ct>')
def get_collections(ct):

    return renderer(content_type=ct).collections_index()


@app.route('/stores/<sid>.<ct>')
def get_store(sid, ct):

    return renderer(content_type=ct).store(sid)


@app.route('/stores/<sid>/tables/<tvid>.<ct>')
def get_store_table(sid, tvid, ct):

    return renderer(content_type=ct).store_table(sid, tvid)


@app.route('/sources.<ct>')
def get_sources(ct):

    return renderer(content_type=ct).sources()


@app.route('/warehouses/<wid>/extracts/<tid>.<ct>')
def get_extract(wid, tid, ct):
    """Return an extract for a table."""

    from os.path import basename, dirname
    from ambry.dbexceptions import NotFoundError
    from flask import Response


    if ct == 'csv':

        row_gen = warehouse(wid).stream_table(tid, content_type=ct)

        return Response(row_gen(), mimetype='text/csv')

    else:

        try:

            path, attach_filename = warehouse(wid).extract_table(tid, content_type=ct)

            return send_from_directory(directory=dirname(path),
                                       filename=basename(path),
                                       as_attachment=True,
                                       attachment_filename=attach_filename)
        except NotFoundError:
            abort(404)


@app.route('/warehouses/<wid>/sample/<tid>')
def get_sample(wid, tid, ct):
    """Return an extract for a table."""

    # from os.path import basename, dirname
    from ambry.dbexceptions import NotFoundError

    try:
        warehouse(wid).extract_table(tid, content_type='json')
        # path, attach_filename = warehouse(wid).extract_table(tid, content_type='json')

    except NotFoundError:
        abort(404)


@app.route('/warehouses/<wid>/extractors/<tid>')
def get_extractors(wid, tid):
    from ambry.warehouse.extractors import get_extractors

    return jsonify(results=get_extractors(warehouse(wid).orm_table(tid)))


@app.route('/warehouses/download/<wid>.db')
def get_download(wid):
    from os.path import basename, dirname
    w = warehouse(wid)
    path = w.database.path
    return send_from_directory(directory=dirname(path),
                               filename=basename(path),
                               as_attachment=True,
                               attachment_filename="{}.db".format(wid))


def warehouse(uid):
    return renderer().library.warehouse(uid)
