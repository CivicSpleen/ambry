
import os
from . import app, aac


from flask import g, current_app, send_from_directory, send_file, request, abort, url_for
from flask.json import jsonify

@app.teardown_appcontext
def close_connection(exception):
    aac().renderer.library.close()


@app.errorhandler(500)
def page_not_found(e):

    aac().render('500.html', e=e)

@app.route('/')
@app.route('/index')
def index():
    r = aac().renderer

    cxt = dict(
        bundles=[b for b in r.library.bundles],
        **r.cc()
    )

    return r.render('index.html', **cxt)


@app.route('/bundles')
def bundle_index():

    r = aac().renderer

    cxt = dict(
        bundles=[b for b in r.library.bundles],
        **r.cc()
    )

    return r.render('bundles.html', **cxt)


@app.route('/json')
def bundle_index_json():
    import os

    r = aac().renderer

    def augment(b):
        o = b.dataset.dict
        o['bundle_url'] = url_for('bundle_json', vid=o['vid'])
        o['title'] = b.metadata.about.title
        o['summary'] = b.metadata.about.summary
        o['created'] = b.buildstate.new_datetime.isoformat() if b.buildstate.new_datetime else None
        o['updated'] = b.buildstate.last_datetime.isoformat() if b.buildstate.last_datetime else None
        return o

    return r.json(
        bundles=[augment(b) for b in r.library.bundles]

    )

@app.route('/bundles/<vid>.<ct>')
def bundle_about(vid, ct):

    r = aac().renderer.cts(ct)

    cxt = dict(
        vid=vid,
        b=r.library.bundle(vid),
        sources_header=['name', 'source_table_name', 'ref'],
        **r.cc()
    )

    return r.render('bundle/about.html', **cxt)

@app.route('/json/bundle/<vid>')
def bundle_json(vid):

    r = aac().renderer

    b = r.library.bundle(vid)

    def aug_dataset(b):
        o = b.dataset.dict
        del o['dataset']
        o['title'] = b.metadata.about.title
        o['summary'] = b.metadata.about.summary
        o['created'] = b.buildstate.new_datetime.isoformat() if b.buildstate.new_datetime else None
        o['updated'] = b.buildstate.last_datetime.isoformat() if b.buildstate.last_datetime else None
        return o

    def aug_partition(o):

        o['csv_url'] = url_for('stream_file', pvid=o['vid'], ct='csv')
        o['details_url'] = url_for('partition_json', vid=o['vid'])
        o['description'] = p.table.description
        o['sub_description'] = p.sub_description
        return o

    def partitions():
        from ambry.orm import Partition
        from sqlalchemy.orm import noload, joinedload
        for p in (b.dataset.query(Partition).filter(Partition.d_vid == b.identity.vid)
                          .options(noload('*'), joinedload('table')).all()):
            yield p

    return r.json(
        dataset=aug_dataset(b),
        partitions = [ aug_partition(p.dict) for p in partitions() ]

    )


@app.route('/json/partition/<vid>')
def partition_json(vid):

    r = aac().renderer

    p = r.library.partition(vid)

    d = p.dict
    d['csv_url'] = url_for('stream_file', pvid=p.vid, ct='csv')

    d['description'] = p.table.description
    d['sub_description'] = p.sub_description

    def aug_col(c):
        d = c.dict
        d['stats'] = [ s.dict for s in c.stats ]
        return d

    d['table'] = p.table.dict
    d['table']['columns'] = [ aug_col(c) for c in p.table.columns ]
    return r.json(
        partition = d

    )

@app.route('/bundles/<vid>/meta.<ct>')
def bundle_meta(vid, ct):

    r = aac().renderer.cts(ct)

    def flatten_dict(d):
        def expand(key, value):
            if isinstance(value, dict):
                return [(key + '.' + k, v) for k, v in flatten_dict(value).items()]
            else:
                return [(key, value)]

        items = [item for k, v in d.items() for item in expand(k, v)]

        return dict(items)

    b = r.library.bundle(vid)

    metadata = { k : sorted(flatten_dict(v).items()) for k, v in b.metadata.dict.items() }

    cxt = dict(
        vid=vid,
        b=b,
        metadata=sorted(metadata.items()),
        **r.cc()
    )

    return r.render('bundle/meta.html', **cxt)

@app.route('/bundles/<vid>/files.<ct>')
def bundle_files(vid, ct):

    r = aac().renderer.cts(ct)

    cxt = dict(
        vid=vid,
        b=r.library.bundle(vid),
        **r.cc()
    )

    return r.render('bundle/partitions.html', **cxt)

@app.route('/bundles/<vid>/documentation.<ct>')
def bundle_documentation(vid, ct):

    r = aac().renderer.cts(ct)

    cxt = dict(
        vid=vid,
        b=r.library.bundle(vid),
        **r.cc()

    )

    return r.render('bundle/documentation.html', **cxt)

@app.route('/bundles/<vid>/sources.<ct>')
def bundle_sources(vid, ct):
    from ambry.util import drop_empty

    r = aac().renderer.cts(ct)

    b = r.library.bundle(vid)

    sources = []
    for i, row in enumerate(b.sources):
        if not sources:
            sources.append(list(row.dict.keys()))

        sources.append(list(row.dict.values()))

    sources = drop_empty(sources)

    cxt = dict(
        vid=vid,
        b=b,
        sources = sources[1:] if sources else [],
        sources_header = ['name','source_table_name','ref'],
        **r.cc()

    )

    return r.render('bundle/sources.html', **cxt)

@app.route('/bundles/<vid>/build.<ct>')
def bundle_build(vid, ct):

    r = aac().renderer.cts(ct)

    cxt = dict(
        vid=vid,
        b=r.library.bundle(vid),
        **r.cc()

    )

    return r.render('bundle/build.html', **cxt)

@app.route('/bundles/<vid>/file/<name>')
def bundle_file(vid,name):
    """Return a file from the bundle"""
    from cStringIO import StringIO
    from ambry.orm.file import File

    m = { v:k for k,v in File.path_map.items()}

    b = aac().renderer.library.bundle(vid)

    bs = b.build_source_files.file(m[name])

    sio = StringIO()

    bs.record_to_fh(sio)

    return send_file(StringIO(sio.getvalue()))

    sio.close()

@app.route('/search/bundle')
def bundle_search():
    """Search for a datasets and partitions, using a structured JSON term."""

    return aac().renderer.bundle_search(terms=request.args['terms'])

@app.route('/bundles/<vid>/tables/<tvid>.<ct>')
def get_table(vid, tvid, ct):

    r = aac().renderer.cts(ct)
    b = r.library.bundle(vid)

    cxt = dict(
        vid=vid,
        tvid=tvid,
        b=b,
        t=b.table(tvid),
        **r.cc()

    )

    return r.render('bundle/table.html', **cxt)

@app.route('/bundles/<bvid>/partitions/<pvid>.<ct>')
def get_bundle_partitions(bvid, pvid, ct):
    r = aac().renderer.cts(ct)
    b = r.library.bundle(bvid)
    p = b.partition(pvid)

    # FIXME This should be cached somewhere.

    source_names = [ s.name for s in p.table.sources ]

    docs = []

    for k, v in b.metadata.external_documentation.group_by_source().items():
        if k in source_names:
            for d in v:
                docs += v

    cxt = dict(
        vid=bvid,
        b=b,
        p=p,
        t=p.table,
        docs=docs,
        **r.cc()
    )

    return r.render('bundle/partition.html', **cxt)

@app.route('/file/<pvid>.<ct>')
def stream_file(pvid,ct):
    from flask import abort

    if ct == 'csv':
        return stream_csv(pvid)
    elif ct == 'mpack':
        return stream_mpack(pvid)
    else:
        abort(404)

def stream_csv(pvid):
    from flask import Response
    import cStringIO as StringIO
    import unicodecsv as csv

    r = aac().renderer
    p = r.library.partition(pvid)

    if p.is_local:
        reader = p.reader
    else:
        reader = p.remote_datafile.reader

    def yield_csv_row(w, b, row):
        w.writerow(row)
        b.seek(0)
        data = b.read()
        b.seek(0)
        b.truncate()
        return data

    def stream_csv():
        b = StringIO.StringIO()
        writer = csv.writer(b)

        yield yield_csv_row(writer, b, reader.headers)

        for row in reader.rows:
            yield yield_csv_row(writer, b, row)


    return Response(stream_csv(), mimetype='text/csv')


def stream_mpack(pvid):
    from flask import Response
    import cStringIO as StringIO
    import unicodecsv as csv
    import msgpack

    r = aac().renderer
    p = r.library.partition(pvid)

    if p.is_local:
        reader = p.reader
    else:
        reader = p.remote_datafile.reader

    def stream_msgp():
        yield msgpack.packb(reader.headers)
        for row in reader.rows:
            yield msgpack.packb(row)

    return Response(stream_msgp(), mimetype='application/msgpack')

# Really should be  serving this from a static directory, but this
# is easier for now.
@app.route('/css/<name>')
def css_file(name):
    return send_from_directory(aac().renderer.css_dir, name)


@app.route('/js/<path:path>')
def js_file(path):
    import os.path

    return send_from_directory(*os.path.split(os.path.join(aac().renderer.js_dir,path)))

