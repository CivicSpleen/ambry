"""Support for creating web pages and text representations of schemas."""

from csv import reader
import os

from . import app

from pygments import highlight
from pygments.lexers.sql import SqlLexer
from pygments.formatters import HtmlFormatter

from six import StringIO, string_types
import sqlparse

import jinja2.tests
from jinja2 import Environment, PackageLoader

from flask.json import JSONEncoder as FlaskJSONEncoder
from flask.json import dumps
from flask import Response, make_response, url_for


from ambry.identity import ObjectNumber, NotObjectNumberError, Identity
from ambry.bundle import Bundle
from ambry.orm.partition import Partition
from ambry.orm import Table
from ambry.orm.exc import NotFoundError
from ambry.util import get_logger
from ambry.util import pretty_time

import templates as tdir

from ambry.util import get_logger

logger = get_logger(__name__)

##
# These are in later versions of jinja, but we need them in earlier ones.
if 'equalto' not in jinja2.tests.TESTS:
    def test_equalto(value, other):
        return value == other

    jinja2.tests.TESTS['equalto'] = test_equalto

if 'isin' not in jinja2.tests.TESTS:
    def test_isin(value, other):
        return value in other

    jinja2.tests.TESTS['isin'] = test_isin





def resolve(ref):
    if isinstance(ref, string_types):
        return ref
    elif isinstance(ref, (Identity, Table)):
        return ref.vid
    elif isinstance(ref, Bundle):
        return ref.identity.vid
    elif isinstance(ref, Partition):
        return ref.identity.vid
    elif isinstance(ref, dict):
        if 'identity' in ref:
            return ref['identity'].get('vid', None)
        else:
            return ref.get('vid', None)
    else:
        raise Exception("Failed to resolve reference: '{}' ".format(ref))
        return None


# Path functions, for generating URL paths.


def bundle_path(b):
    return '/bundles/{}.html'.format(resolve(b))


def schema_path(b, format):
    return '/bundles/{}/schema.{}'.format(resolve(b), format)


def table_path(b, t):
    return '/bundles/{}/tables/{}.html'.format(resolve(b), resolve(t))


def proto_vid_path(pvid):

    try:
        b, t, c = deref_tc_ref(pvid)
        return table_path(str(b), str(t))

    except NotFoundError:
        return '#'


def deref_tc_ref(ref):
    """Given a column or table, vid or id, return the object."""
    on = ObjectNumber.parse(ref)

    b = str(on.as_dataset)

    try:
        c = on
        t = on.as_table
    except AttributeError:
        t = on
        c = None

    if not on.revision:
        # The table does not have a revision, so we need to get one, just get the
        # latest one

        r = renderer()
        dc = r.doc_cache

        tm = dc.table_version_map()

        if str(t) not in tm:
            # This happens when the the referenced table is in a bundle that is not installed,
            # often because it is private or restricted
            raise NotFoundError('Table {} not in table_version_map'.format(str(t)))

        t_vid = next(reversed(sorted(tm.get(str(t)))))

        t = ObjectNumber.parse(t_vid)
        b = t.as_dataset

        if c:
            c = c.rev(t.revision)

    return b, t, c


def tc_obj(ref):
    """Return an object for a table or column."""

    dc = renderer().doc_cache

    try:
        b, t, c = deref_tc_ref(ref)
    except NotFoundError:
        return None

    try:
        table = dc.table(str(t))
    except NotFoundError:

        # This can happen when the table reference has a version id in it, and that version is not available.
        # So, try it again without the version
        table = dc.table(str(ObjectNumber.parse(str(t)).rev(None)))

    if c:
        try:
            return table['columns'][str(c.rev(0))]
        except KeyError:
            return None
        except TypeError:
            return None
    else:
        return table


def partition_path(b, p=None):

    if p is None:
        p = b

    try:
        on = ObjectNumber.parse(p)
        b = str(on.as_dataset)
    except NotObjectNumberError as e:
        return None
    except AttributeError:
        b = str(on)
        raise


    return '/bundles/{}/partitions/{}.html'.format(resolve(b), resolve(p))


def manifest_path(m):
    return '/manifests/{}.html'.format(m)


def store_path(s):
    return '/stores/{}.html'.format(s)


def store_table_path(s, t):
    return '/stores/{}/tables/{}.html'.format(s, t)


def extract_url(s, t, format):
    return url_for('get_extract', wid=s, tid=t, ct=format)


def db_download_url(base, s):
    return os.path.join(base, 'download', s + '.db')


def extractor_list(t):
    return ['csv', 'json'] + (['kml', 'geojson'] if t.get('is_geo', False) else [])


class extract_entry(object):
    def __init__(self, extracted, completed, rel_path, abs_path, data=None):
        self.extracted = extracted
        # For deleting files where exception thrown during generation
        self.completed = completed
        self.rel_path = rel_path
        self.abs_path = abs_path
        self.data = data

    def __str__(self):
        return 'extracted={} completed={} rel={} abs={} data={}'.format(
            self.extracted,
            self.completed,
            self.rel_path,
            self.abs_path,
            self.data)


class JSONEncoder(FlaskJSONEncoder):
    def default(self, o):
        return str(type(o))


def format_sql(sql):
    return highlight(
        sqlparse.format(sql, reindent=True, keyword_case='upper'),
        SqlLexer(),
        HtmlFormatter())

def iter_as_dict(itr):
    """Given an iterable, return a comprehension with the dict version of each element"""
    from operator import attrgetter

    ag = attrgetter('dict')

    return [ ag(e) for e in itr if hasattr(e, 'dict') ]


@property
def pygmentize_css(self):
    return HtmlFormatter(style='manni').get_style_defs('.highlight')


class Renderer(object):

    def __init__(self, library, env = None, content_type='html', session=None,
                blueprints=None):

        self.library = library

        self.css_files = ['css/style.css', 'css/pygments.css']

        self.env = env if env else Environment(loader=PackageLoader('public_search.ui', 'templates'))

        # Set to true to get Render to return json instead
        self.content_type = content_type

        self.blueprints = blueprints

        self.session = session if session else {}

        # Monkey patch to get the equalto test

    def cts(self, ct, session=None):
        """Return a clone with the content type set, and maybe the session"""

        return Renderer(self.library, env=self.env, content_type = ct, session = session)

    def cc(self):
        """Return common context values. These are primarily helper functions
        that can be used from the context. """
        from functools import wraps

        # Add a prefix to the URLs when the HTML is generated for the local
        # filesystem.
        def prefix_root(r, f):
            @wraps(f)
            def wrapper(*args, **kwds):
                return os.path.join(r, f(*args, **kwds))

            return wrapper

        return {
            'url_for': url_for,
            'from_root': lambda x: x,
            'schema_path': schema_path,
            'bundle_path': bundle_path,
            'table_path': table_path,
            'partition_path': partition_path,
            'getattr': getattr,
            'title': app.config.get('website_title')
        }



    def render(self, template, *args, **kwargs):
        from flask import render_template

        kwargs.update(self.cc())
        kwargs['l'] = self.library

        if self.content_type == 'json':
            return Response(dumps(kwargs, cls=JSONEncoder, indent=4),mimetype='application/json')

        else:
            return render_template(template, *args, **kwargs)

    def json(self, **kwargs):
        return Response(dumps(kwargs, cls=JSONEncoder), mimetype='application/json')


    @property
    def css_dir(self):
        return os.path.join(os.path.abspath(os.path.dirname(tdir.__file__)), 'css')

    def css_path(self, name):
        return os.path.join(os.path.abspath(os.path.dirname(tdir.__file__)), 'css', name)

    @property
    def js_dir(self):
        return os.path.join(os.path.abspath(os.path.dirname(tdir.__file__)), 'js')


    def bundle_search(self, terms):

        results = list(self.library.search.search(terms))

        return self.render('search/results.html', result_count = len(results), results=results[:10])