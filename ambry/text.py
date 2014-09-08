"""
Support for creating web pages and text representations of schemas.
"""

import os

class Renderer(object):

    def __init__(self, root_path, library = None, warehouse = None):
        import ambry.support.templates as tdir
        from jinja2 import Environment, PackageLoader


        self.warehouse = warehouse
        self.library = library
        self.root_path = root_path.rstrip('/')
        self.css_path = os.path.join(os.path.dirname(tdir.__file__), 'css','style.css')
        self.env = Environment(loader=PackageLoader('ambry.support', 'templates'))

    @property
    def css(self):
        with open(self.css_path) as f:
            return f.read()

class ManifestDoc(Renderer):

    def render(self, m):

        template = self.env.get_template('manifest/layout.html')

        m.add_bundles(self.library)

        return template.render(root_path=self.root_path, m=m)



class BundleDoc(Renderer):

    def render(self, w, b):

        import markdown
        m = b.metadata

        template = self.env.get_template('bundle.html')

        if not m.about.title:
            m.about.title = b.identity.vname

        return template.render(root_path=self.root_path, b=b, m=m,w=self.warehouse,
                               documentation = {
                                  'main': markdown.markdown(m.documentation.main) if m.documentation.main else None,
                                  'readme': markdown.markdown(m.documentation.readme) if m.documentation.readme else None,
                                  })

    def render_partition(self, b, p):
        template = self.env.get_template('bundle/partition.html')

        return template.render(root_path=self.root_path, p=p)



class Tables(Renderer):
    """Creates the index webpage for """

    def render_index(self):
        from jinja2 import Environment, PackageLoader

        template = self.env.get_template('toc/tables.html')

        return template.render(root_path=self.root_path, w = self.warehouse)

    def render_table(self, bundle, table):

        from jinja2 import Environment, PackageLoader

        template = self.env.get_template('table.html')

        return template.render(root_path=self.root_path, w=self.warehouse, b = bundle, table = table)


class WarehouseIndex(Renderer):
    """Creates the index webpage for awarehouse"""

    def render(self, w):

        template = self.env.get_template('warehouse.html')

        return template.render(root_path=self.root_path, w = self.warehouse)

    def render_toc(self, w, toc):
        template = self.env.get_template('toc.html')

        return template.render(root_path=self.root_path, w=self.warehouse,toc = toc)

class LibraryIndex(Renderer):

    def index(self):

        template = self.env.get_template('library_index.html')

        return template.render(root_path=self.root_path, l=self.library, w=self.warehouse)

    def tables(self):
        from collections import OrderedDict

        template = self.env.get_template('toc/tables.html')

        tables_u = []
        for b in self.library.list_bundles():
            for t in b.schema.tables:

                tables_u.append(dict(
                    bundle = b.identity,
                    name = t.name,
                    id_ = t.id_,
                    description = t.description
                ))

        tables = sorted(tables_u, key = lambda i : i['name'])

        return template.render(root_path=self.root_path, l=self.library, w=self.warehouse, tables = tables)

    def bundles(self):
        """Render the bundle Table of Contents for a library"""
        template = self.env.get_template('toc/bundles.html')

        return template.render(root_path=self.root_path, l=self.library, w=self.warehouse)


def maybe_render(extracts, cache, rel_path, render_lambda, metadata={}, force=False):
    """Check if a file exists and maybe runder it"""
    class extract_entry(object):
        def __init__(self, extracted, completed, rel_path, abs_path, data=None):
            self.extracted = extracted
            self.completed = completed  # For deleting files where exception thrown during generation
            self.rel_path = rel_path
            self.abs_path = abs_path
            self.data = data

        def __str__(self):
            return 'extracted={} completed={} rel={} abs={} data={}'.format(self.extracted,
                                                                            self.completed,
                                                                            self.rel_path, self.abs_path,
                                                                            self.data)

    if rel_path.endswith('.html'):
        metadata['content-type'] = 'text/html'

    elif rel_path.endswith('.css'):
        metadata['content-type'] = 'text/css'

    try:
        if not cache.has(rel_path) or force:
            with cache.put_stream(rel_path, metadata=metadata) as s:
                t = render_lambda()
                if t:
                    s.write(t.encode('utf-8'))
            extracted = True
        else:
            extracted = False

        completed = True

    except:
        completed = False
        extracted = True
        raise

    finally:
        extracts.append(extract_entry(extracted, completed, rel_path, cache.path(rel_path)))



def write_all_bundle_doc(cache, library=None, w=None, force=False):

    l = library if library else w.library

    for b in l.list_bundles():
        write_bundle_doc(b, cache, library, w, force)

def write_bundle_doc(b, cache, library=None, w=None, force=False):
    """ Write the bundle documentation into the documentation store """

    from ambry.text import Renderer, BundleDoc, Tables, maybe_render
    from os.path import join
    from functools import partial

    root = cache.path('', missing_ok=True)

    extracts = []

    jbp = partial(join, b.identity.path)

    mr = partial(maybe_render, extracts, cache)

    mr('css/style.css', lambda: Renderer(root).css)

    try:
        mr(jbp('index.html'), lambda: BundleDoc(root).render(w=w, b=b))

        for t in b.schema.tables:
            mr(jbp(t.vid) + '.html', lambda: Tables(root).render_table(b, t), force=force)

        return cache.path(jbp('index.html')), extracts
    except:
        b.error("Doc write failed, deleting generated files")

        for e in extracts:
            if e.completed == False and os.path.exists(e.abs_path):
                os.remove(e.abs_path)

        raise


def write_all_manifest_doc(cache, library, warehouse, force=False):

    from functools import partial

    root = cache.path('', missing_ok=True)

    extracts = []

    mr = partial(maybe_render, extracts, cache)

    md = ManifestDoc(root, library, warehouse)

    for f, m in warehouse.manifests:
        mr(m.uid+".html", lambda: md.render(m))

    return extracts

def write_doc_toc(library, warehouse, cache):
    """Create the table of contents document with links to all of the bundles and tables """
    from ambry.text import Renderer, BundleDoc, Tables, LibraryIndex, maybe_render
    from os.path import join
    from functools import partial

    root = cache.path('', missing_ok=True)

    extracts = []

    mr = partial(maybe_render, extracts, cache)

    mr('css/style.css', lambda: Renderer(root).css)

    li = LibraryIndex(root, library=library, warehouse=warehouse)

    library.logger.info('Rendering index')
    mr('index.html', lambda: li.index())

    library.logger.info('Rendering bundles')
    mr('bundles.html', lambda: li.bundles())

    library.logger.info('Rendering tables')
    mr('tables.html', lambda: li.tables())

    return cache.path('bundles.html', missing_ok=True), extracts
