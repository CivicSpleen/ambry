"""
Support for creating web pages and text representations of schemas.
"""

import os


# Path functions, for generating URL paths.
def bundle_path(b):
    from ambry.bundle import Bundle
    from ambry.identity import Identity
    return "bundles/{}.html".format(
        b.identity.vid if isinstance(b, Bundle) else b.vid if isinstance(b, Identity) else b.get('vid', None),
    )


def table_path(b, t):
    from ambry.bundle import Bundle
    from ambry.identity import Identity
    from ambry.orm import Table

    return "bundles/{}/tables/{}.html".format(
        b.identity.vid if isinstance(b, Bundle) else b.vid if isinstance(b, Identity) else b.get('vid', None),
        t.vid if isinstance(t, Identity) else t.vid if isinstance(t, Table) else t.get('vid', None)
    )


def partition_path(b, p):
    return "bundles/{}/partitions/{}.html".format(b.identity.vid, p.identity.vid)


def manifest_path(m):
    return "manifests/{}.html".format(m.uid)


def store_path(s):
    return "{}".format(s),


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

class Renderer(object):


    def __init__(self, cache, library = None, warehouse = None, root_path = None):

        from jinja2 import Environment, PackageLoader

        self.warehouse = warehouse
        self.library = library

        if not self.library and self.warehouse:
            self.library = self.warehouse.library
            self.logger = self.warehouse.logger
        else:
            self.logger = self.library.logger

        self.cache = cache

        self.css_files = ['css/style.css', 'css/pygments.css' ]

        self.env = Environment(loader=PackageLoader('ambry.support', 'templates'))

        if root_path is not None:
            self.root_path = root_path
        else:
            self.root_path =  cache.path('', missing_ok=True, public_url=True).rstrip("/")

        self.extracts = []

    def maybe_render(self, rel_path, render_lambda, metadata={}, force=False):
        """Check if a file exists and maybe runder it"""

        if rel_path[0] == '/':
            rel_path = rel_path[1:]

        if rel_path.endswith('.html'):
            metadata['content-type'] = 'text/html'

        elif rel_path.endswith('.css'):
            metadata['content-type'] = 'text/css'

        try:
            if not self.cache.has(rel_path) or force:

                with self.cache.put_stream(rel_path, metadata=metadata) as s:
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
            self.extracts.append(extract_entry(extracted, completed, rel_path, self.cache.path(rel_path)))



    def cc(self):
        """return common context values"""
        from functools import wraps


        def prefix_root(r,f):
            @wraps(f)
            def wrapper(*args, **kwds):
                return os.path.join(r,f(*args, **kwds))
            return wrapper

        return {
            'from_root': lambda p : os.path.join(self.root_path,p),
            'bundle_path': prefix_root(self.root_path, bundle_path),
            'table_path' : prefix_root(self.root_path, table_path),
            'partition_path': prefix_root(self.root_path, partition_path),
            'manifest_path': prefix_root(self.root_path, manifest_path),
            'store_path': prefix_root(self.root_path, store_path)
        }

    def clean(self):
        '''Clean up the extracts on failures. '''
        for e in self.extracts:
            if e.completed == False and os.path.exists(e.abs_path):
                os.remove(e.abs_path)


    def index(self):

        template = self.env.get_template('index.html')

        return template.render(root_path=self.root_path, l=self.library, w=self.warehouse, **self.cc())



    def bundles_index(self):
        """Render the bundle Table of Contents for a library"""
        template = self.env.get_template('toc/bundles.html')

        return template.render(root_path=self.root_path, l=self.library, w=self.warehouse, **self.cc())


    def tables_index(self):
        from collections import OrderedDict

        template = self.env.get_template('toc/tables.html')

        tables_u = []
        for b in self.library.list_bundles():
            for t in b.schema.tables:
                tables_u.append(dict(
                    bundle=b.identity,
                    name=t.name,
                    id_=t.id_,
                    vid=t.vid,
                    description=t.description
                ))

        tables = sorted(tables_u, key=lambda i: i['name'])

        return template.render(root_path=self.root_path, l=self.library,
                               w=self.warehouse, tables=tables, **self.cc())

    def _bundle_main(self, b):
        """This actually call the renderer for the bundle """
        import markdown

        m = b.metadata

        template = self.env.get_template('bundle/index.html')

        if not m.about.title:
            m.about.title = b.identity.vname

        return template.render(root_path=self.root_path, b=b, m=m, w=self.warehouse,
                               documentation={
                                   'main': markdown.markdown(b.sub_template(m.documentation.main)) if m.documentation.main else None,
                                   'readme': markdown.markdown(
                                       b.sub_template(m.documentation.readme)) if m.documentation.readme else None,
                               }, **self.cc())

    def bundle(self, b, force=False):
        """ Write the bundle documentation into the documentation store """
        from os.path import join
        from functools import partial

        try:
            self.maybe_render(bundle_path(b), lambda: self._bundle_main(b), force=force)
        except Exception as e:
            self.library.logger.error("Failed to render {}: {} ".format(b.identity, str(e)))
            raise

        for t in b.schema.tables:
            self.maybe_render(table_path(b,t), lambda: self.table(b, t), force=force)

        for p in b.partitions:
            if p.installed:
                self.maybe_render(partition_path(b,p), lambda: self.partition(b, p), force=force)

        return self.cache.path(bundle_path(b))

    def table(self, bundle, table):

        template = self.env.get_template('table.html')

        return template.render(root_path=self.root_path, w=self.warehouse, b=bundle, table=table, **self.cc())


    def partition(self, bundle, partition):

        template = self.env.get_template('bundle/partition.html')

        return template.render(root_path=self.root_path, w=self.warehouse, b=bundle, partition=partition, **self.cc())

    def store(self, store):

        template = self.env.get_template('store/index.html')

        return template.render(root_path=self.root_path, w=self.warehouse, s=store, **self.cc())

    def info(self, app_config, run_config):

        template = self.env.get_template('info.html')

        return template.render(root_path=self.root_path,
                               w=self.warehouse, l=self.library,
                               run_config = run_config,
                               app_config = app_config,
                               **self.cc())

    def manifest(self, f, m=None):
        """F is the file object associated with the manifest"""

        template = self.env.get_template('manifest/index.html')

        if m is None:
            from ..warehouse.manifest import Manifest

            m = Manifest(f.content)

        m.add_bundles(self.library)

        tables = {}

        if m.partitions:
            for k, b in m.bundles.items():
                tables.update(dict( [ (t, self.library.table(t)) for p in b['partitions'] for t in p['table_vids'] ]))

        table_dicts = {}

        for k,t in tables.items():

            d = t.dict
            d['description'] = t.description
            d['bundle'] = t.dataset.dict

            if self.warehouse:
                b = self.warehouse.elibrary.get(t.dataset.vid)
            else:
                b = self.library.get(t.dataset.vid)

            d['bundle']['metadata'] = b.metadata
            d['bundle']['path'] = b.identity.path

            table_dicts[k] = d

        #for line, section in m.tagged_sections(['view', 'mview']):
        #    print section.content['html']

        return template.render(root_path=self.root_path, m=m, manifest_file_ref = f,
                               tables = table_dicts.values(), **self.cc() )

    @property
    def css_dir(self):
        import ambry.support.templates as tdir

        return os.path.join(os.path.dirname(tdir.__file__),'css')


    def css_path(self, name):
        import ambry.support.templates as tdir

        return os.path.join(os.path.dirname(tdir.__file__), 'css', name)

    def write_library_doc(self, force=False, force_index = False):
        """Create the table of contents document with links to all of the bundles and tables """
        import ambry.support.templates as tdir
        from os.path import join

        root = self.cache.path('', missing_ok=True)

        for css_fn in self.css_files:
            source = os.path.join(os.path.dirname(tdir.__file__),css_fn)
            def _css():
                with open(source) as f:
                    return f.read()

            self.maybe_render(css_fn, lambda: _css(), force=force)


        self.logger.info('Rendering bundles')
        for b in self.library.list_bundles(last_version_only = False):
            self.bundle(b, force=force)

        if sum( 1 for i in self.extracts if i.extracted ):
            force_index = True

        if self.warehouse or self.library:
            self.logger.info('Rendering manifests')
            for f, m in (self.warehouse if self.warehouse else self.library).manifests:
                self.maybe_render(join('manifests',m.uid + ".html"), lambda: self.manifest(f,m), force=force or force_index )

        if sum( 1 for i in self.extracts if i.extracted ):
            force_index = True

        self.logger.info('Rendering bundles index')
        self.maybe_render('bundles.html', lambda: self.bundles_index(), force=force or force_index)

        self.logger.info('Rendering index')
        self.maybe_render('index.html', lambda: self.index(), force=force or force_index)

        #self.logger.info('Rendering tables')
        #self.maybe_render('tables.html', lambda: self.tables())


        return self.cache.path('index.html', missing_ok=True, public_url = True), self.extracts
