"""
Support for creating web pages and text representations of schemas.
"""

import os


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

    def __init__(self, cache, library = None, warehouse = None):
        import ambry.support.templates as tdir
        from jinja2 import Environment, PackageLoader

        self.warehouse = warehouse
        self.library = library

        if not self.library and self.warehouse:
            self.library = self.warehouse.library
            self.logger = self.warehouse.logger
        else:
            self.logger = self.library.logger

        self.cache = cache
        self.css_path = os.path.join(os.path.dirname(tdir.__file__), 'css','style.css')
        self.env = Environment(loader=PackageLoader('ambry.support', 'templates'))

        self.root_path =  cache.path('', missing_ok=True)

        self.extracts = []



    @property
    def css(self):
        with open(self.css_path) as f:
            return f.read()


    def maybe_render(self, rel_path, render_lambda, metadata={}, force=False):
        """Check if a file exists and maybe runder it"""

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


    def clean(self):
        '''Clean up the extracts on failures. '''
        for e in self.extracts:
            if e.completed == False and os.path.exists(e.abs_path):
                os.remove(e.abs_path)


    def index(self):

        template = self.env.get_template('library_index.html')

        return template.render(root_path=self.root_path, l=self.library, w=self.warehouse)

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
                    description=t.description
                ))

        tables = sorted(tables_u, key=lambda i: i['name'])

        return template.render(root_path=self.root_path, l=self.library, w=self.warehouse, tables=tables)



    def bundles_index(self):
        """Render the bundle Table of Contents for a library"""
        template = self.env.get_template('toc/bundles.html')

        return template.render(root_path=self.root_path, l=self.library, w=self.warehouse)


    def _bundle_main(self, b):

        import markdown

        m = b.metadata

        template = self.env.get_template('bundle.html')

        if not m.about.title:
            m.about.title = b.identity.vname

        return template.render(root_path=self.root_path, b=b, m=m, w=self.warehouse,
                               documentation={
                                   'main': markdown.markdown(m.documentation.main) if m.documentation.main else None,
                                   'readme': markdown.markdown(
                                       m.documentation.readme) if m.documentation.readme else None,
                               })

    def bundle(self, b):
        """ Write the bundle documentation into the documentation store """
        from os.path import join
        from functools import partial

        jbp = partial(join, b.identity.path)

        self.maybe_render(jbp('index.html'), lambda: self._bundle_main(b))

        for t in b.schema.tables:
            self.maybe_render(jbp(t.vid) + '.html', lambda: self.table(b, t))


        for p in b.partitions:
            if p.installed:
                self.maybe_render(jbp(p.vid) + '.html', lambda: self.partition(b, p))

        return self.cache.path(jbp('index.html'))

    def table(self, bundle, table):

        template = self.env.get_template('table.html')

        return template.render(root_path=self.root_path, w=self.warehouse, b=bundle, table=table)

    def tables_index(self, bundle, table):

        template = self.env.get_template('table.html')

        return template.render(root_path=self.root_path, w=self.warehouse, b=bundle, table=table)

    def partition(self, bundle, partition):

        template = self.env.get_template('bundle/partition.html')

        return template.render(root_path=self.root_path, w=self.warehouse, b=bundle, partition=partition)

    def manifest(self, m):

        template = self.env.get_template('manifest/layout.html')

        m.add_bundles(self.library)

        return template.render(root_path=self.root_path, m=m)

    def write_library_doc(self):
        """Create the table of contents document with links to all of the bundles and tables """

        root = self.cache.path('', missing_ok=True)

        self.maybe_render('css/style.css', lambda: self.css)

        self.logger.info('Rendering index')
        self.maybe_render('index.html', lambda: self.index())

        self.logger.info('Rendering bundles')
        for b in self.library.list_bundles():
            self.bundle(b)

        self.logger.info('Rendering bundles index')
        self.maybe_render('bundles.html', lambda: self.bundles_index())

        if self.warehouse:
            self.logger.info('Rendering manifests')
            for f, m in self.warehouse.manifests:
                self.maybe_render(m.uid + ".html", lambda: self.manifest(m))

        #self.logger.info('Rendering tables')
        #self.maybe_render('tables.html', lambda: self.tables())


        return self.cache.path('index.html', missing_ok=True), self.extracts
