"""
Support for creating web pages and text representations of schemas.
"""

import os

class Renderer(object):
    def __init__(self, root_path):
        import ambry.support.templates as tdir
        from jinja2 import Environment, PackageLoader

        self.root_path = root_path.rstrip('/')
        self.css_path = os.path.join(os.path.dirname(tdir.__file__), 'css','style.css')
        self.env = Environment(loader=PackageLoader('ambry.support', 'templates'))

    @property
    def css(self):
        with open(self.css_path) as f:
            return f.read()

class ManifestDoc(Renderer):

    def render(self, m, library):

        template = self.env.get_template('manifest/layout.html')

        m.add_bundles(library)

        return template.render(root_path=self.root_path, m=m)

class PartitionDoc(Renderer):

    def render(self,p):

        template = self.env.get_template('layout.html')

        return template.render(p=p)

class BundleDoc(Renderer):

    def render(self, w, b):

        import markdown
        m = b.metadata

        template = self.env.get_template('bundle.html')

        if not m.about.title:
            m.about.title = b.identity.vname

        return template.render(root_path=self.root_path, b=b, m=m,w=w,
                               documentation = {
                                  'main': markdown.markdown(m.documentation.main) if m.documentation.main else None,
                                  'readme': markdown.markdown(m.documentation.readme) if m.documentation.readme else None,
                                  })

class WarehouseIndex(Renderer):
    """Creates the index webpage for awarehouse"""

    def render(self, w):

        template = self.env.get_template('warehouse.html')

        return template.render(root_path=self.root_path, w = w)

    def render_toc(self, w, toc):
        template = self.env.get_template('toc.html')

        return template.render(root_path=self.root_path, w=w,toc = toc)

class Tables(Renderer):
    """Creates the index webpage for """

    def render_index(self, w, table):
        from jinja2 import Environment, PackageLoader

        template = self.env.get_template('tables.html')

        return template.render(root_path=self.root_path, w = w)

    def render_table(self, w, table):
        from jinja2 import Environment, PackageLoader

        template = self.env.get_template('table.html')

        return template.render(root_path=self.root_path, w=w, table = table, embed_css=self.css)