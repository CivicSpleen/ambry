"""
Support for creating web pages and text representations of schemas.
"""

import os

class Renderer(object):
    def __init__(self):
        import ambry.support.templates as tdir
        from jinja2 import Environment, PackageLoader

        self.css_path = os.path.join(os.path.dirname(tdir.__file__), 'css','style.css')
        self.env = Environment(loader=PackageLoader('ambry.support', 'templates'))

    @property
    def css(self):
        with open(self.css_path) as f:
            return f.read()

class ManifestDoc(Renderer):

    def render(self, m, link_database):

        template = self.env.get_template('layout.html')

        return template.render(m=self.m,  link_database = link_database)

class PartitionDoc(Renderer):

    def render(self,p):

        template = self.env.get_template('layout.html')

        return template.render(p=p)

class BundleDoc(Renderer):

    def render(self, b):

        import markdown
        m = b.metadata

        template = self.env.get_template('bundlelayout.html')

        return template.render(b=b, m=m,
                               documentation = {
                                  'main': markdown.markdown(m.documentation.main) if m.documentation.main else None,
                                  'readme': markdown.markdown(m.documentation.readme) if m.documentation.readme else None,
                                  })

class WarehouseIndex(Renderer):
    """Creates the index webpage for awarehouse"""

    def render(self, w):

        template = self.env.get_template('warehouse/layout.html')

        return template.render(w = w)

    def render_toc(self, w, toc):
        template = self.env.get_template('warehouse/toc.html')

        return template.render(w=w,toc = toc)

class Tables(Renderer):
    """Creates the index webpage for """

    def render_index(self, w, table):
        from jinja2 import Environment, PackageLoader

        template = self.env.get_template('layout.html')

        return template.render(w = w)

    def render_table(self, w, table):
        from jinja2 import Environment, PackageLoader

        template = self.env.get_template('table/table_layout.html')

        return template.render(w=w, table = table, embed_css=self.css)