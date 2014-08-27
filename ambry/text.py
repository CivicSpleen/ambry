"""
Support for creating web pages and text representations of schemas.
"""

import os

class ManifestDoc(object):
    def __init__(self, m, link_database=False):
        from jinja2 import Environment, PackageLoader

        self.m = m
        self.link_database = link_database

        self.env = Environment(loader=PackageLoader('ambry.support.templates', 'manifest'))

    @property
    def css(self):
        import ambry.support.templates.manifest as mdir

        css_path = os.path.join(os.path.dirname(mdir.__file__), 'manifest.css')

        with open(css_path) as f:
            return f.read()


    def render(self):
        from pygments.formatters import HtmlFormatter
        template = self.env.get_template('layout.html')

        css = self.m.css + '\n' + self.css

        return template.render(m=self.m, embed_css = css, link_database = self.link_database)

class PartitionDoc(object):
    def __init__(self, p):
        from jinja2 import Environment, PackageLoader

        self.p = p

        self.env = Environment(loader=PackageLoader('ambry.support.templates', 'partition'))

    @property
    def css(self):
        import ambry.support.templates.partition as pdir

        css_path = os.path.join(os.path.dirname(pdir.__file__), 'partition.css')

        with open(css_path) as f:
            return f.read()


    def render(self):

        template = self.env.get_template('layout.html')

        return template.render(p=self.p, embed_css=self.css)

class BundleDoc(object):
    def __init__(self, b):
        from jinja2 import Environment, PackageLoader

        self.b = b

        self.env = Environment(loader=PackageLoader('ambry.support.templates', 'bundle'))

    @property
    def css(self):
        import ambry.support.templates.bundle as pdir

        css_path = os.path.join(os.path.dirname(pdir.__file__), 'bundle.css')

        with open(css_path) as f:
            return f.read()


    def render(self):

        template = self.env.get_template('layout.html')

        import markdown
        m = self.b.metadata
        return template.render(b=self.b, m=m,
                               documentation = {
                                  'main': markdown.markdown(m.documentation.main) if m.documentation.main else None,
                                  'readme': markdown.markdown(m.documentation.readme) if m.documentation.readme else None,
                                  },
                               embed_css=self.css)

class WarehouseIndex(object):
    """Creates the index webpage for awarehouse"""
    def __init__(self, w):
        from jinja2 import Environment, PackageLoader

        self.env = Environment(loader=PackageLoader('ambry.support.templates', 'warehouse_index'))

        self.w = w

    @property
    def css(self):
        import ambry.support.templates.warehouse_index as pdir

        css_path = os.path.join(os.path.dirname(pdir.__file__), 'index.css')

        with open(css_path) as f:
            return f.read()


    def render(self):

        template = self.env.get_template('layout.html')

        return template.render(w = self.w, embed_css=self.css)

    def render_toc(self, toc):
        template = self.env.get_template('toc.html')

        return template.render(w=self.w,toc = toc,  embed_css=self.css)



class Tables(object):
    """Creates the index webpage for """
    def __init__(self, w, table):

        self.w = w
        self.table = table

    @property
    def css(self):
        import ambry.support.templates.warehouse_index as pdir

        css_path = os.path.join(os.path.dirname(pdir.__file__), 'index.css')

        with open(css_path) as f:
            return f.read()

    def render_index(self):
        from jinja2 import Environment, PackageLoader

        self.env = Environment(loader=PackageLoader('ambry.support.templates', 'table'))

        template = self.env.get_template('layout.html')

        return template.render(w = self.w, embed_css=self.css)

    def render_table(self):
        from jinja2 import Environment, PackageLoader

        self.env = Environment(loader=PackageLoader('ambry.support.templates', 'table'))

        template = self.env.get_template('table_layout.html')

        return template.render(w=self.w, table = self.table, embed_css=self.css)