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

    def render(self, m, library):

        template = self.env.get_template('manifest/layout.html')

        m.add_bundles(library)

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

        return template.render(p=p)


class WarehouseIndex(Renderer):
    """Creates the index webpage for awarehouse"""

    def render(self, w):

        template = self.env.get_template('warehouse.html')

        return template.render(root_path=self.root_path, w = self.warehouse)

    def render_toc(self, w, toc):
        template = self.env.get_template('toc.html')

        return template.render(root_path=self.root_path, w=self.warehouse,toc = toc)

class Tables(Renderer):
    """Creates the index webpage for """

    def render_index(self):
        from jinja2 import Environment, PackageLoader

        template = self.env.get_template('tables.html')

        return template.render(root_path=self.root_path, w = self.warehouse)

    def render_table(self, bundle, table):
        from jinja2 import Environment, PackageLoader

        template = self.env.get_template('table.html')

        return template.render(root_path=self.root_path, w=self.warehouse, b = bundle, table = table)