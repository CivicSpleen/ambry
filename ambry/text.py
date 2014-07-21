"""
Support for creating web pages and text representations of schemas.
"""

import os

class ManifestDoc(object):
    def __init__(self, m):
        from jinja2 import Environment, PackageLoader

        self.m = m

        self.env = Environment(loader=PackageLoader('ambry.support.templates', 'manifest'))

    def css(self):
        import ambry.support.templates.manifest as mdir

        css_path = os.path.join(os.path.dirname(mdir.__file__), 'manifest.css')

        with open(css_path) as f:
            return f.read()


    def render(self):
        from pygments.formatters import HtmlFormatter
        template = self.env.get_template('layout.html')

        css = self.m.css + '\n' + self.css()

        return template.render(m=self.m, embed_css = css)


class PartitionDoc(object):
    def __init__(self, p):
        from jinja2 import Environment, PackageLoader

        self.p = p

        self.env = Environment(loader=PackageLoader('ambry.support.templates', 'partition'))

    def css(self):
        import ambry.support.templates.pdir as pdir

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

    def css(self):
        import ambry.support.templates.pdir as pdir

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

