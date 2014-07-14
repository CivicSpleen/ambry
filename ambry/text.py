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



