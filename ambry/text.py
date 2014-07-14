"""
Support for creating web pages and text representations of schemas.
"""

import os

class ManifestDoc(object):
    def __init__(self, m):
        from jinja2 import Environment, PackageLoader

        self.m = m

        self.env = Environment(loader=PackageLoader('ambry.support.templates', 'manifest'))

    def render(self):
        template = self.env.get_template('layout.html')

        return template.render(m=self.m)



