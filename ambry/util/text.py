"""Text handling and conversion utilities

Copyright (c) 2014 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import markdown

f_name = extract_data['file']

if extract_data['name'].endswith('.html') and f_name.endswith('.md'):


    with open(self.bundle.filesystem.path(f_name)) as f:
        html_body = markdown.markdown(f.read())

    template = pkgutil.get_data('ambry.support', 'extract_template.html')

    out_file = self.bundle.filesystem.path('extracts', extract_data['name'])

    with open(out_file, 'wb') as f:
        html_body = html_body.format(**dict(self.bundle.config.about))

        html = str(template).format(
            body=html_body,
            **dict(self.bundle.config.about))
        f.write(html)

    return out_file


def compile_tempate(bundle, source, template):
    from jinja2 import Environment, PackageLoader

    env = Environment(loader=PackageLoader('yourapplication', 'templates'))

def build_readme(bundle):
    """Finalize the markdown version of the documentation by interpolating bundle configuration values

    Looks first for a meta/README.md file, and if that doesn't exist, looks for

    """
    pass




def build_documentation(bundle):
    """Create the HTML version of the documentation"""
    pass


