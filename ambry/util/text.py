"""Text handling and conversion utilities

Copyright (c) 2014 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


def compile_tempate(bundle, source, template):
    from jinja2 import Environment, PackageLoader, DebugUndefined

    env = Environment(loader=PackageLoader('ambry.support','templates'))

    template = env.get_template('default_documentation.md.jinja')

    c = bundle.metadata.dict

    #import pprint
    #pprint.pprint(c)

    return template.render(**c)

def build_readme(bundle):
    """Finalize the markdown version of the documentation by interpolating bundle configuration values

    Looks first for a meta/README.md file, and if that doesn't exist, looks for

    """
    pass




def build_documentation(bundle):
    """Create the HTML version of the documentation"""
    pass


