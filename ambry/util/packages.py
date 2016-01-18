"""Install packages using pip.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import pip

from ..util import memoize


def install(install_dir, egg, url):
    import subprocess

    initial_args = ['pip', 'install', '-U', '--install-option=--install-purelib={}'.format(install_dir), url]

    output = subprocess.check_output(initial_args)

    print '!!!!', output

def uninstall(install_dir, egg):

    raise NotImplementedError()

    from pip.commands import UninstallCommand

    initial_args = ['uninstall', egg]

    cmd_name, options = pip.parseopts(initial_args)

    command = UninstallCommand()

    return command.main(options)


def qualified_name(o):
    """Return the fully qualfied name of the class of an object."""

    return o.__module__ + '.' + o.__class__.__name__


@memoize
def import_class_by_string(name):
    """Return a class by importing its module from a fully qualified string."""
    components = name.split('.')
    clazz = components.pop()
    mod = __import__('.'.join(components))

    components += [clazz]
    for comp in components[1:]:
        mod = getattr(mod, comp)

    return mod
