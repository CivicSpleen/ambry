"""Install packages using pip.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import pip

from ..util import memoize


def install(install_dir, egg, url):

    initial_args = ['install', '-U',
        '--install-option=--install-purelib={}'.format(install_dir),
        url]

    try:
        # This version works for Pip 6.
        from pip.commands import InstallCommand

        cmd_name, options = pip.parseopts(initial_args)

        command = InstallCommand()

        return command.main(options)

    except:
        pass

    try:
        # An earlier version of pip
        cmd_name, options, args, parser = pip.parseopts(initial_args)

        command = InstallCommand(parser)
        return command.main(args[1:], options)

    except ValueError:
        from pip.commands import commands

        cmd_name, cmd_args = pip.parseopts(initial_args)
        command = commands[cmd_name]()

        return command.main(cmd_args)


def uninstall(install_dir, egg):

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
