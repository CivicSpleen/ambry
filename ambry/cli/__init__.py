"""Main script for the databaundles package, providing support for creating new
bundles.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import argparse
import logging

import ambry._meta
import os.path
from ambry.run import get_runconfig
from six import print_
from ..util import get_logger

# The Bundle's get_runconfig ( in Bundle:config ) will use this if it is set. It gets set
# by the CLI when the user assigns a specific configuration to use instead
# of the defaults.
global_run_config = None

global_logger = None  # Set in main()

# Name of the evironmental var for the config file.
AMBRY_CONFIG_ENV_VAR = 'AMBRY_CONFIG'

def prt_no_format(template, *args, **kwargs):
    # global global_logger
    print_(template)

def prt(template, *args, **kwargs):
    # global global_logger
    print_(template.format(*args, **kwargs))


def err(template, *args, **kwargs):
    global global_logger

    global_logger.error(template.format(*args, **kwargs))


def fatal(template, *args, **kwargs):
    import sys

    global global_logger

    try:
        global_logger.critical(template.format(*args, **kwargs))
    except KeyError:
        # When the error string is a template
        global_logger.critical(template.replace('{', '{{').replace('}', '}}').format(*args, **kwargs))
    except:
        # No idea ...
        global_logger.critical(';'.join(str(e) for e in [template] + list(args) + list(kwargs.items())))

    sys.exit(1)


def warn(template, *args, **kwargs):
    global command
    global subcommand

    try:
        global_logger.warning(template.format(*args, **kwargs))
    except:
        global_logger.warning(';'.join(str(e) for e in [template] + list(args) + list(kwargs.items())))


def get_parser(commands):
    from os.path import dirname
    parser = argparse.ArgumentParser(
        prog='ambry',
        description='Ambry {}. Management interface for ambry, libraries '
                    'and repositories. '.format(ambry._meta.__version__))

    parser.add_argument('-c', '--config', default=os.getenv(AMBRY_CONFIG_ENV_VAR), action='append',
                        help='Path to a run config file. Alternatively, set the AMBRY_CONFIG env var')
    parser.add_argument('--single-config', default=False, action='store_true',
                        help='Load only the config file specified')
    parser.add_argument('-E', '--exceptions', default=False, action='store_true',
                        help='Show full exception trace on all exceptions')
    parser.add_argument('-e', '--echo', default=False, action='store_true',
                        help='Echo database queries, for debugging')
    parser.add_argument('-t', '--test-library', default=False, action='store_true',
                        help='Use the test library and database')

    cmd = parser.add_subparsers(title='commands', help='command help')

    for command_name, ( _, make_parser) in commands.items():
        make_parser(cmd)

    return parser


BASE_COMMANDS = ['ambry.cli.bundle', 'ambry.cli.config', 'ambry.cli.library', 'ambry.cli.root']


def get_extra_commands():
    """Use the configuration to discover additional CLI packages to load"""
    from ambry.run import find_config_file
    from ambry.dbexceptions import ConfigurationError
    from ambry.util import yaml

    try:
        plugins_dir = find_config_file('cli.yaml')
    except ConfigurationError:
        return []

    with open(plugins_dir) as f:
        cli_modules = yaml.load(f)

    return cli_modules


def get_commands(extra_commands=[]):
    from ambry.dbexceptions import ConfigurationError

    commands = {}

    for module_name in BASE_COMMANDS + extra_commands:
        try:
            m = __import__(module_name, fromlist=['command_name', 'make_parser', 'run_command'])
            commands[m.command_name] = (m.run_command, m.make_parser)
        except ImportError as e:
            warn("Failed to import CLI module '{}'. Ignoring it. ({}) ".format(module_name, e))

        except AttributeError:
            pass

    return commands

def main(argsv=None, ext_logger=None):
    from ..dbexceptions import ConfigurationError

    global global_logger
    # For failures in importing CLI modules. Re-set later.
    global_logger = get_logger(__name__, template='%(levelname)s: %(message)s')

    extras = get_extra_commands()

    commands =  get_commands(extras)

    parser = get_parser(commands)

    args = parser.parse_args()

    if args.single_config:
        if args.config is None or len(args.config) > 1:
            raise Exception('--single_config can only be specified with one -c')
        else:
            rc_path = args.config
    elif args.config is not None and len(args.config) == 1:
        rc_path = args.config.pop()
    else:
        rc_path = args.config


    if ext_logger:
        global_logger = ext_logger
    else:
        name = '{}.{}'.format(args.command, args.subcommand)
        global_logger = get_logger(name,  template='%(levelname)s: %(message)s')

    global_logger.setLevel(logging.INFO)

    run_command, _ = commands.get(args.command, None)

    if args.command == 'config' and args.subcommand == 'install':
        rc = None
    else:
        try:
            rc = get_runconfig(rc_path)

        except ConfigurationError as e:
            fatal("Could not find configuration file \nRun 'ambry config install; to create one ")

        global global_run_config
        global_run_config = rc

    if args.test_library:
        rc.group('filesystem')['root'] = rc.group('filesystem')['test']

    if run_command is None:
        fatal('Error: No command: ' + args.command)
    else:
        try:
            run_command(args, rc)
        except KeyboardInterrupt:
            prt('\nExiting...')
        except ConfigurationError as e:
            if args.exceptions:
                raise
            fatal('{}: {}'.format(str(e.__class__.__name__), str(e)))
