
__all__ = ['command_name', 'make_parser', 'run_command']
command_name = 'config'
command_name = 'config'

from six import iterkeys, iteritems
from ..cli import prt, fatal, warn, err


def make_parser(cmd):
    config_p = cmd.add_parser('config', help='Install or display the configuration')
    config_p.set_defaults(command='config')

    asp = config_p.add_subparsers(title='Config commands', help='Configuration commands')

    sp = asp.add_parser('install', help='Install a configuration file')
    sp.set_defaults(subcommand='install')
    sp.add_argument(
        '-t', '--template', default='devel',
        help="Suffix of the configuration template. One of: 'devel', 'library', 'builder'. Default: 'devel' ")
    sp.add_argument('-r', '--root', default=None, help="Set the root dir")
    sp.add_argument('-p', '--print', dest='prt', default=False, action='store_true',
        help='Print, rather than save, the config file')

    group = sp.add_mutually_exclusive_group()
    group.add_argument(
        '-f', '--force', default=False, action='store_true',
        help="Force using the default config; don't re-use the existing config")

    sp.add_argument('args', nargs='*', help='key=value entries')  # Get everything else.

    sp = asp.add_parser('value', help='Return a configuration value, or all values if no key is specified')
    sp.set_defaults(subcommand='value')
    sp.add_argument('-y', '--yaml', default=False, action='store_true', help="Return the result as YAML")
    sp.add_argument('-j', '--json', default=False, action='store_true', help="Return the result as JSON")
    sp.add_argument('key', nargs='*', help='Value key')  # Get everything else.

    sp = asp.add_parser('password', help='Set a password for a service')
    sp.set_defaults(subcommand='password')
    group.add_argument('-d', '--delete', default=False, action='store_true', help="Delete the password")
    sp.add_argument('service', metavar='service', nargs=1, help='Service name, usually a hostname')  # Get everything else.
    sp.add_argument('username', metavar='username', nargs=1, help='username')

    sp = asp.add_parser('edit', help='Edit the config file by setting a value for a key. ')
    sp.set_defaults(subcommand='edit')
    sp.add_argument('-y', '--yaml', default=False, action='store_true', help="Load the edits as a YAML string")
    sp.add_argument('-j', '--json', default=False, action='store_true', help="Load the edits as a JSON string")
    sp.add_argument('args', nargs='*', help='key=value entries, YAML or JSON')  # Get everything else.

    sp = asp.add_parser('dump', help='Dump the config file')
    sp.set_defaults(subcommand='dump')
    sp.add_argument('args', nargs='*', help='key=value entries')  # Get everything else.

    sp = asp.add_parser('installcli', help='Install a CLI module')
    sp.set_defaults(subcommand='installcli')
    sp.add_argument('modules', nargs='*', help='Module names')  # Get everything else.


def run_command(args, rc):
    from ..library import new_library
    from . import global_logger

    try:
        l = new_library(rc)
        l.logger = global_logger
    except Exception as e:
        l = None
        if args.subcommand != 'install':
            warn("Failed to setup library: {} ".format(e))

    globals()['config_' + args.subcommand](args, l, rc)

def config_edit(args, l, rc):
    from ambry.dbexceptions import ConfigurationError
    from ambry.util import AttrDict

    edit_args = ' '.join(args.args)

    if args.yaml or args.json:
        if args.yaml:
            import yaml
            v = yaml.load(edit_args)
        elif args.json:
            import json
            v = json.loads(edit_args)

        d = AttrDict()
        d.update(v)

        print d

        rc.config.update_flat(d.flatten())

    else:
        key, value = edit_args.split('=')

        value = value.strip()
        key = key.strip()
        key_parts = key.split('.')
        e = rc.config
        for k in key_parts:
            k = k.strip()
            #print(k, str(key_parts[-1]))
            if str(k) == str(key_parts[-1]):
                e[k] = value
            else:
                e = e[k]


    configs = rc.config['loaded']['configs']

    if len(configs) != 1:
        raise ConfigurationError("Configuration was loaded from multiple files; don't know which to edit; "
                                 "'{}'".format(configs))

    try:
        del rc.config['accounts']
    except KeyError:
        pass

    try:
        del rc.config['loaded']
    except KeyError:
        pass

    with open(configs[0], 'w') as f:
        rc.config.dump(f)

def config_install(args, l, rc):
    import yaml
    import pkgutil
    import os
    from os.path import join, dirname
    import getpass
    import ambry.support
    from ambry.run import ROOT_DIR, USER_DIR, CONFIG_FILE, ACCOUNTS_FILE
    from ambry.util import AttrDict

    user = getpass.getuser()

    default_config_file = join(dirname(ambry.support.__file__),'ambry-{}.yaml'.format(args.template))

    d = AttrDict().update_yaml(default_config_file)

    user_config_dir = os.path.join(os.path.expanduser('~'), USER_DIR)

    if user == 'root': # Root user
        config_dir = ROOT_DIR
        default_root = d.library.filesystem_root

    elif os.getenv('VIRTUAL_ENV'):  # Special case for python virtual environments
        config_dir = os.path.join(os.getenv('VIRTUAL_ENV'), USER_DIR)
        default_root = os.path.join(os.getenv('VIRTUAL_ENV'), 'data')

    else: # Non-root user, outside of virtualenv
        config_dir = user_config_dir
        warn(("Installing as non-root, to '{}'\n" +
              "Run as root to install for all users.").format(config_dir))
        default_root = os.path.join(os.path.expanduser('~'), 'ambry')

    if args.root:
        default_root = args.root

    if not os.path.exists(user_config_dir):
        os.makedirs(user_config_dir)

    if not os.path.exists(config_dir):
        os.makedirs(config_dir)

    if os.path.exists(os.path.join(config_dir, CONFIG_FILE)):
        if args.force:
            prt("File output file exists, overwriting: {}".format(config_dir))

        else:
            fatal("Output file {} exists. Use  -f to overwrite".format(config_dir))

    d['library']['filesystem_root'] = default_root

    s = d.dump()

    if args.prt:
        prt(s.replace("{", "{{").replace("}", "}}"))
        return

    #Create an empty accounts file, if it does not exist
    user_accounts_file =os.path.join(user_config_dir, ACCOUNTS_FILE)

    if not os.path.exists(user_accounts_file):
        with open(user_accounts_file, 'w') as f:
            from ambry.util import random_string
            d = dict(accounts=dict(
                        password=random_string(16),
                        ambry=dict(
                            name=None, email=None
                        )
                    )
                )

            prt('Writing accounts file: {}'.format(user_accounts_file))
            f.write(yaml.dump(d, indent=4, default_flow_style=False))

    config_file = os.path.join(config_dir, CONFIG_FILE)

    with open(config_file, 'w') as f:
        prt('Writing config file: {}'.format(config_file))
        f.write(s)

    # Make the directories.

    from ..run import get_runconfig
    rc = get_runconfig(config_file)

    for name, v in iteritems(rc.filesystem):
        dr = v.format(root=rc.library.filesystem_root)

        try:

            if not os.path.exists(dr):
                prt("Making directory: {}".format(dr))
                os.makedirs(dr)
        except KeyError:
            pass


def config_value(args, l, rc):
    from ambry.util import AttrDict

    def sub_value(value, subs):

        if isinstance(value, (list, tuple)):
            return [i.format(**subs) for i in value]
        else:
            try:
                return value.format(**subs)
            except AttributeError:
                return str(value)

    def dump_key(key, subs):
        values = []

        for path, value in rc.config.flatten():

            dot_path = '.'.join(path)
            if key:
                if key == dot_path: # Exact matches
                    return sub_value(value, subs)

                elif dot_path.startswith(key):
                    values.append((dot_path.split('.'), sub_value(value, subs) ))

            else:
                return ''.join(dot_path, '=', sub_value(value, subs))


        if not values:
            return

        d = AttrDict()

        d.update_flat(values)

        return d


    subs = dict(root=rc.filesystem('root')) # Interpolations

    key = args.key[0] if args.key[0] else None

    if args.yaml:
        v = dump_key(key, subs)
        if isinstance(v, AttrDict):
            print v.dump()
        else:
            import yaml
            print yaml.dump(v)
            print 'X', v
    elif args.json:
        import json
        print json.dumps(dump_key(key, subs))
    else:
        print dump_key(key, subs)

def config_dump(args, l, rc):

    print rc.dump()


def config_installcli(args, l, rc):

    from ambry import config
    from os.path import dirname, join, exists
    import yaml
    import sys


    try:
        config_dir = dirname(rc.loaded[0])
    except KeyError:
        fatal("ERROR: Failed to properly load config")
        sys.exit(1)

    if not exists(config_dir):
        fatal("ERROR: Failed to find config directory: {}".format(config_dir))
        sys.exit(1)

    cli_path = join(config_dir, 'cli.yaml')

    try:
        with open(cli_path, 'rt') as f:
            clis = set(yaml.load(f))
    except IOError:
        clis = set()

    for module_name in args.modules:


        m = __import__(module_name)

        for cmd in m.commands:
            prt("Adding {} from {}".format(cmd, module_name))
            clis.add(cmd)

    with open(cli_path, 'wt') as f:
        yaml.dump(list(clis), f, default_flow_style=False)

    prt("Wrote updated cli config to: {}".format(cli_path))