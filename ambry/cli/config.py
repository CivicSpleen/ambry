
from ..cli import prt, fatal, warn, err


def config_parser(cmd):
    config_p = cmd.add_parser(
        'config',
        help='Install or display the configuration')
    config_p.set_defaults(command='config')

    asp = config_p.add_subparsers(
        title='Config commands',
        help='Configuration commands')

    sp = asp.add_parser('install', help='Install a configuration file')
    sp.set_defaults(subcommand='install')
    sp.add_argument(
        '-t',
        '--template',
        default='devel',
        help="Suffix of the configuration template. One of: 'devel', 'library', 'builder'. Default: 'devel' ")
    sp.add_argument('-r', '--root', default=None, help="Set the root dir")

    sp.add_argument(
        '-R',
        '--remote',
        default=None,
        help="Url of remote library")
    sp.add_argument(
        '-p',
        '--print',
        dest='prt',
        default=False,
        action='store_true',
        help='Print, rather than save, the config file')

    group = sp.add_mutually_exclusive_group()
    group.add_argument(
        '-e',
        '--edit',
        default=False,
        action='store_true',
        help="Edit existing file")
    group.add_argument(
        '-f',
        '--force',
        default=False,
        action='store_true',
        help="Force using the default config; don't re-use the existing config")

    sp.add_argument(
        'args',
        nargs='*',
        help='key=value entries')  # Get everything else.

    sp = asp.add_parser(
        'value',
        help='Return a configuration value, or all values if no key is specified')
    sp.set_defaults(subcommand='value')
    sp.add_argument(
        '-y',
        '--yaml',
        default=False,
        action='store_true',
        help="If no key is specified, return the while configuration as yaml")
    sp.add_argument('key', nargs='*', help='Value key')  # Get everything else.


def config_command(args, rc):
    globals()['config_' + args.subcommand](args, rc)


def config_install(args, rc):
    import yaml
    import pkgutil
    import os
    from ambry.run import RunConfig as rc
    import getpass

    edit_args = ' '.join(args.args)

    user = getpass.getuser()

    if user == 'root':
        install_file = rc.ROOT_CONFIG
        default_root = '/ambry'
    elif os.getenv('VIRTUAL_ENV'):  # Special case for python virtual environments
        install_file = os.path.join(os.getenv('VIRTUAL_ENV'), '.ambry.yaml')
        default_root = os.path.join(os.getenv('VIRTUAL_ENV'), 'data')
    else:
        install_file = rc.USER_CONFIG
        warn(("Installing as non-root, to '{}'\n" +
              "Run as root to install for all users.").format(install_file))
        default_root = os.path.join(os.path.expanduser('~'), 'ambry')

    if os.path.exists(install_file):
        if args.edit:
            prt("File output file exists, editing: {}".format(install_file))
            with open(install_file) as f:
                contents = f.read()
        elif args.force:
            prt("File output file exists, overwriting: {}".format(
                install_file))
            contents = pkgutil.get_data(
                "ambry.support", 'ambry-{}.yaml'.format(args.template))
        else:
            fatal(
                "Output file {} exists. Use -e to edit, or -f to overwrite".format(install_file))
    else:
        contents = pkgutil.get_data(
            "ambry.support", 'ambry-{}.yaml'.format(args.template))

    d = yaml.load(contents)

    # Set the key-value entries.
    if edit_args:
        key, value = edit_args.split('=')

        value = value.strip()
        key = key.strip()
        key_parts = key.split('.')
        e = d
        for k in key_parts:
            k = k.strip()
            print k, str(key_parts[-1])
            if str(k) == str(key_parts[-1]):
                e[k] = value
            else:
                
                e = e[k]
                

    if args.root:
        d['filesystem']['root'] = args.root
    elif default_root:
        d['filesystem']['root'] = default_root

    if args.remote:
        try:
            d['library']['default']['remotes'] = [args.remote]
        except Exception as e:
            err("Failed to set remote: {} ".format(e))

    s = yaml.dump(d, indent=4, default_flow_style=False)

    if args.prt:
        prt(s.replace("{", "{{").replace("}", "}}"))

    else:

        dirname = os.path.dirname(install_file)

        if not os.path.isdir(dirname):
            os.makedirs(dirname)

        with open(install_file, 'w') as f:
            prt('Writing config file: {}'.format(install_file))
            f.write(s)

    if not os.path.exists(rc.USER_ACCOUNTS):
        with open(rc.USER_ACCOUNTS, 'w') as f:

            d = dict(accounts=dict(ambry=dict(name=None, email=None)))

            prt('Writing config file: {}'.format(rc.USER_ACCOUNTS))
            f.write(yaml.dump(d, indent=4, default_flow_style=False))

    # Make the directories.

    from ..run import get_runconfig
    rc = get_runconfig(install_file)

    for name in rc.group('filesystem').keys():
        fs = rc.filesystem(name)

        try:
            dr = fs['dir']
            if not os.path.exists(dr):
                prt("Making directory: {}".format(dr))
                os.makedirs(dr)
        except KeyError:
            pass


def config_value(args, rc):

    def sub_value(value, subs):

        if isinstance(value, (list, tuple)):
            return [i.format(**subs) for i in value]
        else:
            try:
                return value.format(**subs)
            except AttributeError:
                return str(value)

    def dump_key(key, subs):
        for path, value in rc.config.flatten():
            dot_path = '.'.join(path)
            if key:
                if key == dot_path:
                    print sub_value(value, subs)
                    return
            else:
                print dot_path, '=', sub_value(value, subs)

    subs = dict(root=rc.filesystem_path('root'))

    if not args.key:
        if args.yaml:
            print rc.dump()
        else:
            dump_key(None, subs)
    else:
        dump_key(args.key[0], subs)
