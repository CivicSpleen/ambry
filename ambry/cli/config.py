
from ..cli import prt, err, warn

def config_parser(cmd):
    import argparse

    config_p = cmd.add_parser('config', help='Install or display the configuration')
    config_p.set_defaults(command='config')

    asp = config_p.add_subparsers(title='Config commands', help='Configuration commands')

    sp = asp.add_parser('install', help='Install a configuration file')
    sp.set_defaults(subcommand='install')
    sp.add_argument('-t', '--template', default='devel',
                    help="Suffix of the configuration template. One of: 'devel','library','builder'. Default: 'devel' ")
    sp.add_argument('-r', '--root', default=None, help="Set the root dir")

    sp.add_argument('-R', '--remote', default=None, help="Url of remote library")
    sp.add_argument('-p', '--print', dest='prt', default=False, action='store_true',
                          help='Print, rather than save, the config file')

    group = sp.add_mutually_exclusive_group()
    group.add_argument('-e', '--edit', default=False, action='store_true', help="Edit existing file")
    group.add_argument('-f', '--force', default=False, action='store_true',
                      help="Force using the default config; don't re-use the existing config")

    sp.add_argument('args', nargs=argparse.REMAINDER, help='key=value entries') # Get everything else.

def config_command(args, rc):
    from  ..library import new_library

    globals()['config_'+args.subcommand](args, rc)

def config_install(args, rc):
    import yaml, pkgutil
    import os
    from ambry.run import RunConfig as rc
    import getpass

    user =  getpass.getuser()

    if user == 'root':
        install_file = rc.ROOT_CONFIG
    else:
        install_file = rc.USER_CONFIG
        warn(("Installing as non-root, to '{}'\n" +
              "Run as root to install for all users.").format(install_file))

    if os.path.exists(install_file):
        if args.edit:
            prt("File output file exists, editing: {}".format(install_file))
            with open(install_file) as f:
                contents = f.read()
        elif args.force:
            prt("File output file exists, overwriting: {}".format(install_file))
            contents = pkgutil.get_data("ambry.support", 'ambry-{}.yaml'.format(args.template))
        else:
            err("Output file {} exists. Use -e to edit, or -f to overwrite".format(install_file))
    else:
        contents = pkgutil.get_data("ambry.support", 'ambry-{}.yaml'.format(args.template))

    d = yaml.load(contents)

    # Set the key-value entries.
    for p in args.args:
        key,value = p.split('=')
        key_parts = key.split('.')
        e = d
        for k in key_parts:
            if k == key_parts[-1]:
                e[k]  = value

            else:
                e = e[k]

    if args.root:
        d['filesystem']['root'] = args.root

    s =  yaml.dump(d, indent=4, default_flow_style=False)

    if args.prt:
        prt(s.replace("{","{{").replace("}","}}"))
    else:

        dirname = os.path.dirname(install_file)

        if not os.path.isdir(dirname):
            os.makedirs(dirname)

        with open(install_file,'w') as f:
            f.write(s)

    if not os.path.exists(rc.USER_ACCOUNTS):
        with open(rc.USER_ACCOUNTS,'w') as f:

            d = dict(accounts=dict(
                 ambry=dict(
                     name = None,
                     email = None
                 )
            ))

            f.write(yaml.dumps(d, indent=4, default_flow_style=False))
