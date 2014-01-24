
from ..cli import prt, err, warn

def config_parser(cmd):

    config_p = cmd.add_parser('config', help='Install or display the configuration')
    config_p.set_defaults(command='config')
    config_p.add_argument('-p', '--print', dest='prt', default=False, action='store_true',
                    help='Print, rather than save, the config file')

    config_p.add_argument('-f', '--force', default=False, action='store_true',
                help="Force using the default config; don't re-use the xisting config")

    asp = config_p.add_subparsers(title='Config commands', help='Configuration commands')

    sp = asp.add_parser('install', help='Install a configuration file')
    sp.set_defaults(subcommand='install')
    sp.add_argument('-r', '--root', default=None, help="Set the root dir")
    sp.add_argument('-R', '--remote', default=None, help="Url of remote library")


def config_command(args, rc):
    from  ..library import new_library

    l = new_library(rc.library(args.name))

    globals()['config_'+args.subcommand](args, l,rc)


def config_install(args, rc):
    import yaml, pkgutil
    import os
    from ambry.run import RunConfig as rc

    if not args.force and  os.path.exists(rc.ROOT_CONFIG):
        with open(rc.ROOT_CONFIG) as f:
            contents = f.read()
    else:
        contents = pkgutil.get_data("ambry.support", 'ambry.yaml')

    d = yaml.load(contents)

    if args.root:
        d['filesystem']['root_dir'] = args.root

    s =  yaml.dump(d, indent=4, default_flow_style=False)

    if args.prt:
        prt(s)
    else:

        dirname = os.path.dirname(rc.ROOT_CONFIG)

        if not os.path.isdir(dirname):
            os.makedirs(dirname)

        with open(rc.ROOT_CONFIG,'w') as f:
            f.write(s)