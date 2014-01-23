"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..cli import prt

def install_parser(cmd):
    lib_p = cmd.add_parser('install', help='Install configuration files')
    lib_p.set_defaults(command='install')
    asp = lib_p.add_subparsers(title='Install', help='Install configuration files')
    

def install_command(args, rc, src):
    import yaml, pkgutil
    import os
    from ambry.run import RunConfig as rc

    if args.subcommand == 'config':

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