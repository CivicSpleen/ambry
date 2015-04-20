"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..cli import prt


def ckan_parser(cmd):
    lib_p = cmd.add_parser('ckan', help='Access a CKAN repository')
    lib_p.set_defaults(command='ckan')
    lib_p.add_argument(
        '-n',
        '--name',
        default='default',
        help='Select the configuration name for the repository')
    asp = lib_p.add_subparsers(
        title='CKAN commands',
        help='Access a CKAN repository')

    sp = asp.add_parser(
        'package',
        help='Dump a package by name, as json or yaml')
    sp.set_defaults(subcommand='package')
    sp.add_argument('term', type=str, help='Query term')
    group = sp.add_mutually_exclusive_group()
    group.add_argument(
        '-y',
        '--yaml',
        default=True,
        dest='use_json',
        action='store_false')
    group.add_argument(
        '-j',
        '--json',
        default=True,
        dest='use_json',
        action='store_true')


def ckan_command(args, rc):
    from ambry.dbexceptions import ConfigurationError
    import ambry.client.ckan
    import requests

    repo_name = args.name

    repo_config = rc.datarepo(repo_name)

    api = ambry.client.ckan.Ckan(repo_config.url, repo_config.key)

    if args.subcommand == 'package':
        try:
            pkg = api.get_package(args.term)
        except requests.exceptions.HTTPError:
            return

        if args.use_json:
            import json
            print(
                json.dumps(
                    pkg,
                    sort_keys=True,
                    indent=4,
                    separators=(
                        ',',
                        ': ')))
        else:
            import yaml
            yaml.dump(args, indent=4, default_flow_style=False)

    else:
        pass
