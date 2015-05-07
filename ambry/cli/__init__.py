"""Main script for the databaundles package, providing support for creating new
bundles.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""
from __future__ import print_function
import logging
import argparse

import os.path
from ambry.run import get_runconfig
import ambry._meta
from ..util import get_logger




# The Bundle's get_runconfig ( in Bundle:config ) will use this if it is set. It gets set
# by the CLI when the user assigns a specific configuration to use instead
# of the defaults.
global_run_config = None

global_logger = None  # Set in main()

# Name of the evironmental var for the config file.
AMBRY_CONFIG_ENV_VAR = 'AMBRY_CONFIG'


def prt(template, *args, **kwargs):
    # global global_logger
    print(template.format(*args, **kwargs))


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

        global_logger.critical(
            template.replace(
                '{',
                '{{').replace(
                '}',
                '}}').format(
                *args,
                **kwargs))

    sys.exit(1)


def warn(template, *args, **kwargs):
    global command
    global subcommand

    global_logger.warning(template.format(*args, **kwargs))


def load_bundle(bundle_dir):
    from ambry.run import import_file

    rp = os.path.realpath(os.path.join(bundle_dir, 'bundle.py'))
    mod = import_file(rp)

    return mod.Bundle


def _source_list(dir_):
    lst = {}
    for root, _, files in os.walk(dir_):
        if 'bundle.yaml' in files:
            bundle_class = load_bundle(root)
            bundle = bundle_class(root)

            ident = bundle.identity.dict
            ident['in_source'] = True
            ident['source_dir'] = root
            ident['source_built'] = True if bundle.is_built else False
            ident['source_version'] = ident['revision']
            lst[ident['name']] = ident

    return lst


def _print_bundle_entry(ident, show_partitions=False, prtf=prt, fields=None):
    fields = fields or []
    from datetime import datetime

    record_entry_names = ('name', 'd_format', 'p_format', 'extractor')

    def deps(ident):
        if not ident.data:
            return '.'
        if 'dependencies' not in ident.data:
            return '.'
        if not ident.data['dependencies']:
            return '0'
        return str(len(ident.data['dependencies']))

    all_fields = [
        # Name, width, d_format_string, p_format_string, extract_function
        ('deps', '{:3s}', '{:3s}', lambda ident: deps(ident)),
        ('order', '{:6s}', '{:6s}',
         lambda ident: "{major:02d}:{minor:02d}".format(
             **ident.data['order'] if 'order' in ident.data else {'major': -1, 'minor': -1})
         ),
        ('locations', '{:6s}', '{:6s}', lambda ident: ident.locations),
        ('pcount', '{:5s}', '{:5s}',
         lambda ident: str(len(ident.partitions)) if ident.partitions else ''),
        ('vid', '{:18s}', '{:20s}', lambda ident: ident.vid),
        ('time', '{:20s}', '{:20s}', lambda ident: datetime.fromtimestamp(
            ident.data['time']).isoformat() if 'time' in ident.data else ''),
        ('status', '{:20s}', '{:20s}',
         lambda ident: ident.bundle_state if ident.bundle_state else ''),
        ('vname', '{:40s}', '    {:40s}', lambda ident: ident.vname),
        ('sname', '{:40s}', '    {:40s}', lambda ident: ident.sname),
        ('fqname', '{:40s}', '    {:40s}', lambda ident: ident.fqname),
        ('source_path', '{:s}', '    {:s}', lambda ident: ident.source_path),
    ]

    if not fields:
        fields = ['locations', 'vid', 'vname']

    d_format = ""
    p_format = ""
    extractors = []

    for e in all_fields:
        # Just to make the following code easier to read
        e = dict(zip(record_entry_names, e))

        if e['name'] not in fields:
            continue

        d_format += e['d_format']
        p_format += e['p_format']

        extractors.append(e['extractor'])

    prtf(d_format, *[f(ident) for f in extractors])

    if show_partitions and ident.partitions:

        for pi in ident.partitions.values():
            prtf(p_format, *[f(pi) for f in extractors])


def _print_bundle_list(idents, subset_names=None, prtf=prt, fields=None, show_partitions=False, sort=True):
    """Create a nice display of a list of source packages."""
    fields = fields or []
    if sort:
        idents = sorted(idents, key=lambda i: i.sname)

    for ident in idents:
        _print_bundle_entry(ident, prtf=prtf, fields=fields, show_partitions=show_partitions)


def _print_info(l, ident, list_partitions=False):
    from ..identity import LocationRef

    resolved_ident = l.resolve(
        ident.vid,
        None)  # Re-resolve to get the URL or Locations

    if not resolved_ident:
        fatal("Failed to resolve while trying to print: {}", ident.vid)

    d = ident

    prt("D --- Dataset ---")
    prt("D Vid       : {}", d.vid)
    prt("D Vname     : {}", d.vname)
    prt("D Fqname    : {}", d.fqname)
    prt("D Locations : {}", str(resolved_ident.locations))
    prt("P Is Local  : {}",
        (l.cache.has(d.cache_key) is not False) if d else '')
    prt("D Rel Path  : {}", d.cache_key)
    prt("D Abs Path  : {}", l.cache.path(d.cache_key, missing_ok=True))
    if d.url:
        prt("D Web Path  : {}", d)

    # For Source Bundles
    ##

    if resolved_ident.locations.has(LocationRef.LOCATION.SOURCE):

        bundle = l.source.resolve_build_bundle(d.vid) if l.source else None

        if l.source:
            if bundle:
                prt('B Bundle Dir: {}', bundle.bundle_dir)
            else:
                source_dir = l.source.source_path(d.vid)
                prt('B Source Dir: {}', source_dir)

        if bundle and bundle.is_built:
            process = bundle.get_value_group('process')
            prt('B Partitions: {}', bundle.partitions.count)
            prt('B Created   : {}', process.get('dbcreated', ''))
            prt('B Prepared  : {}', process.get('prepared', ''))
            prt('B Built     : {}', process.get('built', ''))
            prt('B Build time: {}',
                str(round(float(process['buildtime']),
                          2)) + 's' if process.get('buildtime',
                                                   False) else '')

    else:

        bundle = l.get(d.vid)

        process = bundle.get_value_group('process')
        prt('B Partitions: {}', bundle.partitions.count)
        prt('B Created   : {}', process.get('dbcreated', ''))
        prt('B Prepared  : {}', process.get('prepared', ''))
        prt('B Built     : {}', process.get('built', ''))
        prt('B Build time: {}',
            str(round(float(process['buildtime']),
                      2)) + 's' if process.get('buildtime',
                                               False) else '')

    if ident.partitions:

        if len(ident.partitions) == 1 and not list_partitions:

            ds_ident = l.resolve(ident.partition.vid, location=None)

            # This happens when the dataset is not in the local library, I
            # think ...
            if not ds_ident:
                return

            resolved_ident = ds_ident.partition
            p = ident.partition
            prt("P --- Partition ---")
            prt("P Partition : {}; {}", p.vid, p.vname)
            prt("P Is Local  : {}", (
                l.cache.has(p.cache_key) is not False) if p else '')
            prt("P Rel Path  : {}", p.cache_key)
            prt("P Abs Path  : {}", l.cache.path(p.cache_key, missing_ok=True))
            prt("P G Cover   : {}", p.data.get('geo_coverage', ''))
            prt("P G Grain   : {}", p.data.get('geo_grain', ''))
            prt("P T Cover   : {}", p.data.get('time_coverage', ''))

            if resolved_ident.url:
                prt("P Web Path  : {}", resolved_ident.url)

        elif list_partitions:
            prt("D Partitions: {}", len(ident.partitions))
            for p in sorted(ident.partitions.values(), key=lambda x: x.vname):
                prt("P {:15s} {}", p.vid, p.vname)


def _print_bundle_info(bundle=None, ident=None):
    if ident is None and bundle:
        ident = bundle.identity

    prt('Name      : {}', ident.vname)
    prt('Id        : {}', ident.vid)

    if bundle:
        prt('Dir       : {}', bundle.bundle_dir)
    else:
        prt('URL       : {}', ident.url)

    if bundle and bundle.is_built:
        d = dict(bundle.db_config.dict)
        process = d['process']

        prt('Created   : {}', process.get('dbcreated', ''))
        prt('Prepared  : {}', process.get('prepared', ''))
        prt('Built     : {}', process.get('built', ''))
        prt('Build time: {}',
            str(round(float(process['buildtime']),
                      2)) + 's' if process.get('buildtime',
                                               False) else '')


def main(argsv=None, ext_logger=None):
    import ambry._meta
    import os

    parser = argparse.ArgumentParser(prog='ambry', description='Ambry {}. Management interface for ambry, libraries and'
                                                               ' repositories. '.format(ambry._meta.__version__))

    parser.add_argument(
        '-l',
        '--library',
        dest='library_name',
        default="default",
        help="Name of library, from the library secton of the config")
    parser.add_argument(
        '-c',
        '--config',
        default=os.getenv(AMBRY_CONFIG_ENV_VAR),
        action='append',
        help="Path to a run config file. Alternatively, set the AMBRY_CONFIG env var")
    parser.add_argument(
        '--single-config',
        default=False,
        action="store_true",
        help="Load only the config file specified")
    parser.add_argument(
        '-E',
        '--exceptions',
        default=False,
        action="store_true",
        help="Show full exception trace on all exceptions")

    cmd = parser.add_subparsers(title='commands', help='command help')

    from .library import library_parser, library_command
    from .warehouse import warehouse_command, warehouse_parser
    from .remote import remote_parser, remote_command
    from test import test_parser, test_command
    from config import config_parser, config_command
    from ckan import ckan_parser, ckan_command
    from source import source_command, source_parser
    from bundle import bundle_command, bundle_parser
    from root import root_command, root_parser
    from ..dbexceptions import ConfigurationError

    library_parser(cmd)
    warehouse_parser(cmd)
    ckan_parser(cmd)
    source_parser(cmd)
    remote_parser(cmd)
    test_parser(cmd)
    config_parser(cmd)
    bundle_parser(cmd)
    root_parser(cmd)

    args = parser.parse_args()

    if args.single_config:
        if args.config is None or len(args.config) > 1:
            raise Exception(
                "--single_config can only be specified with one -c")
        else:
            rc_path = args.config
    elif args.config is not None and len(args.config) == 1:
        rc_path = args.config.pop()
    else:
        rc_path = args.config

    funcs = {
        'bundle': bundle_command,
        'library': library_command,
        'warehouse': warehouse_command,
        'remote': remote_command,
        'test': test_command,
        'ckan': ckan_command,
        'source': source_command,
        'config': config_command,
        'root': root_command,
    }

    global global_logger

    if ext_logger:
        global_logger = ext_logger
    else:
        name = "{}.{}".format(args.command, args.subcommand)
        global_logger = get_logger(name, template="%(levelname)s: %(message)s")

    global_logger.setLevel(logging.INFO)

    f = funcs.get(args.command, None)

    if args.command == 'config' and args.subcommand == 'install':
        rc = None
    else:
        try:
            rc = get_runconfig(rc_path)

        except ConfigurationError:
            fatal("Could not find configuration file at {}\nRun 'ambry config install; to create one ", rc_path)

        global global_run_config
        global_run_config = rc


        if not rc.environment.get('category', False):
            raise ConfigurationError("Must set a config value for environment.class, one of: "
                                      "development, production, testing, staging")

        if not rc.environment.get('category', False):
            raise ConfigurationError("Must set a config value for environment.class, one of: "
                                     "development, production, testing, staging")

    if f is None:
        fatal("Error: No command: " + args.command)
    else:
        try:
            f(args, rc)
        except KeyboardInterrupt:
            prt('\nExiting...')
            pass
        except ConfigurationError as e:
            if args.exceptions:
                raise
            fatal("{}: {}".format(str(e.__class__.__name__), str(e)))
