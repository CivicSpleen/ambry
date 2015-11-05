"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""

from ..cli import warn, fatal
from . import prt


def root_parser(cmd):
    import argparse

    sp = cmd.add_parser('list', help='List bundles and partitions')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='list')
    sp.add_argument('-f', '--fields', type=str,
                    help="Specify fields to use. One of: 'locations', 'vid', 'status', 'vname', 'sname', 'fqname")
    sp.add_argument('-s', '--sort', help='Sort outputs on a field')
    group = sp.add_mutually_exclusive_group()
    group.add_argument('-t', '--tab', action='store_true', help='Print field tab seperated, without pretty table and header')
    group.add_argument('-j', '--json', action='store_true',
                    help='Output as a list of JSON dicts')
    sp.add_argument('term', nargs='?', type=str,
                    help='Name or ID of the bundle or partition')

    sp = cmd.add_parser('makemigration', help='Create empty migration (for developers only).')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='makemigration')
    sp.add_argument('migration_name', type=str, help='Name of the migration')

    sp = cmd.add_parser('info', help='Information about a bundle or partition')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='info')
    group = sp.add_mutually_exclusive_group()
    group.add_argument('-l', '--library', default=False, action='store_true',
                       help='Information about the library')
    group.add_argument('-r', '--remote', default=False, action='store_true',
                       help='Information about the remotes')
    group.add_argument('-b', '--bundle', default=False, action='store_true',
                       help='Information about a bundle')
    sp.add_argument('term', type=str, nargs='?',
                    help='Name or ID of the bundle or partition')


    sp = cmd.add_parser('doc', help='Start the documentation server')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='doc')

    sp.add_argument('-c', '--clean', default=False, action='store_true',
                    help='When used with --reindex, delete the index and old files first. ')
    sp.add_argument('-d', '--debug', default=False, action='store_true', help='Debug mode ')
    sp.add_argument('-p', '--port', help='Run on a sepecific port, rather than pick a random one')

    sp = cmd.add_parser('search', help='Search the full-text index')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='search')
    sp.add_argument('term', type=str, nargs=argparse.REMAINDER, help='Query term')
    sp.add_argument('-l', '--list', default=False, action='store_true',
                    help='List documents instead of search')
    sp.add_argument('-i', '--identifiers', default=False, action='store_true',
                    help='Search only the identifiers index')
    sp.add_argument('-r', '--reindex', default=False, action='store_true',
                    help='Generate documentation files and index the full-text search')

    sp = cmd.add_parser('sync', help='Sync with the remotes')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='sync')

    sp = cmd.add_parser('remove', help='Remove a bundle from the library')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='remove')
    sp.add_argument('term', nargs='?', type=str, help='bundle reference')

    sp = cmd.add_parser('import', help='Import multiple source directories')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='import')
    sp.add_argument('-d', '--detach', default=False, action='store_true',
                    help="Detach the imported source. Don't set the location of the imported source as the"
                    " source directory for the bundle ")
    sp.add_argument('-f', '--force', default=False, action='store_true',
                           help='Force importing an already imported bundle')
    sp.add_argument('term', nargs=1, type=str, help='Base directory')

    #
    # Search Command
    #

    sp = cmd.add_parser('search', help='Search for bundles and partitions')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='search')
    sp.add_argument('-r', '--reindex', default=False, action='store_true',
                           help='Reindex the bundles')
    sp.add_argument('terms', nargs='*', type=str, help='additional arguments')


def root_command(args, rc):
    from ..library import new_library
    from . import global_logger
    from ambry.orm.exc import DatabaseError

    try:
        l = new_library(rc)
        l.logger = global_logger
    except DatabaseError as e:
        warn('No library: {}'.format(e))
        l = None
    except Exception as e:

        warn('Failed to instantiate library: {}'.format(e))
        raise

        l = None

    globals()['root_' + args.subcommand](args, l, rc)


def root_makemigration(args, l, rc):
    from ambry.orm.database import create_migration_template
    file_name = create_migration_template(args.migration_name)
    print('New empty migration created. Now populate {} with appropriate sql.'.format(file_name))

def root_list(args, l, rc):
    from tabulate import tabulate
    import json

    if args.fields:
        header = list( str(e).strip() for e in args.fields.split(','))

        display_header = len(args.fields) > 1

    else:
        display_header = True
        header = ['vid', 'vname', 'state', 'about.title']

    records = []

    for b in l.bundles:
        records.append(b.field_row(header))

    if args.sort:
        idx = header.index(args.sort)
        records = sorted(records, key=lambda r: r[idx])

    if args.term:

        matched_records = []

        for r in records:
            if args.term in ' '.join( str(e) for e in r):
                matched_records.append(r)

        records = matched_records

    if args.tab:
        for row in records:
            print('\t'.join(str(e) for e in row))
    elif args.json:

        rows = {}

        for row in records:
            rows[row[0]] = dict(list(zip(header, row)))

        print(json.dumps(rows))

    elif display_header:
        print(tabulate(records, headers=header))
    else:
        for row in records:
            print ' '.join(row)


def root_info(args, l, rc):
    from ..cli import prt
    from ..dbexceptions import ConfigurationError

    import ambry

    prt('Version:   {}, {}', ambry._meta.__version__, rc.environment.category)
    prt('Root dir:  {}', rc.filesystem('root'))

    try:
        if l.filesystem.source():
            prt('Source :   {}', l.filesystem.source())
    except (ConfigurationError, AttributeError) as e:
        prt('Source :   No source directory')

    prt('Configs:   {}', rc.dict['loaded'])
    if l:
        prt('Library:   {}', l.database.dsn)
        prt('Remotes:   {}', ', '.join([str(r) for r in l.remotes]) if l.remotes else '')
    else:
        prt('No library defined!')

def root_sync(args, l, config):
    """Sync with the remote. For more options, use library sync
    """

    l.logger.info('args: %s' % args)

    for r in l.remotes:
        prt("Sync with remote {}", r)
        l.sync_remote(r)


def root_search(args, l, rc):

    if args.reindex:
        def tick(message):
            """Writes a tick to the stdout, without a space or newline."""
            import sys

            sys.stdout.write("\033[K{}\r".format(message))
            sys.stdout.flush()

        l.search.index_library_datasets(tick)
        return

    terms = ' '.join(args.terms)
    print(terms)

    results = l.search.search(terms)

    for r in results:
        print(r.vid, r.bundle.metadata.about.title)
        for p in r.partition_records:
            if p:
                print('    ', p.vid, p.vname)

def root_doc(args, l, rc):

    from ambry.ui import app, app_config
    import os

    import logging
    from logging import FileHandler
    import webbrowser

    app_config['port'] = args.port if args.port else 8085

    cache_dir = l.filesystem.logs()

    file_handler = FileHandler(os.path.join(cache_dir, 'web.log'))
    file_handler.setLevel(logging.WARNING)
    app.logger.addHandler(file_handler)

    print('Serving documentation for cache: ', cache_dir)

    url = 'http://localhost:{}/'.format(app_config['port'])

    if not args.debug:
        # Don't open the browser on debugging, or it will re-open on every
        # application reload
        webbrowser.open(url)
    else:
        print("Running at: {}".format(url))

    app.run(host=app_config['host'], port=int(app_config['port']), debug=args.debug)


def root_remove(args, l, rc):

    b = l.bundle(args.term)

    fqname = b.identity.fqname

    l.remove(b)

    prt("Removed {}".format(fqname))


def root_import(args, l, rc):
    import yaml
    from fs.opener import fsopendir
    from . import err
    from ambry.orm.exc import NotFoundError
    import os

    fs = fsopendir(args.term[0])

    for f in fs.walkfiles(wildcard='bundle.yaml'):

        prt("Visiting {}".format(f))
        config = yaml.load(fs.getcontents(f))

        if not config:
            err("Failed to get a valid bundle configuration from '{}'".format(f))

        bid = config['identity']['id']

        try:
            b = l.bundle(bid)

            if not args.force:
                prt("Skipping existing  bundle: {}".format(b.identity.fqname))
                continue

        except NotFoundError:
            b = None

        if not b:
            b = l.new_from_bundle_config(config)
            prt("Loading bundle: {}".format(b.identity.fqname))
        else:
            prt("Loading existing bundle: {}".format(b.identity.fqname))

        b.set_file_system(source_url=os.path.dirname(fs.getsyspath(f)))

        b.sync_in()

        if args.detach:
            b.set_file_system(source_url=None)