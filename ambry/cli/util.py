"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""
from six import iteritems

__all__ = ['command_name', 'make_parser', 'run_command']
command_name = 'util'


def make_parser(cmd):

    #
    # Library Command
    #
    lib_p = cmd.add_parser('util', help='Miscelaneous utilities')
    lib_p.set_defaults(command='util')

    asp = lib_p.add_subparsers(title='Utility commands', help='command help')

    sp = asp.add_parser('test', help='Just test the util cli')
    sp.set_defaults(subcommand='test')
    sp.add_argument('-w', '--watch', default=False, action='store_true',
                    help='Check periodically for new files.')
    sp.add_argument('-f', '--force', default=False, action='store_true', help='Push all files')
    sp.add_argument('-n', '--dry-run', default=False, action='store_true',
                    help="Dry run, don't actually send the files.")

    # ckan_export command
    sp = asp.add_parser('ckan_export', help='Export dataset to CKAN.')
    sp.set_defaults(subcommand='ckan_export')
    sp.add_argument('dvid', type=str, help='Dataset vid')
    sp.add_argument('-f', '--force', action='store_true',
                    help='Ignore existance error and continue to publish.')
    sp.add_argument('-fr', '--debug-force-restricted', action='store_true',
                    help='Export restricted datasets. For debugging only.')

    # makemigration command
    #
    sp = asp.add_parser('makemigration', help='Create empty migration (for developers only).')
    sp.set_defaults(subcommand='makemigration')
    sp.add_argument('migration_name', type=str, help='Name of the migration')

    sp = asp.add_parser('scrape', help='Scrape')
    sp.set_defaults(subcommand='scrape')
    sp.add_argument('url', nargs=1)  # Get everything else.
    group = sp.add_mutually_exclusive_group(required=True)
    group.add_argument('-d', '--doc', default=False, action='store_const',
                       const='external_documentation', dest='group',
                       help='Show documentation links')
    group.add_argument('-s', '--source', default=False, action='store_const', const='sources', dest='group',
                       help='Show sources links')
    group.add_argument('-l', '--links', default=False, action='store_const', const='links', dest='group',
                       help='Show other links')

    sp.add_argument('-c', '--csv', default=False, action='store_true', help='Output to CSV')


def run_command(args, rc):
    from ..library import new_library
    from . import global_logger

    l = new_library(rc)

    l.logger = global_logger
    globals()['util_' + args.subcommand](args, l, rc)


def util_scrape(args, l, config):
    from ambry.util import scrape
    from tabulate import tabulate
    import re

    d = scrape(l, args.url[0], as_html=True)[args.group]

    headers = 'name description url'.split()
    rows = []
    for k, v in iteritems(d):
        v['description'] = re.sub(r'\s+', ' ', v['description']).strip()
        rows.append([k, v['description'], v['url']])

    if args.csv:
        import csv
        import sys
        w = csv.writer(sys.stdout)
        w.writerow(headers)
        w.writerows(rows)
        sys.stdout.flush()
    else:
        print(tabulate(rows, headers))


def util_ckan_export(args, library, run_config):
    from ambry.orm.exc import NotFoundError
    from ambry.exporters.ckan import export, is_exported, UnpublishedAccessError
    try:
        bundle = library.bundle(args.dvid)
        if not args.force and is_exported(bundle):
            print('{} dataset is already exported. Update is not implemented!'.format(args.dvid))
            exit(1)
        else:
            try:
                export(bundle, force=args.force, force_restricted=args.debug_force_restricted)
            except UnpublishedAccessError:
                print('Did not publish because dataset access ({}) restricts publishing.'
                      .format(bundle.config.metadata.about.access))
                exit(1)
            print('{} dataset successfully exported to CKAN.'.format(args.dvid))
    except NotFoundError:
        print('Dataset with {} vid not found.'.format(args.dvid))
        exit(1)


def util_makemigration(args, l, rc):
    from ambry.orm.database import create_migration_template
    file_name = create_migration_template(args.migration_name)
    print('New empty migration created. Now populate {} with appropriate sql.'.format(file_name))
