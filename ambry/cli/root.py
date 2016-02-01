"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""

__all__ = ['command_name', 'make_parser', 'run_command']
command_name = 'root'

from ..cli import warn
from . import prt
from six import print_
from ambry.orm.exc import NotFoundError
from . import fatal

def make_parser(cmd):
    import argparse

    sp = cmd.add_parser('list', help='List bundles and partitions')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='list')
    sp.add_argument('-f', '--fields', type=str,
                    help="Specify fields to use. One of: 'locations', 'vid', 'status', 'vname', 'sname', 'fqname")
    sp.add_argument('-s', '--sort', help='Sort outputs on a field')
    group = sp.add_mutually_exclusive_group()
    group.add_argument('-t', '--tab', action='store_true',
                       help='Print field tab seperated, without pretty table and header')
    group.add_argument('-p', '--partitions', action='store_true',
                       help='List partitions instead of bundles')
    group.add_argument('-j', '--json', action='store_true',
                       help='Output as a list of JSON dicts')
    sp.add_argument('term', nargs='?', type=str,
                    help='Name or ID of the bundle or partition')

    sp = cmd.add_parser('info', help='Information about a bundle or partition')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='info')
    group = sp.add_mutually_exclusive_group()
    group.add_argument('-C', '--config-path', default=False, action='store_true',
                       help='Print just the main config path')
    group.add_argument('-c', '--configs', default=False, action='store_true',
                       help=' Also dump the root config entries')
    group.add_argument('-r', '--remote', default=False, action='store_true',
                       help='Information about the remotes')
    group.add_argument('-a', '--accounts', default=False, action='store_true',
                       help='Information about accounts')


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
    sp.add_argument('-a', '--all', default=False, action='store_true', help='Sync with all remotes')
    sp.add_argument('refs', nargs='*', type=str, help='Names of a remote or a bundle references')

    sp = cmd.add_parser('remove', help='Remove a bundle from the library')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='remove')
    sp.add_argument('term', nargs='*', type=str, help='bundle reference')

    sp = cmd.add_parser('import', help='Import multiple source directories')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='import')
    sp.add_argument('-d', '--detach', default=False, action='store_true',
                    help="Detach the imported source. Don't set the location of the imported source as the"
                    " source directory for the bundle ")
    sp.add_argument('-f', '--force', default=False, action='store_true',
                    help='Force importing an already imported bundle')
    sp.add_argument('term', nargs="*", type=str, help='Base directory')

    # Search Command
    #

    sp = cmd.add_parser('search', help='Search for bundles and partitions')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='search')
    sp.add_argument('-r', '--reindex', default=False, action='store_true',
                    help='Reindex the bundles')
    sp.add_argument('terms', nargs='*', type=str, help='additional arguments')


def run_command(args, rc):
    from . import global_logger

    if args.test_library:
        rc.set_lirbary_database('test')

    try:
        from ambry.library import global_library, Library
        global global_library

        l = Library(rc, echo=args.echo)

        global_library = l

        l.logger = global_logger
        l.sync_config()

    except Exception as e:
        if args.subcommand != 'info':
            warn('Failed to instantiate library: {}'.format(e))
        l = None

        if args.exceptions:
            raise

    try:
        globals()['root_' + args.subcommand](args, l, rc)
    except NotFoundError as e:
        if args.exceptions:
            raise
        fatal(e)
    except Exception:
        raise


def root_list(args, l, rc):
    from tabulate import tabulate
    import json
    from . import fatal

    if not l:
        fatal('No database')

    if args.fields:
        header = list(str(e).strip() for e in args.fields.split(','))

        display_header = len(args.fields) > 1

    elif not args.partitions:
        display_header = True
        header = ['vid', 'vname', 'dstate', 'bstate', 'about.title']
    else:
        header = ['vid', 'vname', 'dstate', 'bstate', 'table']

    records = []

    if args.term and '='in args.term:
        search_key, search_value = args.term.split('=')
        args.term = None
    else:
        search_key, search_value = None, None

    for b in l.bundles:

        if search_key:
            d = dict(b.metadata.kv)
            v = d.get(search_key, None)
            if v and search_value and v.strip() == search_value.strip():
                records.append(b.field_row(header))

        elif not args.partitions:
            records.append(b.field_row(header))
        else:
            for p in b.partitions:
                records.append



    idx = header.index(args.sort) if args.sort else 1
    records = sorted(records, key=lambda r: r[idx])


    if args.term:

        matched_records = []

        for r in records:
            if args.term in ' '.join(str(e) for e in r):
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
    from tabulate import tabulate
    from ambry.library.filesystem import LibraryFilesystem
    from ambry.util.text import ansicolors
    from ambry.util import drop_empty
    from ambry.orm import Account
    from ambry.orm.database import SCHEMA_VERSION

    import ambry

    if args.config_path:
        prt(rc.loaded[0])
        return

    prt('Version:   {}', ambry._meta.__version__)
    prt('Schema:    {}', SCHEMA_VERSION)
    prt('Root dir:  {}', rc.library.filesystem_root)

    try:
        if l.filesystem.source():
            prt('Source :   {}', l.filesystem.source())
    except (ConfigurationError, AttributeError) as e:
        prt('Source :   No source directory')

    prt('Config:    {}', rc.loaded[0])
    prt('Accounts:  {}', rc.accounts.loaded[0])
    if l:
        prt('Library:   {}', l.database.dsn)
        prt('Remotes:   {}', ', '.join([str(r.short_name) for r in l.remotes]) if l.remotes else '')
    else:
        fs = LibraryFilesystem(rc)
        prt('Library:   {} {}(Inaccessible!){}', fs.database_dsn, ansicolors.FAIL, ansicolors.ENDC)

    if args.configs:
        ds = l.database.root_dataset
        prt("Configs:")
        records = []
        for config in ds.configs:
            # Can't use prt() b/c it tries to format the {} in the config.value
            records.append((config.dotted_key, config.value))

        print tabulate(sorted(records, key=lambda e: e[0]), headers=['key', 'value'])

    if args.accounts:

        headers = 'Id Service User Access Url'.split()

        records = []

        for k in l.accounts.keys():

            acct = l.account(k)

            records.append([acct.account_id, acct.major_type, acct.user_id, acct.access_key, acct.url])

        accounts = [v for k, v in l.accounts.items()]

        if not records:
            return

        records = drop_empty([headers] + records)

        print tabulate(sorted(records[1:]), records[0])


def root_sync(args, l, config):
    """Sync with the remote. For more options, use library sync
    """
    from requests.exceptions import ConnectionError

    all_remote_names = [ r.short_name for r in l.remotes ]

    if args.all:
        remotes = all_remote_names
    else:
        remotes = args.refs

    prt("Sync with {} remotes or bundles ".format(len(remotes)))

    if not remotes:
        return

    for ref in remotes:
        l.commit()

        try:
            if ref in all_remote_names: # It's a remote name
                l.sync_remote(l.remote(ref))

            else: # It's a bundle reference
                l.checkin_remote_bundle(ref)

        except NotFoundError as e:
            warn(e)
            continue
        except ConnectionError as e:
            warn(e)
            continue


def root_search(args, l, rc):
    from six import text_type

    if args.reindex:
        def tick(message):
            """Writes a tick to the stdout, without a space or newline."""
            import sys

            sys.stdout.write("\033[K{}\r".format(message))
            sys.stdout.flush()

        l.search.index_library_datasets(tick)
        return

    terms = ' '.join(text_type(t) for t in args.terms)
    print(terms)

    results = l.search.search(terms)

    for r in results:
        print(r.vid, r.bundle.metadata.about.title)
        for p in r.partition_records:
            if p:
                print('    ', p.vid, p.vname)



def root_remove(args, l, rc):

    for term in args.term:

        try:
            b = l.bundle(term)
        except NotFoundError:
            warn("Didn't find bundle for reference '{}'".format(term))
            return

        fqname = b.identity.fqname

        l.remove(b)

        prt('Removed {}'.format(fqname))


def root_import(args, l, rc):
    import yaml
    from fs.opener import fsopendir
    from . import err
    from ambry.orm.exc import NotFoundError
    import os

    for term in args.term:

        fs = fsopendir(term)

        for f in fs.walkfiles(wildcard='bundle.yaml'):

            prt("Visiting {}".format(f))
            config = yaml.load(fs.getcontents(f))

            if not config:
                err("Failed to get a valid bundle configuration from '{}'".format(f))

            bid = config['identity']['id']

            try:
                b = l.bundle(bid)

                if not args.force:
                    prt('Skipping existing  bundle: {}'.format(b.identity.fqname))
                    continue

            except NotFoundError:
                b = None

            if not b:
                b = l.new_from_bundle_config(config)
                prt('Loading bundle: {}'.format(b.identity.fqname))
            else:
                prt('Loading existing bundle: {}'.format(b.identity.fqname))

            b.set_file_system(source_url=os.path.dirname(fs.getsyspath(f)))

            b.sync_in()

            if args.detach:
                b.set_file_system(source_url=None)

