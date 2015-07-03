"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""

from ..cli import warn, fatal
from ..identity import LocationRef

default_locations = [LocationRef.LOCATION.LIBRARY,  LocationRef.LOCATION.REMOTE]


def root_parser(cmd):
    import argparse
    from ..identity import LocationRef

    lr = LocationRef.LOCATION

    sp = cmd.add_parser('list', help='List bundles and partitions')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='list')
    sp.add_argument('-P', '--plain', default=False, action="store_true", help="Print only vids")
    sp.add_argument('-F', '--fields', type=str,
                    help="Specify fields to use. One of: 'locations', 'vid', 'status', 'vname', 'sname', 'fqname")
    sp.add_argument('-p', '--partitions', default=False, action="store_true", help="Show partitions")
    sp.add_argument('-t', '--tables', default=False, action="store_true", help="Show tables")
    sp.add_argument('-a', '--all', default=False, action="store_true", help='List everything')
    sp.add_argument('-l', '--library', default=False, action="store_const", const=lr.LIBRARY,
                    help='List only the library')
    sp.add_argument('-r', '--remote', default=False, action="store_const", const=lr.REMOTE, help='List only the remote')
    sp.add_argument('-s', '--source', default=False, action="store_const", const=lr.SOURCE, help='List only the source')
    sp.add_argument('-w', '--warehouse', default=False, action="store_const", const='warehouse', help='List warehouses')
    sp.add_argument('-c', '--collection', default=False, action="store_const", const='collection',
                    help='List collections')
    sp.add_argument('term', nargs='?', type=str, help='Name or ID of the bundle or partition')

    sp = cmd.add_parser('info', help='Information about a bundle or partition')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='info')
    group = sp.add_mutually_exclusive_group()
    group.add_argument('-l', '--library', default=False, action="store_true", help='Information about the library')
    group.add_argument('-r', '--remote', default=False, action="store_true", help='Information about the remotes')
    group.add_argument('-b', '--bundle', default=False, action="store_true", help='Information about a bundle')
    sp.add_argument('term', type=str, nargs='?', help='Name or ID of the bundle or partition')

    sp = cmd.add_parser('meta', help='Dump the metadata for a bundle')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='meta')
    sp.add_argument('term', type=str, nargs='?', help='Name or ID of the bundle or partition')
    sp.add_argument('-k', '--key', default=False, type=str, help='Return the value of a specific key')
    group = sp.add_mutually_exclusive_group()
    group.add_argument('-y', '--yaml', default=False, action='store_true', help='Output yaml')
    group.add_argument('-j', '--json', default=False, action='store_true', help='Output json')
    group.add_argument('-r', '--rows', default=False, action='store_true', help='Output key/value pair rows')
    sp.add_argument('terms', type=str, nargs=argparse.REMAINDER, help='Query commands to find packages with. ')

    sp = cmd.add_parser('doc', help='Start the documentation server')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='doc')

    sp.add_argument('-c', '--clean', default=False, action="store_true",
                    help='When used with --reindex, delete the index and old files first. ')
    sp.add_argument('-d', '--debug', default=False, action="store_true", help='Debug mode ')
    sp.add_argument('-p', '--port', help='Run on a sepecific port, rather than pick a random one')

    sp = cmd.add_parser('search', help='Search the full-text index')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='search')
    sp.add_argument('term', type=str, nargs=argparse.REMAINDER, help='Query term')
    sp.add_argument('-l', '--list', default=False, action="store_true", help='List documents instead of search')
    sp.add_argument('-i', '--identifiers', default=False, action="store_true", help='Search only the identifiers index')
    sp.add_argument('-R', '--reindex', default=False, action="store_true",
                    help='Generate documentation files and index the full-text search')
    sp.add_argument('-d', '--document', default=False, action="store_true",
                    help='Return the search document for an object id')
    sp.add_argument('-u', '--unparsed', default=False, action="store_true",
                    help='Pass the search term to the engine without parsing')

    sp = cmd.add_parser('sync', help='Sync with the remotes')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='sync')


def root_command(args, rc):
    from ..library import new_library
    from . import global_logger
    from ambry.orm.exc import DatabaseError

    try:
        l = new_library(rc)
        l.logger = global_logger
    except DatabaseError as e:
        warn("No library: {}".format(e))
        l = None
    except Exception as e:
        warn("Failed to instantiate library: {}".format(e))
        l = None

    globals()['root_' + args.subcommand](args, l, rc)


def root_list(args, l, rc):


    ##
    # Listing warehouses and collections is different

    if args.collection:
        #root_list_collections(args,l,rc)
        pass

    if args.warehouse:
        #root_list_collections(args, l, rc)
        pass
    ##
    # The remainder are for listing bundles and partitions.

    if args.tables:
        # root_list_tables(args, l, rc)
        pass

    if args.plain:
        fields = ['vid']

    elif args.fields:
        fields = args.fields.split(',')

    else:
        fields = ['locations', 'vid', 'vname']

        if args.source:
            fields += ['status']

    root_list_datasets(args, l, rc)

def root_list_datasets(args, l, rc):
    from tabulate import tabulate

    header = ['vid','vname','state','description']
    records = []

    for b in l.bundles:
        records.append(b.field_row(header))

    print tabulate(records, headers = header)


def root_info(args, l, rc):
    from ..cli import _print_info, err, fatal, prt
    from ..dbexceptions import ConfigurationError
    from ambry.orm.exc import NotFoundError
    import ambry

    locations = filter(bool, [args.library, args.remote])

    if not locations:
        locations = default_locations

    if not args.term:
        prt("Version:   {}, {}",ambry._meta.__version__, rc.environment.category)
        prt("Root dir:  {}",rc.filesystem('root'))

        try:
            if l.source:
                prt("Source :   {}",l.source.base_dir)
        except (ConfigurationError, AttributeError):
            prt("Source :   No source directory")

        prt("Configs:   {}",rc.dict['loaded'])
        prt("Library:   {}", l.database.dsn)
        prt("Remotes:   {}", ', '.join([str(r) for r in l.remotes]) if l.remotes else '')

        return

    if not l:
        fatal("No library, probably due to a configuration error")

    ident = l.resolve(args.term, location=locations)

    if not ident:
        fatal("Failed to find record for: {}", args.term)
        return

    try:
        b = l.get(ident.vid)

        if not ident.partition:
            for p in b.partitions.all:
                ident.add_partition(p.identity)

    except NotFoundError:
        # fatal("Could not find bundle file for '{}'".format(ident.path))
        pass

    # Always list partitions if there are 10 or fewer. If more, defer to the partitions flag
    list_partitions = args.partitions if len(ident.partitions) > 10 else True

    _print_info(l, ident, list_partitions=list_partitions)


def root_sync(args, l, config):
    """Sync with the remote. For more options, use library sync
    """

    l.logger.info("==== Sync Remotes")
    l.sync_remotes()


def root_search(args, l, config):
    # This will fetch the data, but the return values aren't quite right
    from ambry.orm.exc import NotFoundError

    term = ' '.join(args.term)

    if args.reindex:

        print 'Updating the identifier'

        def tick(message):
            """Writes a tick to the stdout, without a space or newline."""
            import sys

            sys.stdout.write("\033[K{}\r".format(message))
            sys.stdout.flush()

        records = []

        source = 'civicknowledge.com-terms-geoterms'

        p = l.get(source).partition

        for row in p.rows:
            records.append(dict(identifier=row['gvid'], type=row['type'], name=row['name']))

        l.search.index_identifiers(records)

        print "Reindexing docs"
        l.search.index_datasets(tick_f=tick)

        return

    if args.document:
        import json

        b = l.get(term)

        if b.partition:
            print json.dumps(l.search.partition_doc(b.partition), indent=4)
        else:
            print json.dumps(l.search.dataset_doc(b), indent=4)

        return

    elif args.identifiers:

        if args.list:
            for x in l.search.identifiers:
                print x

        else:
            for score, gvid, rtype, name in l.search.search_identifiers(term, limit=30):
                print "{:6.2f} {:9s} {} {}".format(score, gvid, rtype, name)

    else:

        if args.list:

            for x in l.search.datasets:
                print x
                ds = l.dataset(x)
                print x, ds.name, ds.data.get('title')

        else:
            if args.unparsed:
                parsed = term
            else:
                parsed = l.search.make_query_from_terms(term)

            print "search for ", parsed

            datasets = l.search.search_datasets(parsed)

            for result in sorted(datasets.values(), key=lambda e: e.score, reverse=True):
                ds = l.dataset(result.vid)
                print result.score, result.vid, ds.name, ds.data.get('title'), list(result.partitions)[:5]




def root_doc(args, l, rc):

    from ambry.ui import app, app_config
    import ambry.ui.views as views
    import os

    import logging
    from logging import FileHandler
    import webbrowser

    cache_dir = l._doc_cache.path('', missing_ok=True)

    app_config['port'] = args.port if args.port else 8085

    file_handler = FileHandler(os.path.join(cache_dir, "web.log"))
    file_handler.setLevel(logging.WARNING)
    app.logger.addHandler(file_handler)

    print 'Serving documentation for cache: ', cache_dir

    if not args.debug:
        # Don't open the browser on debugging, or it will re-open on every
        # application reload
        webbrowser.open("http://localhost:{}/".format(app_config['port']))

    app.run(host=app_config['host'], port=int(app_config['port']), debug=args.debug)

