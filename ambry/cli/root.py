"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..cli import prt, warn, fatal, _find, _print_bundle_list, _print_bundle_entry


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
    sp.add_argument('-a', '--all', default=False, action="store_true", help='List everything')
    sp.add_argument('-l', '--library', default=False, action="store_const", const = lr.LIBRARY, help='List only the library')
    sp.add_argument('-r', '--remote', default=False, action="store_const", const = lr.REMOTE, help='List only the remote')
    sp.add_argument('-s', '--source', default=False, action="store_const", const = lr.SOURCE, help='List only the source')
    sp.add_argument('term', nargs = '?', type=str, help='Name or ID of the bundle or partition')

    sp = cmd.add_parser('info', help='Information about a bundle or partition')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='info')
    sp.add_argument('-p', '--partitions', default=False, action="store_true", help="Show partitions")
    sp.add_argument('term',  type=str, nargs = '?', help='Name or ID of the bundle or partition')

    sp = cmd.add_parser('doc', help='Open a browser displaying documentation')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='doc')
    sp.add_argument('term',  type=str, nargs = '?', help='Name or ID of the bundle or partition')

    sp = cmd.add_parser('meta', help='Dump the metadata for a bundle')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='meta')
    sp.add_argument('term',  type=str, nargs = '?', help='Name or ID of the bundle or partition')
    sp.add_argument('-k', '--key', default=False, type=str, help='Return the value of a specific key')
    group = sp.add_mutually_exclusive_group()
    group.add_argument('-y', '--yaml', default=False, action='store_true', help='Output yaml')
    group.add_argument('-j', '--json', default=False, action='store_true', help='Output json')
    group.add_argument('-r', '--rows', default=False, action='store_true', help='Output key/value pair rows')


    sp = cmd.add_parser('find', prefix_chars='-+',
                        help='Search for the argument as a bundle or partition name or id')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='find')

    sp.add_argument('-P', '--plain', default=False, action='store_true',
                    help='Plain output; just print the bundle path, with no logging decorations')
    sp.add_argument('-F', '--fields', type=str,
                    help="Specify fields to use. One of: 'locations', 'vid', 'status', 'name'")
    group = sp.add_mutually_exclusive_group()
    group.add_argument('+s', '--source', default=False, action='store_true',
                       help='Find source bundle (Source is downloaded, not just referenced)')
    group.add_argument('-s', '--not-source', default=False, action='store_true',
                       help='Find source bundle that is referenced but not downloaded')
    group.add_argument('+b', '--built', default=False, action='store_true',
                       help='Find bundles that have been built')
    group.add_argument('-b', '--not-built', default=False, action='store_true',
                       help='Find bundles that have not been built')
    group.add_argument('-c', '--commit', default=False, action='store_true',
                       help='Find bundles that need to be committed')
    group.add_argument('-p', '--push', default=False, action='store_true',
                       help='Find bundles that need to be pushed')
    group.add_argument('-i', '--init', default=False, action='store_true',
                       help='Find bundles that need to be initialized')
    group.add_argument('-a', '--all', default=False, action='store_true',
                       help='List all bundles, from root or sub dir')

    sp.add_argument('terms', type=str, nargs=argparse.REMAINDER, help='Query commands to find packages with. ')


def root_command(args, rc):
    from ..library import new_library
    from . import global_logger

    l = new_library(rc.library(args.library_name))
    l.logger = global_logger

    st = l.source

    globals()['root_' + args.subcommand](args, l, st, rc)


def root_list(args, l, st, rc):
    from ..cli import load_bundle, _print_bundle_list
    from ..orm import Dataset

    if args.plain:
        fields = ['vid']

    elif args.fields:
        fields = args.fields.split(',')

    else:
        fields = ['locations',  'vid',  'vname']

    locations = filter(bool, [args.library, args.remote, args.source])

    key = lambda ident : ident.vname

    if 'pcount' in fields:
        with_partitions = True
    else:
        with_partitions = False

    idents = sorted(l.list(with_partitions=with_partitions).values(), key=key)

    if args.term:
        idents = [ ident for ident in idents if args.term in ident.fqname ]

    if locations:
        idents = [ ident for ident in idents if ident.locations.has(locations)]


    _print_bundle_list(idents,
                       fields=fields,
                       show_partitions=args.partitions)

def root_info(args, l, st, rc):
    from ..cli import load_bundle, _print_info
    from ..orm import Dataset
    import ambry

    if not args.term:
        print "Version:  {}".format(ambry._meta.__version__)
        print "Root dir: {}".format(rc.filesystem('root')['dir'])

        if l.source:
            print "Source :  {}".format(l.source.base_dir)

        print "Configs:  {}".format(rc.dict['loaded'])

        return

    ident = l.resolve(args.term)

    if not ident:
        fatal("Failed to find record for: {}", args.term)
        return

    b = l.get(ident.vid)

    if b and not ident.partition:
        for p in b.partitions.all:
            ident.add_partition(p.identity)

    _print_info(l, ident, list_partitions=args.partitions)

def root_meta(args, l, st, rc):
    from ..cli import load_bundle, _print_info
    from ..orm import Dataset
    import ambry

    ident = l.resolve(args.term)

    if not ident:
        fatal("Failed to find record for: {}", args.term)
        return

    b = l.get(ident.vid)

    meta = b.metadata

    if not args.key:
        # Return all of the rows
        if args.yaml:
            print meta.yaml

        elif args.json:
            print meta.json

        elif args.key:
            for row in meta.rows:
                print '.'.join([e for e in row[0] if e])+'='+str(row[1] if row[1] else '')
        else:
            print meta.yaml

    else:

        v = None
        from ..util import AttrDict
        o = AttrDict()
        count = 0

        for row in meta.rows:
            k = '.'.join([e for e in row[0] if e])
            if k.startswith(args.key):
                v = row[1]
                o.unflatten_row(row[0], row[1])
                count +=1

        if count == 1:
            print v

        else:
            if args.yaml:
                print o.dump()

            elif args.json:
                print o.json()

            elif args.rows:
                for row in o.flatten():
                    print '.'.join([e for e in row[0] if e]) + '=' + str(row[1] if row[1] else '')

            else:
                print o.dump()


def root_find(args, l, st, rc):
    from ..source.repository.git import GitRepository
    from ..library.files import Files
    from ..identity import Identity
    from ..bundle.bundle import BuildBundle

    if args.plain:
        fields = ['vid']

    elif  args.fields:
        fields = args.fields.split(',')

    else:
        fields = []

    key = lambda ident: ident.vname

    if args.terms:

        show_partitions = any(['partition' in term for term in args.terms])

        identities = sorted(_find(args, l, rc).values(), key=key)

        _print_bundle_list(identities,fields=fields, show_partitions=show_partitions)

    else:

        idents =sorted(l.list(key='fqname').values(), key=key)

        s = l.source

        for ident in idents:
            try:
                bundle = s.resolve_build_bundle(ident.vid)
            except Exception as e:
                warn("Failed to load for {}: {}".format(ident, e.message))

            if bundle:
                repo = GitRepository(None, bundle.bundle_dir)
                try:
                    repo.bundle_dir = bundle.bundle_dir
                except Exception as e:
                    warn("Failed to instantiate for {}: {}".format(ident, e.message))
                    continue
            else:
                repo = None

            show = [False]

            def toggle(show, cond):
                if not show[0] and cond:
                    show[:] = [True]

            toggle(show, args.all)

            toggle(show, args.commit and repo and repo.needs_commit())
            toggle(show, args.push and repo and repo.needs_push())
            toggle(show, args.init_descriptor and repo and repo.needs_init())

            toggle(show, args.source and ident.locations.is_in(Files.TYPE.SOURCE) )
            toggle(show, args.not_source and not ident.locations.is_in(Files.TYPE.SOURCE))

            toggle(show, args.built and bundle and bundle.is_built)
            toggle(show, args.not_built and bundle and not bundle.is_built)

            if show[0]:

                if args.plain:
                    prt('{}'.format(ident.fqname))
                else:
                    _print_bundle_entry(ident, show_partitions=False, prtf=prt, fields=fields)

def root_doc(args, l, st, rc):
    from ambry.cache import new_cache
    import webbrowser

    try:
        ident = l.resolve(args.term)
    except ValueError:
        fatal("Can't parse ref: {} ".format(args.term))

    if not ident:
        fatal("Failed to find record for: {}", args.term)
        return

    b = l.get(ident.vid)

    if b.partition:
        ck = b.partition.identity + '.html'
        doc = b.partition.html_doc()
    else:
        ck = b.identity.path + '.html'
        doc = b.html_doc()


    cache_config = rc.filesystem('documentation')

    cache = new_cache(cache_config)

    s = cache.put_stream(ck,{'Content-Type':'text/html'})
    s.write(doc)
    s.close()

    path = cache.path(ck)

    prt("Opening file: {} ".format(path))

    webbrowser.open_new("file://"+path)

