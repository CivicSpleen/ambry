"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..cli import prt,err, _find, plain_prt, _print_bundle_list, _print_bundle_entry


def root_parser(cmd):
    import argparse

    sp = cmd.add_parser('list', help='List bundles and partitions')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='list')
    sp.add_argument('-P', '--plain', default=False, action="store_true", help="Print only vids")
    sp.add_argument('-F', '--fields', type=str,
                    help="Specify fields to use. One of: 'locations', 'vid', 'status', 'vname', 'sname', 'fqname")
    sp.add_argument('-p', '--partitions', default=False, action="store_true", help="Show partitions")
    group = sp.add_mutually_exclusive_group()
    group.add_argument('-a', '--all', default=False, action="store_true", help='List everything')
    group.add_argument('-l', '--library', default=False, action="store_true", help='List only the library')
    group.add_argument('-r', '--remote', default=False, action="store_true", help='List only the remote')
    group.add_argument('-u', '--upstream', default=False, action="store_true", help='List only the upstream')
    group.add_argument('-g', '--srepo', default=False, action="store_true", help='List only the srepo')
    group.add_argument('-s', '--source', default=False, action="store_true", help='List only the source')
    sp.add_argument('term', nargs = '?', type=str, help='Name or ID of the bundle or partition')

    sp = cmd.add_parser('info', help='Information about a bundle or partition')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='info')
    sp.add_argument('-p', '--partitions', default=False, action="store_true", help="Show partitions")
    sp.add_argument('term',  type=str, help='Name or ID of the bundle or partition')

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
    from . import logger


    l = new_library(rc.library(args.library_name))
    l.logger = logger

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
        fields = ['locations', 'vid', 'status', 'vname']

    locations = []

    if args.library:
        locations.append(Dataset.LOCATION.LIBRARY)

    if args.remote:
        locations.append(Dataset.LOCATION.REMOTE)

    if args.upstream:
        locations.append(Dataset.LOCATION.UPSTREAM)

    if args.source:
        locations.append(Dataset.LOCATION.SOURCE)

    if args.srepo:
        locations.append(Dataset.LOCATION.SREPO)

    if not locations:
        locations = None # list everything.

    if args.partitions:
        locations = [Dataset.LOCATION.LIBRARY]


    key = lambda ident : ident.vname

    idents = sorted(l.list(locations=locations, key='fqname').values(), key=key)

    _print_bundle_list(idents,
                       fields=fields,
                       show_partitions=args.partitions)

def root_info(args, l, st, rc):
    from ..cli import load_bundle, _print_info
    from ..orm import Dataset

    ident = l.resolve(args.term, location=None)

    if not ident:
        err("Failed to find record for: {}", args.term)
        return



    if ident.locations.is_in(Dataset.LOCATION.LIBRARY):
        b = l.get(ident.vid)

        if not ident.partition:
            for p in b.partitions.all:
                ident.add_partition(p.identity)

    elif ident.locations.is_in(Dataset.LOCATION.REMOTE):
        from ..client.rest import RemoteLibrary

        f = l.files.query.type(Dataset.LOCATION.REMOTE).ref(ident.vid).one
        rl = RemoteLibrary(f.group)

        ds = rl.dataset(ident.vid)

        if not ident.partition:
            ident.partitions = ds.partitions


    _print_info(l, ident, list_partitions=args.partitions)


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

        identities = sorted(_find(args, l, rc).values(), key=key)

        _print_bundle_list(identities,fields=fields)

    else:

        idents =sorted(l.list(key='fqname').values(), key=key)

        s = l.source

        for ident in idents:
            bundle = s.resolve_build_bundle(ident.vid)

            if bundle:
                repo = GitRepository(None, bundle.bundle_dir)
                repo.bundle_dir = bundle.bundle_dir
            else:
                repo = None

            show = [False]

            def toggle(show, cond):
                if not show[0] and cond:
                    show[:] = [True]

            toggle(show, args.all)

            toggle(show, args.commit and repo and repo.needs_commit())
            toggle(show, args.push and repo and repo.needs_push())
            toggle(show, args.init and repo and repo.needs_init())

            toggle(show, args.source and ident.locations.is_in(Files.TYPE.SOURCE) )
            toggle(show, args.not_source and not ident.locations.is_in(Files.TYPE.SOURCE))

            toggle(show, args.built and bundle and bundle.is_built)
            toggle(show, args.not_built and bundle and not bundle.is_built)

            if show[0]:

                if args.plain:
                    plain_prt('{}'.format(ident.fqname))
                else:
                    _print_bundle_entry(ident, show_partitions=False, prtf=prt, fields=fields)
