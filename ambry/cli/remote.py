"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""

from ..cli import prt, _print_bundle_entry
import argparse


def remote_parser(cmd):

    lib_p = cmd.add_parser('remote', help='Access the remote library')
    lib_p.set_defaults(command='remote')
    asp = lib_p.add_subparsers(
        title='remote commands',
        help='Access the remote library')
    lib_p.add_argument(
        '-n',
        '--name',
        default='default',
        help='Select a different name for the library, '
             'from which the remote is located')

    group = lib_p.add_mutually_exclusive_group()
    group.add_argument(
        '-s',
        '--server',
        default=False,
        dest='is_server',
        action='store_true',
        help='Select the server configuration')
    group.add_argument(
        '-c',
        '--client',
        default=False,
        dest='is_server',
        action='store_false',
        help='Select the client configuration')

    sp = asp.add_parser('info', help='Display the remote configuration')
    sp.set_defaults(subcommand='info')
    sp.add_argument(
        'term',
        nargs='?',
        type=str,
        help='Name or ID of the bundle or partition to print information for')

    sp = asp.add_parser('list', help='List remote files')
    sp.set_defaults(subcommand='list')
    sp.add_argument(
        '-m',
        '--meta',
        default=False,
        action='store_true',
        help="Force fetching metadata for remotes that don't provide it while "
             "listing, like S3")
    sp.add_argument('term', nargs=argparse.REMAINDER)

    sp = asp.add_parser('fix', help='Repair brokenness')
    sp.set_defaults(subcommand='fix')
    sp.add_argument(
        '-l',
        '--stored-list',
        default=False,
        action='store_true',
        help="Re-generate the stored list")
    sp.add_argument(
        'term',
        type=str,
        nargs=argparse.REMAINDER,
        help='Query term')

    sp = asp.add_parser(
        'find',
        help='Search for the argument as a bundle or partition name or id')
    sp.set_defaults(subcommand='find')
    sp.add_argument(
        'term',
        type=str,
        nargs=argparse.REMAINDER,
        help='Query term')


def remote_command(args, rc):
    from ambry.library import new_library

    l = new_library(rc.library(args.library_name))

    globals()['remote_' + args.subcommand](args, l, rc)


def remote_info(args, l, rc):
    from ..identity import Identity
    from ambry.client.exceptions import NotFound

    if args.term:
        ip, ident = l.remote_resolver.resolve_ref_one(args.term)

        if ident:
            _print_bundle_entry(ident, prtf=prt)

    else:
        for r in l.remotes:
            print r


def remote_list(args, l, rc, return_meta=False):
    from . import _print_bundle_entry

    fields = ['locations', 'vid', 'status', 'vname']
    show_partitions = True

    if args.term:
        # List just the partitions in some data sets. This should probably be
        # combined into info.
        for term in args.term:

            ip, ident = l.remote_resolver.resolve_ref_one(term)

            print vars(ident)

            _print_bundle_entry(ident, prtf=prt, fields=fields,
                                show_partitions=show_partitions)

            return

            dsi = l.upstream.get_ref(ds)

            prt("dataset {0:11s} {1}",
                dsi['dataset']['id'],
                dsi['dataset']['name'])

            for id_, p in dsi['partitions'].items():
                vs = ''
                for v in ['time', 'space', 'table', 'grain', 'format']:
                    val = p.get(v, False)
                    if val:
                        vs += "{}={} ".format(v, val)
                prt("        {0:11s} {1:50s} {2} ", id_, p['name'], vs)

    else:

        datasets = l.upstream.list(with_metadata=return_meta)

        import pprint
        pprint.pprint(datasets)
        for id_, data in sorted(datasets.items(),
                                key=lambda x: x[1]['identity']['vname']):

            try:
                prt("{:10s} {:50s} {:s}",
                    data['identity']['vid'],
                    data['identity']['vname'],
                    id_)
            except Exception as e:
                prt("{:10s} {:50s} {:s}", '[ERROR]', '', id_)


def remote_fix(args, l, rc):
    from sqlalchemy.orm.exc import NoResultFound

    if args.stored_list:
        prt('Fix stored list on remotes')

    for remote in l.remotes:
        prt("  {}".format(remote.repo_id))

        remote.store_list()

    return

    d = {}
    for k, v in l.list().items():
        file_ = l.files.query.installed.ref(v.vid).one
        d[v.cache_key] = v.to_meta(file=file_.path)

        for pvid, pident in v.partitions.items():
            try:
                file_ = l.files.query.installed.ref(pident.vid).one
                meta = pident.to_meta(file=file_.path)
            except NoResultFound:
                meta = pident.to_meta(md5='x')

            d[pident.cache_key] = meta

    import pprint
    pprint.pprint(d)

    return
