"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""

import os
from ..cli import prt, err, fatal, warn, _print_info  # @UnresolvedImport
from ambry.util import Progressor


def library_parser(cmd):
    import argparse

    #
    # Library Command
    #
    lib_p = cmd.add_parser('library', help='Manage a library')
    lib_p.set_defaults(command='library')

    asp = lib_p.add_subparsers(title='library commands', help='command help')

    sp = asp.add_parser('push', help='Push new library files')
    sp.set_defaults(subcommand='push')
    sp.add_argument('-w', '--watch', default=False, action="store_true", help='Check periodically for new files.')
    sp.add_argument('-f', '--force', default=False, action="store_true", help='Push all files')
    sp.add_argument('-n', '--dry-run', default=False, action="store_true",
                    help="Dry run, don't actually send the files.")

    sp = asp.add_parser('files', help='Print out files in the library')
    sp.set_defaults(subcommand='files')
    sp.add_argument('-a', '--all', default='all', action="store_const", const='all', dest='file_state',
                    help='Print all files')
    sp.add_argument('-n', '--new', default=False, action="store_const", const='new', dest='file_state',
                    help='Print new files')
    sp.add_argument('-p', '--pushed', default=False, action="store_const", const='pushed', dest='file_state',
                    help='Print pushed files')
    sp.add_argument('-u', '--pulled', default=False, action="store_const", const='pulled', dest='file_state',
                    help='Print pulled files')
    sp.add_argument('-s', '--synced', default=False, action="store_const", const='synced', dest='file_state',
                    help='Print synced source packages')

    sp = asp.add_parser('new', help='Create a new library')
    sp.set_defaults(subcommand='new')

    sp = asp.add_parser('drop', help='Delete all of the tables in the library')
    sp.set_defaults(subcommand='drop')

    sp = asp.add_parser('clean', help='Remove all entries from the library database')
    sp.set_defaults(subcommand='clean')


    sp = asp.add_parser('sync', help='Synchronize the local directory, upstream and remote with the library')
    sp.set_defaults(subcommand='sync')
    sp.add_argument('-C', '--clean', default=False, action="store_true",
                    help='Clean before syncing. Will clean only the locations that are also synced')

    sp.add_argument('-a', '--all', default=False, action="store_true", help='Sync everything')
    sp.add_argument('-l', '--library', default=False, action="store_true", help='Sync the library')
    sp.add_argument('-r', '--remote', default=False, action="store_true", help='Sync the remote')
    sp.add_argument('-s', '--source', default=False, action="store_true", help='Sync the source')
    sp.add_argument('-j', '--json', default=False, action="store_true", help='Cache JSON versions of library objects')
    sp.add_argument('-w', '--warehouses', default=False, action="store_true", help='Re-synchronize warehouses')
    sp.add_argument('-F', '--bundle-list', help='File of bundle VIDs. Sync only VIDs listed in this file')


    sp = asp.add_parser('get', help='Search for the argument as a bundle or partition name or id. '
                                    'Possible download the file from the remote library')
    sp.set_defaults(subcommand='get')
    sp.add_argument('term', type=str, help='Query term')
    sp.add_argument('-p', '--partitions', default=False, action="store_true", help='Also get all of the partitions. ')
    sp.add_argument('-f', '--force', default=False, action="store_true", help='Force retrieving from the remote')

    sp = asp.add_parser('open', help='Open a bundle or partition file with sqlite3')
    sp.set_defaults(subcommand='open')
    sp.add_argument('term', type=str, help='Query term')
    sp.add_argument('-f', '--force', default=False, action="store_true", help='Force retrieving from the remote')

    sp = asp.add_parser('remove', help='Delete a file from all local caches and the local library')
    sp.set_defaults(subcommand='remove')
    sp.add_argument('-a', '--all', default=False, action="store_true", help='Remove all records')
    sp.add_argument('-b', '--bundle', default=False, action="store_true",
                    help='Remove the dataset and partition records')
    sp.add_argument('-l', '--library', default=False, action="store_true",
                    help='Remove the library file record and library files')
    sp.add_argument('-r', '--remote', default=False, action="store_true", help='Remove the remote record')
    sp.add_argument('-s', '--source', default=False, action="store_true", help='Remove the source record')
    sp.add_argument('terms', type=str, nargs=argparse.REMAINDER, help='Name or ID of the bundle or partition to remove')

    whsp = asp.add_parser('config', help='Configure varibles')
    whsp.set_defaults(subcommand='config')
    whsp.add_argument('term', type=str, nargs='?', help='Var=Value')

def library_command(args, rc):
    from ..library import new_library
    from . import global_logger

    l = new_library(rc.library(args.library_name))

    l.logger = global_logger

    globals()['library_' + args.subcommand](args, l, rc)


def library_init(args, l, config):
    l.database.create()


def library_drop(args, l, config):
    prt("Drop tables")
    l.database.enable_delete = True
    l.database.drop()
    warn("Drop tables for %s" % l.database.dbname)


def library_clean(args, l, config):
    prt("Clean tables")
    l.database.clean()

def library_remove(args, l, config):
    from ambry.orm.exc import NotFoundError

    cache_keys = set()
    refs = set()

    def remove_by_ident(ident):

        try:

            if ident.partition:
                cache_keys.add(ident.partition.cache_key)
                refs.add(ident.partition.vid)

            # The reference is to a bundle, so we have to delete everything
            else:
                cache_keys.add(ident.cache_key)
                refs.add(ident.vid)

                b = l.get(ident.vid)

                if b:
                    for p in b.partitions:
                        cache_keys.add(p.cache_key)
                        refs.add(p.vid)

        except NotFoundError:
            pass

    for name in args.terms:

        ident = l.resolve(name, location=None)

        if ident:
            remove_by_ident(ident)
            continue

        if name.startswith('s'):
            l.remove_store(name)

        elif name.startswith('m'):
            l.remove_manifest(name)

        else:
            warn("Found no references to term {}".format(name))

    if args.library or args.all:
        for ck in cache_keys:
            l.cache.remove(ck, propagate=True)
            prt("Remove file {}".format(ck))

    for ref in refs:

        if args.bundle or args.all:
            prt("Remove bundle record {}".format(ref))
            if ref.startswith('d'):
                l.database.remove_dataset(ref)
            if ref.startswith('p'):
                l.database.remove_partition_record(ref)

            # We also need to delete everything in this case; no point in having
            # a file record if there there is no bundle or partition.
            l.files.query.ref(ref).delete()

        if args.library or args.all:
            prt("Remove library record {}".format(ref))
            if ref.startswith('d'):
                l.files.query.ref(ref).type(l.files.TYPE.BUNDLE).delete()
            if ref.startswith('p'):
                l.files.query.ref(ref).type(l.files.TYPE.PARTITION).delete()

        if args.remote or args.all:
            prt("Remove remote record {}".format(ref))
            l.files.query.ref(ref).type(l.files.TYPE.REMOTE).delete()
            l.files.query.ref(ref).type(l.files.TYPE.REMOTEPARTITION).delete()

        if (args.source or args.all) and ref.startswith('d'):
            prt("Remove source record {}".format(ref))
            l.files.query.ref(ref).type(l.files.TYPE.SOURCE).delete()

        l.database.commit()



def library_push(args, l, config):
    from ..orm import Dataset
    import time
    from functools import partial
    from boto.exception import S3ResponseError
    from collections import defaultdict

    if args.force:
        files = [(f.ref, f.type_) for f in l.files.query.installed.all]
    else:

        files = [(f.ref, f.type_) for f in l.files.query.installed.state('new').all]

    remote_errors = defaultdict(int)

    def push_cb(rate, note, md, t):
        if note == 'Has':
            prt("{} {}", note, md['fqname'])
        elif note == 'Pushing':
            prt("{} {}  {} KB/s ", note, md['fqname'], rate)
        elif note == 'Pushed':
            pass
        else:
            prt("{} {}", note, md['fqname'])

    if len(files):

        total_time = 0.0
        total_size = 0.0
        rate = 0

        # start = time.clock()
        for ref, t in files:

            if t not in (Dataset.LOCATION.LIBRARY, Dataset.LOCATION.PARTITION):
                continue

            bp = l.resolve(ref)

            if not bp:
                err("Failed to resolve file ref to bundle {} ".format(ref))
                continue

            b = l.bundle(bp.vid)

            remote_name = b.metadata.about.access

            if remote_name not in l.remotes:
                err("Can't push {} (bundle: '{}' ); no remote named '{}' ".format(ref, bp.vname, remote_name))
                continue

            if remote_errors[remote_name] > 4:
                err("Too many errors on remote '{}', skipping ".format(remote_name))
                continue

            remote = l.remotes[remote_name]

            try:
                what, start, end, size = l.push(remote, ref, cb=partial(push_cb, rate), dry_run=args.dry_run)
            except S3ResponseError:
                err("Failed to push to remote '{}' ".format(remote_name))
                remote_errors[remote_name] += 1
                continue

            except Exception as e:
                prt("Failed: {}", e)
                raise

            if what == 'pushed':
                total_time += end - start
                total_size += size

                if total_time > 0:
                    rate = int(float(total_size) / float(total_time) / 1024.0)
                else:
                    rate = 0

    # Update the list file. This file is required for use with HTTP access, since you can't get
    # a list otherwise.
    for remote_name, remote in l.remotes.items():
        prt("  {}".format(remote.repo_id))

        if not args.dry_run:
            remote.store_list()


def library_get(args, l, config):
    ident = l.resolve(args.term)

    if not ident:
        fatal("Could not resolve term {} ", args.term)

    # This will fetch the data, but the return values aren't quite right
    prt("get: {}".format(ident.vname))
    b = l.get(
        args.term,
        force=args.force,
        cb=Progressor(
            'Download {}'.format(
                args.term)).progress)

    if not b:
        fatal("Download failed: {}", args.term)

    ident = b.identity

    if b.partition:
        ident.add_partition(b.partition.identity)

    elif b.partitions:
        for p in b.partitions:
            prt("get: {}".format(p.identity.vname))
            l.get(p.identity.vid)

        b.partition = None

    _print_info(l, ident)

    return b



def library_sync(args, l, config):
    """Synchronize the remotes and the upstream to a local library database."""
    l.logger.info('args: %s' % args)

    all = args.all or not (args.library or args.remote or args.source or args.json or args.warehouses)

    vids = None

    if args.bundle_list:
        with open(args.bundle_list) as f:
            vids = set()
            for line in f:
                if line[0] != '#':
                    vid, _ = line.split(None, 1)
                    vids.add(vid)

    if args.library or all:
        l.logger.info("==== Sync Library")
        l.sync_library(clean=args.clean)

    if args.remote or all:
        l.logger.info("==== Sync Remotes")
        l.sync_remotes(clean=args.clean, vids=vids)

    if (args.source or all) and l.source:
        l.logger.info("==== Sync Source")
        l.sync_source(clean=args.clean)

    if args.json or all:
        l.logger.info("==== Sync Cached JSON")
        l.sync_doc_json(clean=args.clean)

    if args.warehouses:
        l.logger.info("==== Sync warehouses")
        l.sync_warehouses()




def library_unknown(args, l, config):
    fatal("Unknown subcommand")
    fatal(args)



