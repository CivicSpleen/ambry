"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import os
from ..cli import prt, fatal, warn,  _print_info #@UnresolvedImport
from ambry.util import Progressor

# If the devel module exists, this is a development system.
try: from ambry.support.devel import *
except ImportError as e: from ambry.support.production import *

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
    sp.add_argument('-w','--watch',  default=False,action="store_true",  help='Check periodically for new files.')
    sp.add_argument('-f','--force',  default=False,action="store_true",  help='Push all files')

    sp = asp.add_parser('files', help='Print out files in the library')
    sp.set_defaults(subcommand='files')
    sp.add_argument('-a','--all',  default='all',action="store_const", const='all', dest='file_state',  help='Print all files')
    sp.add_argument('-n','--new',  default=False,action="store_const", const='new',  dest='file_state', help='Print new files')
    sp.add_argument('-p','--pushed',  default=False,action="store_const", const='pushed', dest='file_state',  help='Print pushed files')
    sp.add_argument('-u','--pulled',  default=False,action="store_const", const='pulled', dest='file_state',  help='Print pulled files')
    sp.add_argument('-s','--synced',  default=False,action="store_const", const='synced', dest='file_state',  help='Print synced source packages')

    sp = asp.add_parser('new', help='Create a new library')
    sp.set_defaults(subcommand='new')

    sp = asp.add_parser('drop', help='Delete all of the tables in the library')
    sp.set_defaults(subcommand='drop')

    sp = asp.add_parser('clean', help='Remove all entries from the library database')
    sp.set_defaults(subcommand='clean')

    sp = asp.add_parser('purge', help='Remove all entries from the library database and delete all files')
    sp.set_defaults(subcommand='purge')

    sp = asp.add_parser('sync', help='Synchronize the local directory, upstream and remote with the library')
    sp.set_defaults(subcommand='sync')
    sp.add_argument('-C', '--clean', default=False, action="store_true", help='Clean before syncing. Will clean only the locations that are also synced')

    sp.add_argument('-a', '--all', default=False, action="store_true", help='Sync everything')
    sp.add_argument('-l', '--library', default=False, action="store_true", help='Sync the library')
    sp.add_argument('-r', '--remote', default=False, action="store_true", help='Sync the remote')
    sp.add_argument('-s', '--source', default=False, action="store_true", help='Sync the source')
    sp.add_argument('-j', '--json', default=False, action="store_true", help='Cache JSON versions of library objects')
    sp.add_argument('-w', '--warehouses', default=False, action="store_true", help='Re-synchronize warehouses')
    sp.add_argument('-F', '--bundle-list', help='File of bundle VIDs. Sync only VIDs listed in this file')


    sp = asp.add_parser('info', help='Display information about the library')
    sp.set_defaults(subcommand='info')

    sp = asp.add_parser('get', help='Search for the argument as a bundle or partition name or id. Possible download the file from the remote library')
    sp.set_defaults(subcommand='get')
    sp.add_argument('term', type=str,help='Query term')
    sp.add_argument('-p', '--partitions', default=False, action="store_true", help='Also get all of the partitions. ')
    sp.add_argument('-f','--force',  default=False, action="store_true",  help='Force retrieving from the remote')

    sp = asp.add_parser('search',help='Search the full-text index')
    sp.set_defaults(subcommand='search')
    sp.add_argument('term', type=str, nargs=argparse.REMAINDER, help='Query term')
    sp.add_argument('-l', '--list', default=False, action="store_true", help='List documents instead of search')
    sp.add_argument('-d', '--datasets', default=False, action="store_true", help='Search only the dataset index')
    sp.add_argument('-i', '--identifiers', default=False, action="store_true", help='Search only the identifiers index')
    sp.add_argument('-p', '--partitions', default=False, action="store_true", help='Search only the partitions index')
    sp.add_argument('-R', '--reindex', default=False, action="store_true",
                    help='Generate documentation files and index the full-text search')

    sp = asp.add_parser('open',
                        help='Open a bundle or partition file with sqlite3')
    sp.set_defaults(subcommand='open')
    sp.add_argument('term', type=str, help='Query term')
    sp.add_argument('-f', '--force', default=False, action="store_true", help='Force retrieving from the remote')

    sp = asp.add_parser('remove', help='Delete a file from all local caches and the local library')
    sp.set_defaults(subcommand='remove')
    sp.add_argument('-a', '--all', default=False, action="store_true", help='Remove all records')
    sp.add_argument('-b', '--bundle', default=False, action="store_true", help='Remove the dataset and partition records')
    sp.add_argument('-l', '--library', default=False, action="store_true", help='Remove the library file record and library files')
    sp.add_argument('-r', '--remote', default=False, action="store_true", help='Remove the remote record')
    sp.add_argument('-s', '--source', default=False, action="store_true", help='Remove the source record')
    sp.add_argument('terms', type=str, nargs=argparse.REMAINDER, help='Name or ID of the bundle or partition to remove')


    sp = asp.add_parser('schema', help='Dump the schema for a bundle')
    sp.set_defaults(subcommand='schema')
    sp.add_argument('term', type=str,help='Query term')
    sp.add_argument('-p','--pretty',  default=False, action="store_true",  help='pretty, formatted output')
    group = sp.add_mutually_exclusive_group()
    group.add_argument('-y', '--yaml',  default='csv', dest='format',  action='store_const', const='yaml')
    group.add_argument('-j', '--json',  default='csv', dest='format',  action='store_const', const='json')
    group.add_argument('-c', '--csv',  default='csv', dest='format',  action='store_const', const='csv')

    sp = asp.add_parser('doc', help='Start the documentation server')
    sp.set_defaults(subcommand='doc')

    sp.add_argument('-c', '--clean', default=False, action="store_true",
                    help='When used with --reindex, delete the index and old files first. ')
    sp.add_argument('-d', '--debug', default=False, action="store_true",
                    help='Debug mode ')
    sp.add_argument('-p', '--port', help='Run on a sepecific port, rather than pick a random one')

    whsp = asp.add_parser('config', help='Configure varibles')
    whsp.set_defaults(subcommand='config')
    whsp.add_argument('term', type=str, nargs='?', help='Var=Value')

    if IN_DEVELOPMENT:
        sp = asp.add_parser('test', help='Run development test code')
        sp.set_defaults(subcommand='test')
        sp.add_argument('terms', type=str, nargs=argparse.REMAINDER,
                        help='Name or ID of the bundle or partition to remove')


def library_command(args, rc):
    from  ..library import new_library
    from . import global_logger

    l = new_library(rc.library(args.library_name))

    l.logger = global_logger

    globals()['library_'+args.subcommand](args, l,rc)


def library_init(args, l, config):

    l.database.create()

def library_backup(args, l, config):

    import tempfile

    if args.file:
        backup_file = args.file
        is_temp = False
    else:
        tfn = tempfile.NamedTemporaryFile(delete=False)
        tfn.close()
    
        backup_file = tfn.name+".db"
        is_temp = True

    if args.date:
        from datetime import datetime
        date = datetime.now().strftime('%Y%m%dT%H%M')
        parts = backup_file.split('.')
        if len(parts) >= 2:
            backup_file = '.'.join(parts[:-1]+[date]+parts[-1:])
        else:
            backup_file = backup_file + '.' + date

    prt('{}: Starting backup', backup_file)

    l.database.dump(backup_file)

    if args.cache:
        dest_dir = l.cache.put(backup_file,'_/{}'.format(os.path.basename(backup_file)))
        is_temp = True
    else:
        dest_dir = backup_file

    if is_temp:
        os.remove(backup_file)

        
    prt("{}: Backup complete", dest_dir)


def library_drop(args, l, config):   

    prt("Drop tables")
    l.database.enable_delete = True
    l.database.drop()

def library_clean(args, l, config):

    prt("Clean tables")
    l.database.clean()
        
def library_purge(args, l, config):

    prt("Purge library")
    l.purge()

def library_remove(args, l, config):
    from ..dbexceptions import NotFoundError

    cache_keys = set()
    refs = set()

    def remove_by_ident(ident):

        try:

            if ident.partition:
                cache_keys.add(ident.partition.cache_key)
                refs.add(ident.partition.vid)

            else:  # The reference is to a bundle, so we have to delete everything
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

        if (args.source or args.all ) and ref.startswith('d') :
            prt("Remove source record {}".format(ref))
            l.files.query.ref(ref).type(l.files.TYPE.SOURCE).delete()

        l.database.commit()


def library_info(args, l, config, list_all=False):    

    prt("Library Info")
    prt("Name:      {}",args.library_name)
    prt("Database:  {}",l.database.dsn)
    prt("Cache:     {}",l.cache)
    prt("Doc Cache: {}", l.doc_cache.cache)
    prt("Whs Cache: {}", l.warehouse_cache)
    prt("Remotes:   {}", ', '.join([ str(r) for r in l.remotes]) if l.remotes else '')

    
def library_push(args, l, config):
    from ..orm import Dataset
    import time
    from functools import partial

    if args.force:
        files = [(f.ref, f.type_) for f in l.files.query.installed.all]
    else:

        files = [(f.ref, f.type_) for f in l.files.query.installed.state('new').all]

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

        prt("-- Pushing to {}", l.remotes)
        start = time.clock()
        for ref, t in files:

            if t not in (Dataset.LOCATION.LIBRARY, Dataset.LOCATION.PARTITION):
                continue

            try:
                what, start, end, size = l.push(ref, cb=partial(push_cb, rate))
            except Exception as e:
                prt("Failed: {}", e)
                raise


            if what == 'pushed':
                total_time += end-start
                total_size += size

                if total_time > 0:
                    rate = int(float(total_size) / float(total_time) / 1024.0)
                else:
                    rate = 0

    # Update the list file. This file is required for use with HTTP access, since you can't get
    # a list otherwise.
    for remote in l.remotes:
        prt("  {}".format(remote.repo_id))

        remote.store_list()


def library_files(args, l, config):

    from ..identity import LocationRef

    files_ = l.files.query.state(args.file_state).type((LocationRef.LOCATION.LIBRARY,LocationRef.LOCATION.PARTITION)).all

    if len(files_):
        prt("-- Display {} files",args.file_state)
        for f in files_:
            prt("{0:14s} {1:4s} {2:6s} {3:20s} {4}",f.ref,f.state,f.type_, f.group, f.path)


def library_schema(args, l, config):
    from ambry.bundle import DbBundle


    # This will fetch the data, but the return values aren't quite right
    r = l.get(args.term, cb=Progressor().progress)


    abs_path = os.path.join(l.cache.cache_dir, r.identity.cache_key)
    b = DbBundle(abs_path)

    if args.format == 'csv':
        b.schema.as_csv()
    elif args.format == 'json':
        import json
        s = b.schema.as_struct()
        if args.pretty:
            print(json.dumps(s, sort_keys=True,indent=4, separators=(',', ': ')))
        else:
            print(json.dumps(s))
    elif args.format == 'yaml': 
        import yaml 
        s = b.schema.as_struct()
        if args.pretty:
            print(yaml.dump(s,indent=4, default_flow_style=False))
        else:
            print(yaml.dump(s))
    else:
        raise Exception("Unknown format" )    
  
def library_get(args, l, config):

    ident = l.resolve(args.term)

    if not ident:
        fatal("Could not resolve term {} ", args.term)

    # This will fetch the data, but the return values aren't quite right
    prt("get: {}".format(ident.vname))
    b = l.get(args.term, force=args.force, cb=Progressor('Download {}'.format(args.term)).progress)

    if not b:
        fatal("Download failed: {}", args.term)

    ident = b.identity

    if b.partition:
        ident.add_partition(b.partition.identity)

    elif b.partitions:
        for p in b.partitions:
            prt("get: {}".format(p.identity.vname))
            bp = l.get(p.identity.vid)

        b.partition = None

    _print_info(l, ident)

    return b


def library_open(args, l, config):
    # This will fetch the data, but the return values aren't quite right

    r = library_get(args, l, config)

    if r:
        if r.partition:
            abs_path = os.path.join(l.cache.cache_dir, r.partition.identity.cache_key)
        else:
            abs_path = os.path.join(l.cache.cache_dir, r.identity.cache_key)

        os.execlp('sqlite3', 'sqlite3', abs_path)

def library_search(args, l, config):
    # This will fetch the data, but the return values aren't quite right

    term = ' '.join(args.term)

    if args.reindex:

        print 'Updating the identifier'

        #sources = ['census.gov-index-counties', 'census.gov-index-places', 'census.gov-index-states']
        sources = ['census.gov-index-counties',  'census.gov-index-states']

        records = []

        def make_record(identifier, type, name):
            return dict(
                identifier=identifier,
                type=type,
                name=name
            )

        for s in sources:
            p = l.get(s).partition
            type = p.table.name

            for row in p.rows:

                if 'name' in row:
                    name = row.name
                    if type == 'states':
                        name += " State"

                    records.append(make_record(row.gvid, type, name))

                if 'stusab' in row:
                    records.append(make_record(row.gvid, type, row.stusab.upper()))

        s = l.search

        l.search.index_identifiers(records)

        print "Reindexing docs"
        l.search.index_datasets()

        return


    if args.identifiers:

        if args.list:
            for x in l.search.identifiers:
                print x

        else:
            for score, gvid, name in l.search.search_identifiers(term):
                print "{:2.2f} {:9s} {}".format(score, gvid, name)

    elif args.datasets:
        if args.list:
            for x in l.search.datasets:
                ds = l.dataset(x)
                print x, ds.name, ds.data.get('title')

        else:

            print "search for ", term

            for x in l.search.search_datasets(term):
                ds = l.dataset(x)
                print x, ds.name, ds.data.get('title')

    elif args.partitions:

        if args.list:
            for x in l.search.partitions:
                p = l.partition(x)
                print p.vid, p.vname
        else:

            from ..identity import ObjectNumber
            from collections import defaultdict

            bundles = defaultdict(set)

            for x in l.search.search_partitions(term):
                bvid = ObjectNumber.parse(x).as_dataset

                bundles[str(bvid)].add(x)

            for bvid, pvids in bundles.items():

                ds = l.dataset(str(bvid))

                print ds.vid, ds.name, len(pvids), ds.data.get('title')


def library_sync(args, l, config):
    '''Synchronize the remotes and the upstream to a local library
    database'''

    all = args.all or not (args.library or args.remote or args.source or args.json or args.warehouses )

    vids=None

    if args.bundle_list:
        with open(args.bundle_list) as f:
            vids  = set()
            for line in f:
                if line[0] != '#':
                    vid,_ = line.split(None,1)
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

    if (args.json or all):
        l.logger.info("==== Sync Cached JSON")
        l.sync_doc_json(clean=args.clean)

    if (args.warehouses):
        l.logger.info("==== Sync warehouses")
        l.sync_warehouses()

def library_doc(args, l, rc):

    from ambry.ui import app, configure_application, setup_logging
    import ambry.ui.views as views

    import logging
    from logging import FileHandler
    import webbrowser
    import socket

    port = args.port if args.port else 8085

    cache_dir = l._doc_cache.path('',missing_ok=True)

    config = configure_application(dict(port = port))

    file_handler = FileHandler(os.path.join(cache_dir, "web.log"))
    file_handler.setLevel(logging.WARNING)
    app.logger.addHandler(file_handler)

    print 'Serving documentation for cache: ', cache_dir


    if not args.debug:
        # Don't open the browser on debugging, or it will re-open on every application reload
        webbrowser.open("http://localhost:{}/".format(port))

    app.run(host=config['host'], port=int(port), debug=args.debug)

def library_config(args, l, config):
    from ..dbexceptions import ConfigurationError

    if args.term:

        parts = args.term.split('=',1)

        var = parts.pop(0);
        val = parts.pop(0) if parts else None

        if not var in l.configurable:
            raise ConfigurationError("Value {} is not configurable. Must be one of: {}".format(args.var, l.configurable))

        if val:
            setattr(l, var, val)
        elif not val and '=' in args.term:
            setattr(l, var, None)
        else:
            print getattr(l, var)

    else:
        for e in l.database.get_config_group('library'):
            print e

def library_unknown(args, l, config):
    fatal("Unknown subcommand")
    fatal(args)

def library_test(args, l, config):
    import time

    term = 'Alabama city New Hope'

    for t in range(len(term)):

        for i, (score, gvid, name) in enumerate(l.search.search_identifiers(term[:t])):

            if i == 0:
                print chr(27) + "[2J" + chr(27) + "[H"

            print score, gvid, name
