"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..cli import prt, fatal, warn, _print_info #@UnresolvedImport
import os
from ambry.util import Progressor

def library_parser(cmd):

    import argparse

    #
    # Library Command
    #
    lib_p = cmd.add_parser('library', help='Manage a library')
    lib_p.set_defaults(command='library')

    group = lib_p.add_mutually_exclusive_group()
    group.add_argument('-s', '--server', default=False, dest='is_server', action='store_true',
                       help='Select the server configuration')
    group.add_argument('-c', '--client', default=False, dest='is_server', action='store_false',
                       help='Select the client configuration')

    asp = lib_p.add_subparsers(title='library commands', help='command help')

    sp = asp.add_parser('push', help='Push new library files')
    sp.set_defaults(subcommand='push')
    sp.add_argument('-w','--watch',  default=False,action="store_true",  help='Check periodically for new files.')
    sp.add_argument('-f','--force',  default=False,action="store_true",  help='Push all files')
    
    sp = asp.add_parser('server', help='Run the library server')
    sp.set_defaults(subcommand='server') 
    sp.add_argument('-d','--daemonize', default=False, action="store_true",   help="Run as a daemon") 
    sp.add_argument('-k','--kill', default=False, action="store_true",   help="With --daemonize, kill the running daemon process") 
    sp.add_argument('-g','--group', default=None,   help="Set group for daemon operation") 
    sp.add_argument('-u','--user', default=None,  help="Set user for daemon operation")  
    sp.add_argument('-t','--test', default=False, action="store_true",   help="Run the test version of the server")   
      
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
    sp.add_argument('-l', '--library', default=False, action="store_true", help='Sync only the library')
    sp.add_argument('-r', '--remote', default=False, action="store_true", help='Sync only the remote')
    sp.add_argument('-s', '--source', default=False, action="store_true", help='Sync only the source')


    sp = asp.add_parser('info', help='Display information about the library')
    sp.set_defaults(subcommand='info')   

    sp = asp.add_parser('get', help='Search for the argument as a bundle or partition name or id. Possible download the file from the remote library')
    sp.set_defaults(subcommand='get')   
    sp.add_argument('term', type=str,help='Query term')
    sp.add_argument('-p', '--partitions', default=False, action="store_true", help='Also get all of the partitions. ')
    sp.add_argument('-f','--force',  default=False, action="store_true",  help='Force retrieving from the remote')


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
        
def library_restore(args, l, config, *kwargs):

    if args.dir:
      
        if args.file:
            # Get the last file that fits the pattern, sorted alpha, with a date inserted
          
            import fnmatch
            
            date = '*' # Sub where the date will be  
            parts = args.file.split('.')
            if len(parts) >= 2:
                pattern = '.'.join(parts[:-1]+[date]+parts[-1:])
            else:
                import tempfile
                tfn = tempfile.NamedTemporaryFile(delete=False)
                tfn.close()
    
                backup_file = tfn.name+".db"
                pattern = backup_file + '.' + date
                
            files = sorted([ f for f in os.listdir(args.dir) if fnmatch.fnmatch(f,pattern) ])
    
        else:
            # Get the last file, by date. 
            files = sorted([ f for f in os.listdir(args.dir) ], 
                           key=lambda x: os.stat(os.path.join(args.dir,x))[8])
    
    
        backup_file = os.path.join(args.dir,files.pop())
        
    elif args.file:
        backup_file = args.file
    
    
    # Backup before restoring. 
    
    args = type('Args', (object,),{'file':'/tmp/before-restore.db','cache': True, 
                                   'date': True, 'is_server': args.is_server, 'name':args.library_name, 
                                   'subcommand': 'backup'})
    library_backup(args, l, config)
    
    prt("{}: Restoring", backup_file)
    l.clean(add_config_root=False)
    l.restore(backup_file)
   
def library_server(args, l, config):
    from ..util import daemonize

    from ambry.server.main import production_run, local_run

    def run_server(args, config):
        production_run(config.library(args.library_name))
    
    if args.daemonize:
        daemonize(run_server, args,  config)
    elif args.test:
        local_run(config.library(args.library_name))
    else:
        production_run(config.library(args.library_name))
        
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
      
def library_rebuild(args, l, config):  

    l.database.enable_delete = True
    if args.upstream:
        prt("Rebuild library from remote")
        l.remote_rebuild()
    else:
        prt("Rebuild library from local storage")
        l.rebuild()

def library_remove(args, l, config):
    from ..dbexceptions import NotFoundError

    cache_keys = set()
    refs = set()

    for name in args.terms:

        ident = l.resolve(name, location=None)

        if not ident:
            warn("Found no references to term {}".format(name))
            continue

        try:

            if ident.partition:
                cache_keys.add(ident.partition.cache_key)
                refs.add(ident.partition.vid)

            else: # The reference is to a bundle, so we have to delete everything
                cache_keys.add(ident.cache_key)
                refs.add(ident.vid)

                b = l.get(ident.vid)

                if b:
                    for p in b.partitions:
                        cache_keys.add(p.cache_key)
                        refs.add(p.vid)

        except NotFoundError:
            pass


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
    prt("Name:     {}",args.library_name)
    prt("Database: {}",l.database.dsn)
    prt("Cache:    {}",l.cache)
    prt("Remotes:  {}", ', '.join([ str(r) for r in l.remotes]) if l.remotes else '')

    
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
            prt("{} {} {}", note, md['fqname'])
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


def library_files(args, l, config):

    files_ = l.files.query.state(args.file_state).all
    if len(files_):
        prt("-- Display {} files",args.file_state)
        for f in files_:
            prt("{0:11s} {1:4s} {2}",f.ref,f.state,f.path)


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

def library_sync(args, l, config):
    '''Synchronize the remotes and the upstream to a local library
    database'''

    all = args.all or not (args.library or args.remote or args.source )

    if args.library or all:
        l.logger.info("==== Sync Library")
        l.sync_library(clean=args.clean)

    if args.remote or all:
        l.logger.info("==== Sync Remotes")
        l.sync_remotes(clean=args.clean)

    if (args.source or all) and l.source:
        l.logger.info("==== Sync Source")
        l.sync_source(clean=args.clean)

    
def library_unknown(args, l, config):
    fatal("Unknown subcommand")
    fatal(args)
