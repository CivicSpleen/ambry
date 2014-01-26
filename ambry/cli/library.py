"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..cli import prt, err, Progressor, _find, _print_info #@UnresolvedImport
import os
from ..cli import  load_bundle, _print_bundle_list


def library_parser(cmd):

    import argparse

    #
    # Library Command
    #
    lib_p = cmd.add_parser('library', help='Manage a library')
    lib_p.set_defaults(command='library')

    lib_p.add_argument('-n', '--name', default='default', help='Select a different name for the library')
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
    
    sp = asp.add_parser('list', help='List datasets in the library, or partitions in dataset')
    sp.set_defaults(subcommand='list')
    sp.add_argument('term', type=str, nargs='?', help='Name of bundle, to list partitions')
    sp.add_argument('-l','--local_only',  default=False, action="store_true",   help='Dont include information from the remote' )
    
    sp = asp.add_parser('rebuild', help='Rebuild the library database from the files in the library')
    sp.set_defaults(subcommand='rebuild')
    sp.add_argument('-r','--remote',  default=False, action="store_true",   help='Rebuild from the remote')
    
    sp = asp.add_parser('backup', help='Backup the library database to the remote')
    sp.set_defaults(subcommand='backup')
    sp.add_argument('-f','--file',  default=None,   help="Name of file to back up to") 
    sp.add_argument('-d','--date',  default=False, action="store_true",   help='Append the date and time, in ISO format, to the name of the file ')
    sp.add_argument('-r','--remote',  default=False, action="store_true",   help='Also load store file to  configured remote')
    sp.add_argument('-c','--cache',  default=False, action="store_true",   help='Also load store file to  configured cache')

    sp = asp.add_parser('restore', help='Restore the library database from the remote')
    sp.set_defaults(subcommand='restore')
    sp.add_argument('-f','--file',  default=None,   help="Base pattern of file to restore from.") 
    sp.add_argument('-d','--dir',  default=None,   help="Directory where backup files are stored. Will retrieve the most recent. ") 
    sp.add_argument('-r','--remote',  default=False, action="store_true",   help='Also load file from configured remote')
    sp.add_argument('-c','--cache',  default=False, action="store_true",   help='Also load file from configured cache')
 
    sp = asp.add_parser('info', help='Display information about the library or a bundle or partition')
    sp.set_defaults(subcommand='info')   
    sp.add_argument('term',  nargs='?', type=str,help='Name or ID of the bundle or partition to print information for')
    
    sp = asp.add_parser('get', help='Search for the argument as a bundle or partition name or id. Possible download the file from the remote library')
    sp.set_defaults(subcommand='get')   
    sp.add_argument('term', type=str,help='Query term')
    sp.add_argument('-o','--open',  default=False, action="store_true",  help='Open the database with sqlite')
    sp.add_argument('-f','--force',  default=False, action="store_true",  help='Force retrieving from the remote')

    sp = asp.add_parser('remove', help='Delete a file from all local caches and the local library')
    sp.set_defaults(subcommand='remove')
    sp.add_argument('term', type=str,help='Name or ID of the bundle or partition to remove')

    sp = asp.add_parser('load', help='Search for the argument as a bundle or partition name or id. Possible download the file from the remote library')
    sp.set_defaults(subcommand='load')   
    sp.add_argument('relpath', type=str,help='Cache rel path of dataset to load from remote')


    sp = asp.add_parser('find', help='Search for the argument as a bundle or partition name or id')
    sp.set_defaults(subcommand='find')   
    sp.add_argument('term', type=str, nargs=argparse.REMAINDER,help='Query term')


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

    l = new_library(rc.library(args.name))

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
                                   'date': True, 'is_server': args.is_server, 'name':args.name, 
                                   'subcommand': 'backup'})
    library_backup(args, l, config)
    
    prt("{}: Restoring", backup_file)
    l.clean(add_config_root=False)
    l.restore(backup_file)
   
def library_server(args, l, config):
    from ..util import daemonize

    from ambry.server.main import production_run, local_run

    def run_server(args, config):
        production_run(config.library(args.name))
    
    if args.daemonize:
        daemonize(run_server, args,  config)
    elif args.test:
        local_run(config.library(args.name))
    else:
        production_run(config.library(args.name))
        
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
        
def library_list(args, l, config):    

    if not args.term:

        _print_bundle_list(l.list(key='fqname'), show_partitions=True )

    else:
        library_info(args, l, config, list_all=True)    
 
def library_remove(args, l, config):   
    
    name = args.term
    
    b = l.get(name)
    
    if not b:
        err("Didn't find")
    
    if b.partition:
        k =  b.partition.identity.cache_key
        prt("Deleting partition {}",k)
 
        l.cache.remove(k, propagate = True)
        
    else:
        
        for p in b.partitions:
            k =  p.identity.cache_key
            prt("Deleting partition {}",k)
            l.cache.remove(k, propagate = True)            
        
        k = b.identity.cache_key
        prt("Deleting bundle {}", k)
        l.remove(b)  

def library_info(args, l, config, list_all=False):    

    if args.term:

        ident = l.resolve(args.term)

        if not ident:
            err("Failed to find record for: {}", args.term)
            return 
 
        _print_info(l,ident, list_partitions=list_all)

    else:

        prt("Library Info")
        prt("Name:     {}",args.name)
        prt("Database: {}",l.database.dsn)
        prt("Cache:    {}",l.cache)
        prt("Remotes:  {}", ', '.join(l.remotes))

    
def library_push(args, l, config):

    if args.force:
        state = 'all'
    else:
        state = 'new'
    
    files_ = l.database.get_file_by_state(state)
    if len(files_):
        prt("-- Pushing to {}",l.upstream)
        for f in files_:
            
            if f.type_ not in ('partition','bundle'):
                continue
            
            prt("Pushing: {}",f.path)
            try:
                l.push(f)
            except Exception as e:
                prt("Failed: {}",e)
                raise
                
def library_files(args, l, config):

    files_ = l.database.get_file_by_state(args.file_state)
    if len(files_):
        prt("-- Display {} files",args.file_state)
        for f in files_:
            prt("{0:11s} {1:4s} {2}",f.ref,f.state,f.path)
      

def library_find(args, l, config):
    return _find(args, l, config, False)

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

    # This will fetch the data, but the return values aren't quite right
    r = l.get(args.term, force=args.force, cb=Progressor('Download {}'.format(args.term)).progress)
  
    if not r:
        prt("{}: Not found",args.term)
        return  


    _print_info(l,r.identity, r.partition.identity if r.partition else None)

    if r and args.open:
        
        if r.partition:
            abs_path = os.path.join(l.cache.cache_dir, r.partition.identity.cache_key)
        else:
            abs_path = os.path.join(l.cache.cache_dir, r.identity.cache_key)
            
        prt("\nOpening: {}\n",abs_path)

        os.execlp('sqlite3','sqlite3',abs_path )
        

def library_load(args, l, config):       

    from ..bundle import get_identity
    from ..identity import Identity
    
    
    print(Identity.parse_name(args.relpath).to_dict())
    
    return 
    
    prt("{}",l.cache.connection_info)
    prt("{}: Load relpath from cache", args.relpath)
    path = l.cache.get(args.relpath)
        
    prt("{}: Stored in local cache", path)
        
    if path:
        print(get_identity(path).name)

    
def library_unknown(args, l, config):
    err("Unknown subcommand")
    err(args)
