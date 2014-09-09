"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from . import prt, fatal, err, warn,  _print_info, _print_bundle_list


def warehouse_command(args, rc):
    from ambry.warehouse import new_warehouse
    from ..library import new_library
    from . import global_logger
    from ambry.warehouse import database_config
    from ..dbexceptions import ConfigurationError

    l = new_library(rc.library(args.library_name))

    l.logger = global_logger

    try:
        if args.database:
            config = database_config(args.database)
        else:
            config = rc.warehouse(args.name)

        w = new_warehouse(config, l, logger = global_logger)

        if not w.exists():
            w.create()

    except ConfigurationError:
        # There was no database configured, so let the called function provide additional information
        from functools import partial
        w = partial(create_warehouse,rc, global_logger, l)

    globals()['warehouse_'+args.subcommand](args, w,rc)

def create_warehouse(rc, logger, library, manifest=None):
    from ..dbexceptions import ConfigurationError
    from ambry.warehouse import database_config
    from ambry.warehouse import new_warehouse
    import os.path

    if manifest:
        database = manifest.database

        if not database:
            raise ConfigurationError('Warehouse database must either be specified on the command line or in the manifest')

        try:

            base_dir = os.path.join(rc.filesystem('warehouse')['dir'], manifest.uid)
        except ConfigurationError:
            base_dir = ''

        config = database_config(database, base_dir = base_dir)


        w = new_warehouse(config, library, logger=logger)

        if not w.exists():
            try:
                w.create()
            except:
                w.logger.error("Failed to create warehouse at: {}".format(w.database.dsn))
                raise

        return w

    raise ConfigurationError('No warehouse database specified')



def warehouse_parser(cmd):
   
    whr_p = cmd.add_parser('warehouse', help='Manage a warehouse')
    whr_p.set_defaults(command='warehouse')
    whp = whr_p.add_subparsers(title='warehouse commands', help='command help')

    whr_p.add_argument('-d', '--database', help='Path or connection url for a database. ')
    whr_p.add_argument('-n','--name',  default='default',  help='Select a different name for the warehouse')

    whsp = whp.add_parser('install', help='Install a bundle or partition to a warehouse')
    whsp.set_defaults(subcommand='install')
    whsp.add_argument('-C', '--clean', default=False, action='store_true', help='Remove all data from the database before installing')
    whsp.add_argument('-n', '--name-only', default=False, action='store_true', help='The only output will be the DSN of the warehouse')
    whsp.add_argument('-R', '--reset-config', default=False, action='store_true',
                      help='Reset all of the values, like title and about, that come from the manifest'),
    whsp.add_argument('-F', '--force', default=False, action='store_true',
                      help='Force re-creation of files that already exist')

    # For extract, when called from install
    group = whsp.add_mutually_exclusive_group()
    group.add_argument('-l', '--local', dest='dest',  action='store_const', const='local')
    group.add_argument('-r', '--remote', dest='dest', action='store_const', const='remote')
    group.add_argument('-c', '--cache' )
    whsp.add_argument('-D', '--dir', default = '', help='Set directory, instead of configured Warehouse filesystem dir, for relative paths')


    whsp.add_argument('term', type=str,help='Name of bundle or partition')

    whsp = whp.add_parser('extract', help='Extract files or documentation to a cache')
    whsp.set_defaults(subcommand='extract')
    whsp.add_argument('-f', '--files-only', default=False, action='store_true', help='Only extract the extract files')
    whsp.add_argument('-d', '--doc-only', default=False, action='store_true', help='Only extract the documentation files')
    group = whsp.add_mutually_exclusive_group()
    group.add_argument('-l', '--local', dest='dest',  action='store_const', const='local', default='local')
    group.add_argument('-r', '--remote', dest='dest', action='store_const', const='remote')
    group.add_argument('-c', '--cache' )
    whsp.add_argument('-F', '--force', default=False, action='store_true',
                      help='Force re-creation of files that already exist')
    whsp.add_argument('-D', '--dir', default = '', help='Set directory, instead of configured Warehouse filesystem dir, for relative paths')

    whsp = whp.add_parser('config', help='Configure varibles')
    whsp.set_defaults(subcommand='config')
    whsp.add_argument('-v', '--var', help="Name of the variable. One of'local','remote','title','about' ")
    whsp.add_argument('term', type=str, nargs = '?', help='Value of the variable')

    whsp = whp.add_parser('doc', help='Build documentation')
    whsp.set_defaults(subcommand='doc')
    group = whsp.add_mutually_exclusive_group()
    group.add_argument('-l', '--local', dest='dest',  action='store_const', const='local', default='local')
    group.add_argument('-r', '--remote', dest='dest', action='store_const', const='remote')
    group.add_argument('-c', '--cache' )
    whsp.add_argument('-D', '--dir', default='',
                      help='Set directory, instead of configured Warehouse filesystem dir, for relative paths')
    whsp.add_argument('-F', '--force', default=False, action='store_true',
                      help='Force re-creation of files that already exist')

    whsp = whp.add_parser('remove', help='Remove a bundle or partition from a warehouse')
    whsp.set_defaults(subcommand='remove')
    whsp.add_argument('term', type=str,help='Name of bundle or partition')

    whsp = whp.add_parser('connect', help='Test connection to a warehouse')
    whsp.set_defaults(subcommand='connect')

    whsp = whp.add_parser('info', help='Configuration information')
    whsp.set_defaults(subcommand='info')   
 
    whsp = whp.add_parser('drop', help='Drop the warehouse database')
    whsp.set_defaults(subcommand='drop')   
 
    whsp = whp.add_parser('create', help='Create required tables')
    whsp.set_defaults(subcommand='create')

    whsp = whp.add_parser('index', help='Create an Index webpage for a warehouse')
    whsp.set_defaults(subcommand='index')
    whsp.add_argument('term', type=str, help="Cache's root URL must have a 'meta' subdirectory")

    whsp = whp.add_parser('users', help='Create and configure warehouse users')
    whsp.set_defaults(subcommand='users')  
    group = whsp.add_mutually_exclusive_group()
    group.add_argument('-L', '--list', dest='action',  action='store_const', const='list')
    group.add_argument('-a', '--add' )
    group.add_argument('-d', '--delete')
       
    whsp = whp.add_parser('list', help='List the datasets inthe warehouse')
    whsp.set_defaults(subcommand='list')   
    whsp.add_argument('term', type=str, nargs='?', help='Name of bundle, to list partitions')

def warehouse_info(args, w,config):
    
    prt("Warehouse Info")
    prt("Name:     {}",args.name)
    prt("Class:    {}",w.__class__)
    prt("Database: {}",w.database.dsn)
    prt("WLibrary: {}",w.wlibrary.database.dsn)
    prt("ELibrary: {}",w.elibrary.database.dsn)

def warehouse_remove(args, w,config):
    from functools import partial
    from ambry.util import init_log_rate

    #w.logger = Logger('Warehouse Remove',init_log_rate(prt,N=2000))
    
    w.remove(args.term )
      
def warehouse_drop(args, w,config):

    w.delete()
 
def warehouse_create(args, w,config):
    
    w.database.enable_delete = True
    try:
        w.library.clean()
        w.drop()
    except:
        pass # Can't clean or drop if doesn't exist
    
    w.create()
    w.library.database.create()
    
def warehouse_users(args, w,config):

    if args.action == 'list' or ( not bool(args.delete) and not bool(args.add)):
        for name, values in w.users().items():
            prt("{} id={} super={}".format(name, values['id'], values['superuser']))
    elif bool(args.delete):
        w.drop_user(args.delete)   
    elif bool(args.add):
        w.create_user(args.add)   

    #w.configure_default_users()
    
def warehouse_list(args, w, config):    

    l = w.library

    if not args.term:

        _print_bundle_list(w.list(),show_partitions=False)
            
    else:
        raise NotImplementedError()
        d, p = l.get_ref(args.term)
                
        _print_info(l,d,p, list_partitions=True)

def warehouse_install(args, w ,config):
    from ambry.warehouse.manifest import Manifest

    m = Manifest(args.term)

    if callable(w):
        w = w(manifest=m)

    if args.clean:
        w.clean()
        w.create()

    if args.name_only:
        from ambry.warehouse import NullLogger
        w.logger = NullLogger()

    w.logger.info("Installing to {}".format(w.database.dsn))

    w.install_manifest(m, reset = args.reset_config)

    w.logger.info("Installed to {}".format(w.database.dsn))

    if args.name_only:
        print w.database.dsn

    if args.dest or args.cache:

        warehouse_extract(args, w, config)

def get_cache(w, args, rc):
    from ..dbexceptions import ConfigurationError
    from ambry.cache import new_cache, parse_cache_string
    import os.path

    if args.cache:
        c_string = args.cache

    elif args.dest == 'local':

        c_string = w.local_cache

        if not c_string:
            raise ConfigurationError(
                "For extracts, must set an extract location, either in the manifest or the warehouse")

        # Join will return c_string if c_string is an absolute path
        c_string = os.path.join(rc.filesystem('warehouse')['dir'], c_string)


    elif args.dest == 'remote':
        c_string = w.remote_cache

    config = parse_cache_string(c_string, root_dir=args.dir)

    if not config:
        raise ConfigurationError("Failed to parse cache spec: '{}'".format(c_string))

    if args.dir and config['type'] == 'file' and not os.path.isabs(config['dir']):
        config['dir'] = os.path.join(args.dir, config['dir'])

    if config['type'] == 'file' and not os.path.exists(config['dir']):
        os.makedirs(config['dir'])

    cache = new_cache(config, run_config=rc)

    return cache

def warehouse_extract(args, w, config):

    if callable(w):
        w = w()

    cache = get_cache(w, args, config)

    w.logger.info("Extracting to: {}".format(cache))

    extracts = w.extract(cache, force=args.force)

    for extract in extracts:
        print extract

    print cache.path('index.html', missing_ok=True, public_url = True)

def warehouse_doc(args, w, config):

    from ..text import Renderer
    import os.path

    if callable(w):
        w = w()

    cache = get_cache(w, args, config)

    cache.prefix = os.path.join(cache.prefix, 'doc')

    w.logger.info("Extracting to: {}".format(cache))

    r = Renderer(cache, warehouse = w)

    path, extracts = r.write_library_doc()


    print path


def warehouse_config(args, w, config):
    from ..dbexceptions import ConfigurationError

    if callable(w):
        w = w()

    if not args.var in w.configurable:
        raise ConfigurationError("Value {} is not configurable. Must be one of: {}".format(args.var, w.configurable))

    if args.term:
        setattr(w, args.var, args.term)

    print getattr(w, args.var)

