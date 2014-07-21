"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from . import prt, fatal, err, warn,  _print_info, _print_bundle_list

class Logger(object):
    def __init__(self,  prefix, lr):
        self.prefix = prefix
        self.lr = lr

    def progress(self,type_,name, n, message=None):
        self.lr("{}: {} {}: {}".format(self.prefix,type_, name, n))

    def copy(self, o,t):
        self.lr("{} {}".format(o,t))

    def info(self,message):
        prt("{}: {}",self.prefix, message)

    def log(self,message):
        prt("{}: {}",self.prefix, message)

    def error(self,message):
        err("{}: {}",self.prefix, message)

    def fatal(self,message):
        fatal("{}: {}",self.prefix, message)

    def warn(self, message):
        warn("{}: {}", self.prefix, message)


def warehouse_command(args, rc):
    from ambry.warehouse import new_warehouse
    from ..library import new_library
    from . import global_logger
    from ambry.warehouse import database_config

    l = new_library(rc.library(args.library_name))

    l.logger = global_logger

    if args.subcommand != 'install':

        if args.database:
            config = database_config(args.database)
        else:
            config = rc.warehouse(args.name)

        w = new_warehouse(config, l)

        globals()['warehouse_'+args.subcommand](args, w,rc)
    else:
        globals()['warehouse_' + args.subcommand](args, l, rc)

def warehouse_parser(cmd):
   
    whr_p = cmd.add_parser('warehouse', help='Manage a warehouse')
    whr_p.set_defaults(command='warehouse')
    whp = whr_p.add_subparsers(title='warehouse commands', help='command help')

    whr_p.add_argument('-d', '--database', help='Path or connection url for a database. ')
    whr_p.add_argument('-n','--name',  default='default',  help='Select a different name for the warehouse')

    whsp = whp.add_parser('install', help='Install a bundle or partition to a warehouse')
    whsp.set_defaults(subcommand='install')
    whsp.add_argument('-t', '--test', default=False, action='store_true', help='Load only 100 records per table')
    whsp.add_argument('-f', '--force', default=False, action='store_true', help='Force re-creation of tables and fiels that already exist')
    whsp.add_argument('-g', '--gen-doc', default=False, action='store_true', help='After installation, generate documentation')
    whsp.add_argument('-n', '--no_install', default=False, action='store_true', help="Don't install")
    whsp.add_argument('-b', '--base-dir', default=None,help='Base directory for installed. Defaults to <ambry-install>/warehouse')
    whsp.add_argument('-d', '--dir', default=None,
                      help='Publication directory for file installs, if different from the work-dir.')
    whsp.add_argument('-p', '--publish', nargs='?', default=False, help='Publication url')
    whsp.add_argument('term', type=str,help='Name of bundle or partition')

    whsp = whp.add_parser('remove', help='Remove a bundle or partition from a warehouse')
    whsp.set_defaults(subcommand='remove')
    whsp.add_argument('term', type=str,help='Name of bundle or partition')
    
    whsp = whp.add_parser('sync', help='Syncronize database to a list of names')
    whsp.set_defaults(subcommand='sync')
    whsp.add_argument('file', type=str,help='Name of file containing a list of names')
    
    whsp = whp.add_parser('connect', help='Test connection to a warehouse')
    whsp.set_defaults(subcommand='connect')

    whsp = whp.add_parser('info', help='Configuration information')
    whsp.set_defaults(subcommand='info')   
 
    whsp = whp.add_parser('drop', help='Drop the warehouse database')
    whsp.set_defaults(subcommand='drop')   
 
    whsp = whp.add_parser('create', help='Create required tables')
    whsp.set_defaults(subcommand='create')   
 
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

def warehouse_install(args, l ,config):
    from os import getcwd, chdir

    last_wd = getcwd()

    try:
        _warehouse_install(args, l ,config)
    finally:
        chdir(last_wd)

def _warehouse_install(args, l ,config):
    from ambry.dbexceptions import ConfigurationError
    from ambry.warehouse.manifest import new_manifest
    from ..util import get_logger

    logger = get_logger('warehouse',template="WH %(levelname)s: %(message)s")

    try:
        d =  config.filesystem('warehouse')

        base_dir = args.base_dir if args.base_dir else d['dir']

    except ConfigurationError:
        base_dir = args.base_dir

    if not base_dir:
        raise ConfigurationError("Must specify -b for base director,  or set filesystem.warehouse in configuration")

    m = new_manifest(args.term, logger=logger, library=l, base_dir = base_dir, force = args.force)

    if not args.no_install:
        m.install()

        logger.info('Installed:')
        for fn in m.file_installs:
            logger.info('    {}'.format(fn))


    if args.publish != False:
        m.publish(config, args.publish)

    if args.gen_doc:
        print m.html_doc()

def warehouse_remove(args, w,config):
    from functools import partial
    from ambry.util import init_log_rate

    w.logger = Logger('Warehouse Remove',init_log_rate(prt,N=2000))
    
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
