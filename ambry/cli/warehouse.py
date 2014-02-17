"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from . import prt, err, _print_info, _print_bundle_list

class Logger(object):
    def __init__(self,  prefix, lr):
        self.prefix = prefix
        self.lr = lr

    def progress(self,type_,name, n, message=None):
        self.lr("{}: {} {}: {}".format(self.prefix,type_, name, n))

    def info(self,message):
        prt("{}: {}",self.prefix, message)

    def log(self,message):
        prt("{}: {}",self.prefix, message)

    def error(self,message):
        err("{}: {}",self.prefix, message)

def warehouse_command(args, rc):
    from ambry.warehouse import new_warehouse
    from ..library import new_library
    from . import logger

    if args.sqlite:
        config = dict(
            service='sqlite',
            database=dict(
                dbname=args.sqlite,
                driver='sqlite'
            )
        )
    elif args.spatialite:
        config = dict(
            service='spatialite',
            database=dict(
                dbname=args.spatialite,
                driver='sqlite'
            )
        )
    else:
        config = rc.warehouse(args.name)

    l = new_library(rc.library(args.library_name))
    l.logger = logger

    w = new_warehouse(config, l)

    globals()['warehouse_'+args.subcommand](args, w,rc)


def warehouse_parser(cmd):
   
    whr_p = cmd.add_parser('warehouse', help='Manage a warehouse')
    whr_p.set_defaults(command='warehouse')
    whp = whr_p.add_subparsers(title='warehouse commands', help='command help')

    whr_p.add_argument('-s', '--sqlite',  help='Path to a Sqlite database to use for the warehouse. ')
    whr_p.add_argument('-t', '--spatialite', help='Path to a Spatialite database to use for the warehouse. ')
    whr_p.add_argument('-n','--name',  default='default',  help='Select a different name for the warehouse')

    whsp = whp.add_parser('install', help='Install a bundle or partition to a warehouse')
    whsp.set_defaults(subcommand='install')
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


def warehouse_install(args, w,config):
    from ..library import new_library
    from functools import partial
    from ambry.util import init_log_rate
    
    if not w.exists():
        w.create()

    l = new_library(config.library(args.library_name))

    w.logger = Logger('Warehouse Install',init_log_rate(prt,N=2000))
 
    w.install(args.term )

def warehouse_remove(args, w,config):
    from functools import partial
    from ambry.util import init_log_rate

    w.logger = Logger('Warehouse Remove',init_log_rate(prt,N=2000))
    
    w.remove(args.term )
      
def warehouse_drop(args, w,config):

    w.library.clean()
    w.database.enable_delete = True
    w.drop()
 
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

        _print_bundle_list(l.list().values(),show_partitions=True)
            
    else:
        raise NotImplementedError()
        d, p = l.get_ref(args.term)
                
        _print_info(l,d,p, list_partitions=True)
