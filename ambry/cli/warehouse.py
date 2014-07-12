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


def database_config(db):
    import urlparse

    parts = urlparse.urlparse(db)

    if parts.scheme == 'sqlite':
        config = dict(service='sqlite', database=dict(dbname=parts.path, driver='sqlite'))
    elif parts.scheme == 'spatialite':
        config = dict(service='spatialite', database=dict(dbname=parts.path, driver='sqlite'))
    elif parts.scheme == 'postgres':
        config = dict(service='postgres',
                      database=dict(driver='postgres',
                                    server=parts.hostname,
                                    username=parts.username,
                                    password=parts.password,
                                    dbname=parts.path.strip('/')
                      ))
    elif parts.scheme == 'postgis':
        config = dict(service='postgis',
                      database=dict(driver='postgis',
                                    server=parts.hostname,
                                    username=parts.username,
                                    password=parts.password,
                                    dbname=parts.path.strip('/')
                      ))
    else:
        raise ValueError("Unknown database connection scheme: {}".format(parts.scheme))

    return config


def warehouse_command(args, rc):
    from ambry.warehouse import new_warehouse
    from ..library import new_library
    from . import global_logger

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
    whsp.add_argument('-w', '--work-dir', default=None,help='Working directory for file installs and temporaty databases.')
    whsp.add_argument('-d', '--dir', default=None,
                      help='Publication directory for file installs, if different from the work-dir.')
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
    from ..library import new_library
    import os.path

    from ambry.util import init_log_rate
    from ..dbexceptions import NotFoundError, ConfigurationError
    from ambry.warehouse.extractors import extract
    from ambry.cache import new_cache
    from ambry.warehouse import new_warehouse

    if os.path.isfile(args.term) or args.term.startswith('http'): # Assume it is a Manifest file.
        from ..warehouse.manifest import Manifest

        m  = Manifest(args.term)
        destination = m.destination

    else:
        m = None
        destination = None

    pub_dir = args.dir
    work_dir = args.work_dir

    if m.count_sections('extract') > 0:
        work_dir = pub_dir = args.work_dir if args.work_dir else m.work_dir

        pub_dir = args.dir if args.dir else work_dir

        if not pub_dir:
            raise ConfigurationError("Manifest has extracts. Must specify either a work dir or publication dir")

    if work_dir:

        if not os.path.isdir(work_dir):
            os.makedirs(work_dir)

        os.chdir(work_dir)

        prt("Working directory: {}".format(work_dir))



    if args.database:
        config = database_config(args.database)

    elif destination:
        config = database_config(destination)

    else:
        config = config.warehouse(args.name)

    w = new_warehouse(config, l)

    if not w.exists():
        w.create()

    w.logger = Logger('Warehouse Install', init_log_rate(prt, N=2000))

    w.test = args.test

    ##
    ## If it isn't a manifest, install a single partition and return
    ##
    if not m:
        try:
            w.install(args.term)
        except NotFoundError:
            err("Partition {} not found in external library".format(args.term))

        return

    ##
    ## Finally! Now we can iterate over the section and do the installation.
    ##

    if not m.uid:
        import uuid
        fatal("Manifest does not have a UID. Add this line to the file:\n\nUID: {}\n".format(uuid.uuid4()))


    for line in sorted(m.sections.keys()):
        section = m.sections[line]

        tag = section['tag']

        prt("== Processing manifest section {} at line {}",section['tag'], section['line_number'])

        if tag == 'partitions':
            for pd in section['content']:
                try:
                    tables = pd['tables']

                    if pd['where'] and len(pd['tables']) == 1:
                        tables = (pd['tables'][0], pd['where'])

                    w.install(pd['partition'], tables)
                except NotFoundError:
                    err("Partition {} not found in external library".format(pd['partition']))

        elif tag == 'sql':
            sql = section['content']

            if w.database.driver in sql:
                w.run_sql(sql[w.database.driver])

        elif tag == 'index':
            c = section['content']
            w.create_index(c['name'], c['table'], c['columns'])

        elif tag == 'mview':
            w.install_material_view(section['args'], section['content'])

        elif tag == 'view':
            w.install_view(section['args'], section['content'])

        elif tag == 'extract':
            print section

    return



    for table, format, dest in extracts:
        prt("Extracting {} to {} as {}".format(table, format, dest))
        cache = new_cache(pub_dir)
        abs_path = extract(w.database, table, format, cache, dest)
        prt("Extracted to {}".format(abs_path))


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
