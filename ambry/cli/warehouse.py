"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""

from six import string_types

from ambry.warehouse import database_config

from . import prt

from ..dbexceptions import ConfigurationError


def warehouse_command(args, rc):
    from ambry.warehouse import new_warehouse
    from ..library import new_library
    from . import global_logger

    l = new_library(rc.library(args.library_name))

    w = None

    l.logger = global_logger

    config = None

    #
    # Special case for installing manifests, can get the database DSN from the
    # manifest
    if not args.database and args.subcommand == 'install':
        from ..warehouse.manifest import Manifest

        m = Manifest(args.term)

        dsnt = "spatialite:///{}.db" if m.is_geo else "sqlite:///{}.db"

        name = args.name if args.name else m.uid

        dsn = dsnt.format(l.warehouse_cache.path(name, missing_ok=True))

        config = database_config(dsn)

        w = new_warehouse(config, l, logger=global_logger)

        if not w.exists():
            w.create()

    elif args.database:
        # Check if the string is the uid of a database in the library.
        s = l.store(args.database)

        if not s:
            raise ConfigurationError("Could not identitfy warehouse for term '{}'".format(args.database))

        config = database_config(s.path)

    elif args.subcommand not in ['list', 'new', 'install', 'test', 'parse']:
        raise ConfigurationError(
            "Must set the id, path or dsn of the database, either on the command line or in a manifest. ")

    if not w and config:

        w = new_warehouse(config, l, logger=global_logger)

        if not w.exists() and args.subcommand not in ['clean', 'delete']:
            raise ConfigurationError("Database {} must be created first".format(w.database.dsn))

    if args.subcommand == 'new':
        globals()['warehouse_' + args.subcommand](args, l, rc)

    else:
        globals()['warehouse_' + args.subcommand](args, w, rc)


def warehouse_parser(cmd):

    whr_p = cmd.add_parser('warehouse', help='Manage a warehouse')
    whr_p.set_defaults(command='warehouse')
    whp = whr_p.add_subparsers(title='warehouse commands', help='command help')

    whr_p.add_argument('-d', '--database', help='Uid, Path or connection url for a database. ')

    whsp = whp.add_parser('install', help='Install a bundle or partition to a warehouse')
    whsp.set_defaults(subcommand='install')
    whsp.add_argument('-n', '--name', help='Set the name of the database')
    whsp.add_argument('-c', '--clean', default=False, action='store_true',
                      help='Recreate the database before installation')
    whsp.add_argument('-D', '--dir', default='',
                      help='Set directory, instead of configured Warehouse filesystem dir, for relative paths')
    whsp.add_argument('term', type=str, help='Name of bundle or partition')

    whsp = whp.add_parser('remove', help='Remove a bundle or partition from a warehouse')
    whsp.set_defaults(subcommand='remove')
    whsp.add_argument('term', type=str, nargs='?', help='Name of bundle, partition, manifest or database')

    whsp = whp.add_parser('connect', help='Test connection to a warehouse')
    whsp.set_defaults(subcommand='connect')

    whsp = whp.add_parser('clean', help='Remove all of the contents from the warehouse')
    whsp.set_defaults(subcommand='clean')

    whsp = whp.add_parser('delete', help='Remove all of the contents from the and delete it')
    whsp.set_defaults(subcommand='delete')

    whsp = whp.add_parser('new', help='Create a new warehouse')
    whsp.set_defaults(subcommand='new')
    whsp.add_argument('-t', '--title', help='Set the title for the database')
    whsp.add_argument('-s', '--summary', help='Set the summary for the database')
    whsp.add_argument('-c', '--cache', help='Specify the cache')

    whsp.add_argument('term', type=str, nargs=1, help='The DSN of the database')

    whsp = whp.add_parser('users', help='Create and configure warehouse users')
    whsp.set_defaults(subcommand='users')
    group = whsp.add_mutually_exclusive_group()
    group.add_argument('-L', '--list', dest='action', action='store_const', const='list')
    group.add_argument('-a', '--add')
    group.add_argument('-d', '--delete')
    whsp.add_argument('-p', '--password')


def warehouse_info(args, w, config):

    prt("Warehouse Info")
    prt("UID:    {}", w.uid)
    prt("Title:    {}", w.title)
    prt("Summary:  {}", w.summary)
    prt("Class:    {}", w.__class__)
    prt("Database: {}", w.database.dsn)
    prt("WLibrary: {}", w.wlibrary.database.dsn)
    prt("ELibrary: {}", w.elibrary.database.dsn)
    prt("Cache:    {}", w.cache)


def warehouse_remove(args, w, config):

    # w.logger = Logger('Warehouse Remove',init_log_rate(prt,N=2000))

    w.remove(args.term)


def warehouse_delete(args, w, config):

    w.elibrary.remove_store(args.database)

    w.delete()


def warehouse_clean(args, w, config):

    w.clean()


def warehouse_new(args, l, config):

    if isinstance(args, string_types):
        term = args
    else:
        term = args.term[0]

    try:
        dbc = database_config(term)
    except ValueError:  # Unknow schema, usually
        dbc = config.warehouse(term)

    data = {
        'title': args.title,
        'summary': args.summary,
        'cache': args.cache
    }

    dbc.update({k: v for k, v in data.items() if v})

    w = _warehouse_new_from_dbc(dbc, l)

    prt("{}: created".format(w.uid))

    return w


def _warehouse_new_from_dbc(dbc, l):
    from ambry.warehouse import new_warehouse
    from . import global_logger

    w = new_warehouse(dbc, l, logger=global_logger)

    w.create()

    for c in w.configurable:
        if c in dbc:
            w._meta_set(c, dbc[c])

    sf = l.sync_warehouse(w)

    w.uid = sf.ref

    return w


def warehouse_users(args, w, config):

    if args.action == 'list' or (not bool(args.delete) and not bool(args.add)):
        for name, values in w.users().items():
            prt("{} id={} super={}".format(
                name, values['id'], values['superuser']))
    elif bool(args.delete):
        w.drop_user(args.delete)
    elif bool(args.add):
        w.create_user(args.add, args.password)

    # w.configure_default_users()


def warehouse_install(args, w, config):
    from ambry.warehouse.manifest import Manifest

    m = Manifest(args.term, logger=w.logger)

    if args.clean:
        w.logger.info("Cleaning before installation")
        raise Exception()
        # w.clean()
        # w.create()

    w.logger.info("Installing to {}".format(w.database.dsn))

    w.install_manifest(m)

    w.logger.info("Installed to {}, {}".format(w.uid, w.database.dsn))

    w.elibrary.sync_warehouse(w)

    w.close()


def warehouse_config(args, w, config):
    from ..dbexceptions import ConfigurationError

    if args.term:

        parts = args.term.split('=', 1)

        var = parts.pop(0)
        val = parts.pop(0) if parts else None

        if var not in w.configurable:
            raise ConfigurationError(
                "Value {} is not configurable. Must be one of: {}".format(args.var,
                                                                          w.configurable))

        if val:
            setattr(w, var, val)
        else:
            print getattr(w, var)

    else:
        for e in w.library.database.get_config_group('warehouse'):
            print e
