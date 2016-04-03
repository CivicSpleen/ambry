"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""

__all__ = ['command_name', 'make_parser', 'run_command']
command_name = 'library'

from six import iteritems
from ..cli import prt, prt_no_format, err, fatal, warn  # @UnresolvedImport
import sys


def make_parser(cmd):
    import argparse

    #
    # Library Command
    #
    lib_p = cmd.add_parser('library', help='Manage a library')
    lib_p.set_defaults(command='library')

    asp = lib_p.add_subparsers(title='library commands', help='command help')

    sp = asp.add_parser('drop', help='Delete all of the tables in the library')
    sp.set_defaults(subcommand='drop')

    sp = asp.add_parser('clean', help='Remove all entries from the library database')
    sp.set_defaults(subcommand='clean')
    sp.add_argument('-a', '--all', default=False, action='store_true', help='Sync everything')
    sp.add_argument('-l', '--library', default=False, action='store_true', help='Sync the library')
    sp.add_argument('-r', '--remote', default=False, action='store_true', help='Sync the remote')
    sp.add_argument('-s', '--source', default=False, action='store_true', help='Sync the source')
    sp.add_argument('-j', '--json', default=False, action='store_true',
                    help='Cache JSON versions of library objects')
    sp.add_argument('-w', '--warehouses', default=False, action='store_true',
                    help='Re-synchronize warehouses')
    sp.add_argument('-F', '--bundle-list',
                    help='File of bundle VIDs. Sync only VIDs listed in this file')

    sp = asp.add_parser('number', help='Return a new number from the number server')
    sp.set_defaults(subcommand='number')
    sp.add_argument('-k', '--key', default='self',
                    help="Set the number server key, or 'self' for self assignment ")

    sp = asp.add_parser('pg', help='Operations on a Postgres library database')
    sp.set_defaults(subcommand='pg')
    sp.add_argument('-p', '--processes', default=False, action='store_true', help='List all processes')
    sp.add_argument('-c', '--connect', default=False, action='store_true',
                    help='Connection and exit with return code to indicate success or failure')
    sp.add_argument('-l', '--locks', default=False, action='store_true', help='List all locks')
    sp.add_argument('-b', '--blocks', default=False, action='store_true',
                    help='List locks that are blocked or are blocking another process')

    sp = asp.add_parser('export', help='Dump a library configuration, remortes, accounts and bundles')
    sp.set_defaults(subcommand='export')
    sp.add_argument('-p', '--password', required=True, help='Encryption password')

    sp.add_argument('config_file', nargs='?', type=argparse.FileType('wb'), default = sys.stdout,
                    help='Config file to write to. If absent, will write to stdout')

    sp = asp.add_parser('import', help='Import or list library configuration, remortes, accounts and bundles')
    sp.set_defaults(subcommand='import')
    sp.add_argument('-p', '--password', required=True, help='Decryption password')
    sp.add_argument('-l', '--list', default=False, action='store_true', help='List, dont import')
    sp.add_argument('config_file', nargs='?', type=argparse.FileType('rb'), default = sys.stdin,
                    help='Config file to read from. If absent, will read from stdin')

def run_command(args, rc):
    from ..library import new_library
    from . import global_logger
    from ambry.orm.exc import NotFoundError
    from sqlalchemy.exc import OperationalError

    if args.subcommand == 'drop':
        l = None
    else:

        try:
            l = new_library(rc)
            l.logger = global_logger
            l.sync_config()
        except (NotFoundError, OperationalError) as e:
            l = None
            warn("Failed to construct library: {}".format(e))

    globals()['library_' + args.subcommand](args, l, rc)

    if l:
        l.close()

def library_drop(args, l, config):
    prt("Drop tables")
    from ambry.orm import Database
    from ambry.library import LibraryFilesystem

    fs = LibraryFilesystem(config)

    db = Database(fs.database_dsn)

    db.drop()

    #db.create()
    #db._create_path()
    #db.create_tables()

def library_clean(args, l, config):
    prt("Clean tables in {}".format(l.database.dsn))
    l.clean()
    l.sync_config()

def library_number(args, l, config):
    print(l.number(assignment_class=args.key))

def library_pg(args, l, config):
    """Report on the operation of a Postgres Library database"""
    import tabulate
    import terminaltables
    from textwrap import fill
    from ambry.util.text import getTerminalSize
    import sys

    if args.connect:
        try:
            l.database.connection.execute('SELECT * FROM pg_stat_activity;')
            sys.exit(0)
        except Exception as e:
            prt(str(e))
            sys.exit(1)

    db = l.database

    (x, y) = getTerminalSize()

    if args.processes:

        headers = None
        rows = []

        for row in db.connection.execute('SELECT pid, client_addr, application_name ass, query FROM pg_stat_activity '):
            if not headers:
                headers = row.keys()
            row = list(str(e) for e in row)

            row[3] = fill(row[3],x-50)
            rows.append(row)

        #print tabulate.tabulate(rows, headers)
        table =  terminaltables.UnixTable([headers]+rows)
        print table.table

    if args.blocks:

        headers = None
        rows = []

        q1 = """
SELECT pid, database, mode, locktype, mode, relation, tuple, virtualxid FROM pg_locks order by pid;
"""


        q2 = """
  SELECT blocked_locks.pid     AS blocked_pid,
         -- blocked_activity.usename  AS blocked_user,
         blocking_locks.pid     AS blocking_pid,
         -- blocking_activity.usename AS blocking_user,
         blocked_activity.query    AS blocked_statement,
         blocking_activity.query   AS current_statement_in_blocking_process
   FROM  pg_catalog.pg_locks         blocked_locks
    JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
    JOIN pg_catalog.pg_locks         blocking_locks
        ON blocking_locks.locktype = blocked_locks.locktype
        AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
        AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
        AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
        AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
        AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
        AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
        AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
        AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
        AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
        AND blocking_locks.pid != blocked_locks.pid

    JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
   WHERE NOT blocked_locks.GRANTED;

"""

        for row in db.connection.execute(q2):
            if not headers:
                headers = row.keys()
            row = list(str(e) for e in row)

            row[2] = fill(row[2],(x-50)/2)
            row[3] = fill(row[3], (x-50)/2)
            rows.append(row)

        if rows:
            table = terminaltables.UnixTable([headers] + rows)
            print table.table

    if args.locks:

            headers = None
            rows = []

            q = """
    SELECT pid, database, mode, locktype, mode, relation::regclass, tuple, virtualxid
    FROM pg_locks
    ORDER BY pid
    ;
    """


            for row in db.connection.execute(q):
                if not headers:
                    headers = row.keys()
                row = list(str(e) for e in row)

                row[2] = fill(row[2], (x - 50) / 2)
                row[3] = fill(row[3], (x - 50) / 2)
                rows.append(row)

            if rows:
                table = terminaltables.UnixTable([headers] + rows)
                print table.table

def library_import(args, l, config):
    import json
    from ambry.library.config import LibraryConfigSyncProxy
    from ambry.orm import Account
    from simplecrypt import encrypt, decrypt, DecryptionException

    try:
        jsn = decrypt(args.password, args.config_file.read().decode('base64'))
    except DecryptionException as e:
        fatal(e)
    finally:
        args.config_file.close()

    args.config_file.close()

    d = json.loads(jsn)

    for k, v in d['accounts'].items():
        if v.get('major_type') != 'user' and 'encrypted_secret' in v:
            v['secret'] = Account.sym_decrypt(args.password, v['encrypted_secret'])

    if args.list:
        prt_no_format(json.dumps(d, indent=4))
    else:
        lcsp = LibraryConfigSyncProxy(l)

        lcsp.sync_remotes(d['remotes'], cb=l.logger.info)
        lcsp.sync_accounts(d['accounts'], cb=l.logger.info)

        for vid, v in d['bundles'].items():
            l.logger.info("Check in remote bundle {}, {}".format(vid, v['vname']))
            l.checkin_remote_bundle(vid)


def library_export(args, l, config):
    from simplecrypt import encrypt, decrypt, DecryptionException
    import json
    from ambry.util import random_string

    if args.password:
        password = args.password
    else:
        password = random_string(16)

    d = {
        'remotes': {},
        'accounts': {},
        'bundles': {}
    }

    for r in l.remotes:
        d['remotes'][r.short_name] = { k:v for k,v in r.dict.items() if bool(v ) and k not in ('id','list' ) }

    for k in l.accounts.keys():
        a = l.account(k)

        da = { k:v for k,v in a.dict.items() if bool(v )and k != 'secret' }

        # Change the secret encryption password
        if 'encrypted_secret' in da and da['encrypted_secret']:
            ds = a.decrypt_secret()
            if ds:
                da['encrypted_secret'] = a.encrypt_secret(ds, password)


        d['accounts'][a.account_id] = da

    for b in l.bundles:
        d['bundles'][b.identity.vid] = {
            'vid' : b.identity.vid,
            'vname': str(b.identity.name),
            'cache_key': b.identity.cache_key
        }

    if not args.password:
        prt("Password: {}".format(password))

    args.config_file.write(encrypt(password, json.dumps(d, indent = 4)).encode('base64'))
    args.config_file.close()




def library_unknown(args, l, config):
    fatal("Unknown subcommand")
    fatal(args)
