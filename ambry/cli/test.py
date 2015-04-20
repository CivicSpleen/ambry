"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""

from ..cli import prt, fatal


def test_parser(cmd):
    import argparse

    test_p = cmd.add_parser('test', help='Test and debugging')
    test_p.set_defaults(command='test')

    asp = test_p.add_subparsers(title='Test commands', help='command help')

    sp = asp.add_parser('config', help='Dump the configuration')
    sp.set_defaults(subcommand='config')
    sp.add_argument(
        '-v',
        '--version',
        default=False,
        action='store_true',
        help='Display module version')

    sp = asp.add_parser('spatialite', help='Test spatialite configuration')
    sp.set_defaults(subcommand='spatialite')

    sp = asp.add_parser('gitaccess', help='Test gitaccess')
    sp.set_defaults(subcommand='gitaccess')


def test_command(args, rc):
    from ..library import new_library

    globals()['test_' + args.subcommand](args, rc)


def test_spatialite(args, rc):
    from pysqlite2 import dbapi2 as db

    import os

    f = '/tmp/_db_spatialite_test.db'

    if os.path.exists(f):
        os.remove(f)

    conn = db.connect(f)

    conn.execute('select spatialite_version()')

    cur = conn.cursor()

    try:
        conn.enable_load_extension(True)
        conn.execute("select load_extension('/usr/lib/libspatialite.so')")
        #loaded_extension = True
    except AttributeError:
        #loaded_extension = False
        prt("WARNING: Could not enable load_extension(). ")

    rs = cur.execute('SELECT sqlite_version(), spatialite_version()')

    for row in rs:
        msg = "> SQLite v%s Spatialite v%s" % (row[0], row[1])
        print(msg)


def test_gitaccess(args, rc):
    from ..source.repository import new_repository

    repo = new_repository(rc.sourcerepo('default'))

    for e in repo.service.list():
        print e
