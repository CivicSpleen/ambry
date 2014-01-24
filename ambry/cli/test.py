"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..cli import prt,err

def test_parser(cmd):

    return

    err('Test command is broken ...')


    lib_p = cmd.add_parser('test', help='Test and debugging')
    lib_p.set_defaults(command='test')
    asp = lib_p.add_subparsers(title='Test commands', help='command help')
    
    sp = asp.add_parser('config', help='Dump the configuration')
    sp.set_defaults(subcommand='config')
    group.add_argument('-v', '--version',  default=False, action='store_true', help='Display module version')
 
    sp = asp.add_parser('spatialite', help='Test spatialite configuration')
    sp.set_defaults(subcommand='spatialite')
         
       
                    
def test_command(args,rc):
    
    if args.subcommand == 'config':
        prt(rc.dump())
    elif args.subcommand == 'spatialite':
        from pysqlite2 import dbapi2 as db
        import os
        
        f = '/tmp/_db_spatialite_test.db'
        
        if os.path.exists(f):
            os.remove(f)
        
        conn = db.connect(f)
    
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

    
    else:
        prt('Testing')
        prt(args)