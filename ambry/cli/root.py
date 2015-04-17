"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..cli import prt, warn, fatal
from ..identity import LocationRef

# If the devel module exists, this is a development system.
try: from ambry.support.devel import *
except ImportError as e: from ambry.support.production import *

default_locations = [LocationRef.LOCATION.LIBRARY, LocationRef.LOCATION.REMOTE ]

def root_parser(cmd):
    import argparse
    from ..identity import LocationRef

    lr = LocationRef.LOCATION

    sp = cmd.add_parser('list', help='List bundles and partitions')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='list')
    sp.add_argument('-P', '--plain', default=False, action="store_true", help="Print only vids")
    sp.add_argument('-F', '--fields', type=str,
                    help="Specify fields to use. One of: 'locations', 'vid', 'status', 'vname', 'sname', 'fqname")
    sp.add_argument('-p', '--partitions', default=False, action="store_true", help="Show partitions")
    sp.add_argument('-t', '--tables', default=False, action="store_true", help="Show tables")
    sp.add_argument('-a', '--all', default=False, action="store_true", help='List everything')
    sp.add_argument('-l', '--library', default=False, action="store_const", const = lr.LIBRARY, help='List only the library')
    sp.add_argument('-r', '--remote', default=False, action="store_const", const = lr.REMOTE, help='List only the remote')
    sp.add_argument('-s', '--source', default=False, action="store_const", const = lr.SOURCE, help='List only the source')
    sp.add_argument('-w', '--warehouse', default=False, action="store_const", const='warehouse', help='List warehouses')
    sp.add_argument('-c', '--collection', default=False, action="store_const", const='collection', help='List collections')
    sp.add_argument('term', nargs = '?', type=str, help='Name or ID of the bundle or partition')

    sp = cmd.add_parser('info', help='Information about a bundle or partition')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='info')
    sp.add_argument('-l', '--library', default=False, action="store_const", const = lr.LIBRARY, help='Search only the library')
    sp.add_argument('-r', '--remote', default=False, action="store_const", const = lr.REMOTE, help='Search only the remote')
    sp.add_argument('-s', '--source', default=False, action="store_const", const = lr.SOURCE, help='Search only the source')
    sp.add_argument('-p', '--partitions', default=False, action="store_true", help="Show partitions")
    sp.add_argument('term',  type=str, nargs = '?', help='Name or ID of the bundle or partition')

    sp = cmd.add_parser('doc', help='Open a browser displaying documentation')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='doc')
    sp.add_argument('-f', '--force', default=False, action="store_true", help='Force generating files that already exist')
    sp.add_argument('term',  type=str, nargs = '?', help='Name or ID of the bundle or partition')

    sp = cmd.add_parser('meta', help='Dump the metadata for a bundle')
    sp.set_defaults(command='root')
    sp.set_defaults(subcommand='meta')
    sp.add_argument('term',  type=str, nargs = '?', help='Name or ID of the bundle or partition')
    sp.add_argument('-k', '--key', default=False, type=str, help='Return the value of a specific key')
    group = sp.add_mutually_exclusive_group()
    group.add_argument('-y', '--yaml', default=False, action='store_true', help='Output yaml')
    group.add_argument('-j', '--json', default=False, action='store_true', help='Output json')
    group.add_argument('-r', '--rows', default=False, action='store_true', help='Output key/value pair rows')


    sp.add_argument('terms', type=str, nargs=argparse.REMAINDER, help='Query commands to find packages with. ')

def root_command(args, rc):
    from ..library import new_library
    from . import global_logger
    from ..dbexceptions import ConfigurationError

    l = new_library(rc.library(args.library_name))
    l.logger = global_logger


    globals()['root_' + args.subcommand](args, l,  rc)

def root_list(args, l, rc):
    from ..cli import  _print_bundle_list
    from ambry.warehouse.manifest import Manifest
    from . import global_logger
    ##
    ## Listing warehouses and collections is different

    if args.collection:

        for f in l.manifests:

            try:
                m = Manifest(f.content)
                print "{:10s} {:25s}| {}".format(m.uid, m.title, m.summary['summary_text'])
            except Exception as e:
                warn("Failed to parse manifest {}: {}".format(f.ref, e))
                continue



        return

    if args.warehouse:

        if args.plain:
            fields = []
        else:
            fields = ['title','dsn','summary','url','cache']

        format = '{:5s}{:10s}{}'
        def _get(s,f):

            if f == 'dsn':
                f = 'path'

            try:
                return s.data[f] if f in s.data else getattr(s, f)
            except AttributeError:
                return ''

        for s in l.stores:
            print s.ref

            for f in fields:
                if _get(s,f):
                    print format.format('',f,_get(s,f))
        return
    ##
    ## The remainder are for listing bundles and partitions.

    if args.tables:
        for table in l.tables:
            print table.name, table.vid, table.dataset.identity.fqname

        return

    if args.plain:
        fields = ['vid']

    elif args.fields:
        fields = args.fields.split(',')

    else:
        fields = ['locations',  'vid',  'vname']

        if args.source:
            fields += ['status']


    locations = filter(bool, [args.library, args.remote, args.source])

    key = lambda ident : ident.vname

    if 'pcount' in fields:
        with_partitions = True
    else:
        with_partitions = args.partitions

    idents = sorted(l.list(with_partitions=with_partitions).values(), key=key)

    if args.term:
        idents = [ ident for ident in idents if args.term in ident.fqname ]

    if locations:
        idents = [ ident for ident in idents if ident.locations.has(locations)]


    _print_bundle_list(idents,
                       fields=fields,
                       show_partitions=args.partitions)

def root_info(args, l, rc):
    from ..cli import  _print_info
    from ..dbexceptions import NotFoundError, ConfigurationError
    import ambry

    locations = filter(bool, [args.library, args.remote, args.source])

    if not locations:
        locations = default_locations

    if not args.term:
        print "Version:  {}, {}".format(ambry._meta.__version__, 'production' if IN_PRODUCTION else 'development')
        print "Root dir: {}".format(rc.filesystem('root')['dir'])

        try:
            if l.source:
                print "Source :  {}".format(l.source.base_dir)
        except ConfigurationError:
            print "Source :  No source directory"

        print "Configs:  {}".format(rc.dict['loaded'])

        return

    ident = l.resolve(args.term, location=locations)

    if not ident:
        fatal("Failed to find record for: {}", args.term)
        return

    try:
        b = l.get(ident.vid)

        if not ident.partition:
            for p in b.partitions.all:
                ident.add_partition(p.identity)

    except NotFoundError:
        #fatal("Could not find bundle file for '{}'".format(ident.path))
        pass

    _print_info(l, ident, list_partitions=args.partitions)

def root_meta(args, l, rc):

    ident = l.resolve(args.term)

    if not ident:
        fatal("Failed to find record for: {}", args.term)
        return

    b = l.get(ident.vid)

    meta = b.metadata

    if not args.key:
        # Return all of the rows
        if args.yaml:
            print meta.yaml

        elif args.json:
            print meta.json

        elif args.key:
            for row in meta.rows:
                print '.'.join([e for e in row[0] if e])+'='+str(row[1] if row[1] else '')
        else:
            print meta.yaml

    else:

        v = None
        from ..util import AttrDict
        o = AttrDict()
        count = 0

        for row in meta.rows:
            k = '.'.join([e for e in row[0] if e])
            if k.startswith(args.key):
                v = row[1]
                o.unflatten_row(row[0], row[1])
                count +=1

        if count == 1:
            print v

        else:
            if args.yaml:
                print o.dump()

            elif args.json:
                print o.json()

            elif args.rows:
                for row in o.flatten():
                    print '.'.join([e for e in row[0] if e]) + '=' + str(row[1] if row[1] else '')

            else:
                print o.dump()


def root_doc(args, l, rc):
    import webbrowser

    try:
        ident = l.resolve(args.term)
    except ValueError:
        fatal("Can't parse ref: {} ".format(args.term))

    if not ident:
        fatal("Failed to find record for: {}", args.term)
        return


    b = l.get(ident.partition.vid if ident.partition else ident.vid)

    if not b:
        fatal("Failed to get bundle for: {}", args.term)
        return


    if b.partition:

        raise NotImplementedError()
        path = None
        #ck = b.partition.identity + '.html'
        #doc = BundleDoc(root_dir).render(p=p)

    else:
        path, extracts = b.write_doc(l.doc_cache, library = l)


    prt("Opening file: {} ".format(path))

    webbrowser.open_new("file://"+path)
