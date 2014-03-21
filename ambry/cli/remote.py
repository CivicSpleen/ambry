"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..cli import prt, fatal, _find, plain_prt, _print_bundle_list, _print_bundle_entry
import argparse

def remote_parser(cmd):

    lib_p = cmd.add_parser('remote', help='Access the remote library')
    lib_p.set_defaults(command='remote')
    asp = lib_p.add_subparsers(title='remote commands', help='Access the remote library')
    lib_p.add_argument('-n','--name',  default='default',  help='Select a different name for the library, from which the remote is located')
 
    group = lib_p.add_mutually_exclusive_group()
    group.add_argument('-s', '--server',  default=False, dest='is_server',  action='store_true', help = 'Select the server configuration')
    group.add_argument('-c', '--client',  default=False, dest='is_server',  action='store_false', help = 'Select the client configuration')
        
    sp = asp.add_parser('info', help='Display the remote configuration')
    sp.set_defaults(subcommand='info')
    sp.add_argument('term',  nargs='?', type=str,help='Name or ID of the bundle or partition to print information for')

    sp = asp.add_parser('list', help='List remote files')
    sp.set_defaults(subcommand='list')
    sp.add_argument('-m','--meta', default=False,  action='store_true',  help="Force fetching metadata for remotes that don't provide it while listing, like S3")
    sp.add_argument('term', nargs=argparse.REMAINDER)
        
    sp = asp.add_parser('find', help='Search for the argument as a bundle or partition name or id')
    sp.set_defaults(subcommand='find')   
    sp.add_argument('term', type=str, nargs=argparse.REMAINDER,help='Query term')


def remote_command(args, rc):
    from ambry.library import new_library

    l = new_library(rc.library(args.name))

    globals()['remote_'+args.subcommand](args, l,rc)



def remote_info(args, l, rc):
    from ..identity import Identity
    from ambry.client.exceptions import NotFound
    
    if args.term:
        try:
            dsi = l.upstream.get_ref(args.term)
        except NotFound:
            dsi = None

        if not dsi:
            fatal("Failed to find record for: {}", args.term)
            return 

        d = Identity.from_dict(dsi['dataset'])
        p = Identity.from_dict(dsi['partitions'].items()[0][1]) if dsi['ref_type'] == 'partition' else None
                
        _print_info(l,d,p)

    else:
        prt(str(l.upstream))

def remote_list(args, l, rc, return_meta=False):
    from . import _print_bundle_entry

    fields = ['locations', 'vid', 'status', 'vname']
    show_partitions = True

    if args.term:
        # List just the partitions in some data sets. This should probably be combined into info.
        for term in args.term:

            ip, ident = l.remote_resolver.resolve_ref_one(term)

            print vars(ident)

            _print_bundle_entry(ident, prtf=prt, fields=fields,
                                show_partitions=show_partitions)

            return

            dsi = l.upstream.get_ref(ds)

            prt("dataset {0:11s} {1}",dsi['dataset']['id'],dsi['dataset']['name'])

            for id_, p in dsi['partitions'].items():
                vs = ''
                for v in ['time','space','table','grain','format']:
                    val = p.get(v,False)
                    if val:
                        vs += "{}={} ".format(v, val)
                prt("        {0:11s} {1:50s} {2} ",id_,  p['name'], vs)
            
    else:

        datasets = l.upstream.list(with_metadata=return_meta)

        import pprint
        pprint.pprint(datasets)
        for id_, data in sorted(datasets.items(), key = lambda x: x[1]['identity']['vname']):

            try:
                prt("{:10s} {:50s} {:s}",data['identity']['vid'],data['identity']['vname'],id_)  
            except Exception as e:
                prt("{:10s} {:50s} {:s}",'[ERROR]','',id_)  


def remote_find(args, l, config):
    return _find(args, l, config, True)

