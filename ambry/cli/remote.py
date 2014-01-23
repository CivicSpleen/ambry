"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from . import prt, err, _print_info, _find #@UnresolvedImport
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
    sp.add_argument('datasets', nargs=argparse.REMAINDER)
        
    sp = asp.add_parser('find', help='Search for the argument as a bundle or partition name or id')
    sp.set_defaults(subcommand='find')   
    sp.add_argument('term', type=str, nargs=argparse.REMAINDER,help='Query term')



def remote_command(args, rc, src):
    from ambry.library import new_library

    if args.is_server:
        config  = src
    else:
        config = rc
    
    l = new_library(config.library(args.name))

    globals()['remote_'+args.subcommand](args, l,config)



def remote_info(args, l, rc):
    from ..identity import new_identity
    from ambry.client.exceptions import NotFound
    
    if args.term:
        try:
            dsi = l.upstream.get_ref(args.term)
        except NotFound:
            dsi = None

        if not dsi:
            err("Failed to find record for: {}", args.term)
            return 

        d = new_identity(dsi['dataset'])
        p = new_identity(dsi['partitions'].items()[0][1]) if dsi['ref_type'] == 'partition' else None
                
        _print_info(l,d,p)

    else:
        prt(str(l.upstream))

def remote_list(args, l, rc, return_meta=False):
        
    if args.datasets:
        # List just the partitions in some data sets. This should probably be combined into info. 
        for ds in args.datasets:
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


        for id_, data in sorted(datasets.items(), key = lambda x: x[1]['identity']['vname']):

            try:
                prt("{:10s} {:50s} {:s}",data['identity']['vid'],data['identity']['vname'],id_)  
            except Exception as e:
                prt("{:10s} {:50s} {:s}",'[ERROR]','',id_)  


def remote_find(args, l, config):
    return _find(args, l, config, True)

