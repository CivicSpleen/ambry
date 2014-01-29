"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt



"""



from ..cli import prt, plain_prt, err, warn, _find, _print_find

from ..cli import  load_bundle, _print_bundle_list
from ..source import SourceTree

import os
import yaml
import shutil

def source_command(args, rc):

    def logger(x):
        prt(x)

    st = SourceTree(rc.sourcerepo.dir, logger=logger)

    globals()['source_'+args.subcommand](args, st, rc)

def source_parser(cmd):
    import argparse
    
    src_p = cmd.add_parser('source', help='Manage bundle source files')
    src_p.set_defaults(command='source')
    src_p.add_argument('-n','--name',  default='default',  help='Select the name for the repository. Defaults to "default" ')
    src_p.add_argument('-l','--library',  default='default',  help='Select a different name for the library')
    asp = src_p.add_subparsers(title='source commands', help='command help')  
   

    sp = asp.add_parser('new', help='Create a new bundle')
    sp.set_defaults(subcommand='new')
    sp.set_defaults(revision=1) # Needed in Identity.name_parts
    sp.add_argument('-s','--source', required=True, help='Source, usually a domain name') 
    sp.add_argument('-d','--dataset',  required=True, help='Name of the dataset') 
    sp.add_argument('-b','--subset',  default=None, help='Name of the subset')
    sp.add_argument('-v','--variation', default=None, help='Name of the variation')
    sp.add_argument('-c','--creator',  required=False, help='Id of the creator')
    sp.add_argument('-n','--dryrun', default=False, help='Dry run')
    sp.add_argument('-k', '--key', help='Number server key')
    sp.add_argument('args', nargs=argparse.REMAINDER) # Get everything else. 

    sp = asp.add_parser('info', help='Information about the source configuration')
    sp.set_defaults(subcommand='info')
    sp.add_argument('terms', type=str, nargs=argparse.REMAINDER, help='Name or ID of the bundle or partition to print information for')
    
    
    sp = asp.add_parser('deps', help='Print the depenencies for all source bundles')
    sp.set_defaults(subcommand='deps')
    sp.add_argument('ref', type=str,nargs='?',help='Name or id of a bundle to generate a sorted dependency list for.')   
    sp.add_argument('-d','--detail',  default=False,action="store_true",  help='Display details of locations for each bundle')   
    group = sp.add_mutually_exclusive_group()
    group.add_argument('-f', '--forward',  default='f', dest='direction',   action='store_const', const='f', help='Display bundles that this one depends on')
    group.add_argument('-r', '--reverse',  default='f', dest='direction',   action='store_const', const='r', help='Display bundles that depend on this one')
    
    sp = asp.add_parser('init', help='Intialize the local and remote git repositories')
    sp.set_defaults(subcommand='init')
    sp.add_argument('dir', type=str,nargs='?',help='Directory')

    sp = asp.add_parser('list', help='List the source dirctories')
    sp.set_defaults(subcommand='list')
    sp.add_argument('-l', '--list-library', default=False, action="store_true", help='Also include a list from the library')
    sp.add_argument('-P', '--plain', default=False, action='store_true',
                    help='Plain output; just print the bundle id, with no logging decorations')

    sp = asp.add_parser('sync', help='Load references from the configured source remotes')
    sp.set_defaults(subcommand='sync')
    sp.add_argument('-l','--library',  default='default',  help='Select a library to add the references to')
    sp.add_argument('-p', '--print',  default=False, action="store_true", help='Print, rather than actually sync')

    sp = asp.add_parser('get', help='Load a source bundle into the local source directory')
    sp.set_defaults(subcommand='get')
    sp.add_argument('terms', type=str, nargs=argparse.REMAINDER, help='Bundle references, a git url or identity reference')
  
    sp = asp.add_parser('build', help='Build sources')
    sp.set_defaults(subcommand='build')
    sp.add_argument('-p','--pull', default=False,action="store_true", help='Git pull before build')
    sp.add_argument('-s','--stash', default=False,action="store_true", help='Git stash before build')
    sp.add_argument('-f','--force', default=False,action="store_true", help='Build even if built or in library')
    sp.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
    sp.add_argument('-i','--install', default=False,action="store_true", help='Install after build')
    sp.add_argument('-n','--dryrun', default=False,action="store_true", help='Only display what would be built')

    sp.add_argument('dir', type=str,nargs='?',help='Directory to start search for sources in. ')      
 
 
    sp = asp.add_parser('run', help='Run a shell command in source directories')
    sp.set_defaults(subcommand='run')
    sp.add_argument('-d','--dir', nargs='?', help='Directory to start recursing from ')
    sp.add_argument('-P','--python', default=None, help=
                    'Path to a python class file to run. Loads as module and calls run(). The '+
                    'run() function can have any combination of arguments of these names: bundle_dir,'+
                    ' bundle, repo')
    sp.add_argument('-m','--message', nargs='+', default='.', help='Directory to start recursing from ')
    sp.add_argument('shell_command',nargs=argparse.REMAINDER, type=str,help='Shell command to run')

    group = sp.add_mutually_exclusive_group()
    group.add_argument('-c', '--commit',  default=False, dest='repo_command',   action='store_const', const='commit', help='Commit')
    group.add_argument('-p', '--push',  default=False, dest='repo_command',   action='store_const', const='push', help='Push to origin/master')    
    group.add_argument('-l', '--pull',  default=False, dest='repo_command',   action='store_const', const='pull', help='Pull from upstream')  
    group.add_argument('-i', '--install',  default=False, dest='repo_command',   action='store_const', const='install', help='Install the bundle')

    sp = asp.add_parser('find', help='Find source packages that meet a variety of conditions')
    sp.set_defaults(subcommand='find')
    sp.add_argument('-d','--dir',  help='Directory to start recursing from ')
    sp.add_argument('-P', '--plain', default=False,  action='store_true',
                    help='Plain output; just print the bundle path, with no logging decorations')
    group = sp.add_mutually_exclusive_group()
    group.add_argument('-s', '--source', default=False, action='store_true', help='Find source bundle')
    group.add_argument('-b', '--built', default=False,  action='store_true', help='Find bundles that have been built')
    group.add_argument('-c', '--commit',  default=False,  action='store_true', help='Find bundles that need to be committed')
    group.add_argument('-p', '--push',  default=False, action='store_true', help='Find bundles that need to be pushed')
    group.add_argument('-i', '--init',  default=False, action='store_true', help='Find bundles that need to be initialized')
    group.add_argument('-a', '--all', default=False, action='store_true',
                       help='List all bundles, from root or sub dir')

    sp.add_argument('terms', type=str, nargs=argparse.REMAINDER, help='Query commands to find packages with. ')

    sp = asp.add_parser('watch', help='Watch the source directory for changes')
    sp.set_defaults(subcommand='watch')

def source_info(args, st, rc):
    from . import _print_bundle_info

    if not args.terms:
        prt("Source dir: {}", rc.sourcerepo.dir)
        for repo in  rc.sourcerepo.list:
            prt("Repo      : {}", repo.ident)
    if args.terms[0] == '-':
        # Read terms from stdin, one per line.
        import sys

        for line in sys.stdin.readlines():
            args.terms = [line.strip()]
            source_info(args,st,rc)


    else:
        import ambry.library as library
        from ..identity import Identity

        term = args.terms.pop(0)

        ident = st.library.resolve(term)

        if not ident:
            err("Didn't find source for term '{}'. (Maybe need to run 'source sync')", term)

        try:
            bundle = st.library.resolve_bundle(term)
            _print_bundle_info(bundle=bundle)
        except ImportError:
            ident = st.library.resolve(term)
            _print_bundle_info(ident=ident)


def source_list(args, st, rc, names=None):
    '''List all of the source packages'''
    from collections import defaultdict
    import ambry.library as library

    l = library.new_library(rc.library(args.library))

    d = {}

    if args.plain:
        from . import plain_prt
        prtf = plain_prt
    else:
        prtf = prt

    if args.list_library:
        pass

    l_list = l.list(datasets=d)

    s_lst =  st.list(datasets=d)

    if args.plain:
        for v in d.values():
            prtf(str(v.id_))
    else:
        _print_bundle_list(d.values(), subset_names=names, prtf=prtf)

def source_get(args, st, rc):
    '''Clone one or more registered source packages ( via sync ) into the source directory '''
    import ambry.library as library
    from ..dbexceptions import ConflictError


    for term in args.terms:
        from ..dbexceptions import ConflictError
        if term.startswith('http'):
            prt("Loading bundle from {}".format(term))
            try:
                bundle = st.clone(term)
                prt("Loaded {} into {}".format(bundle.identity.sname, bundle.bundle_dir))
            except ConflictError as e:
                err(e.message)

        else:
            ident = st.library.resolve(term)


            if not ident:
                err("Could not find bundle for term: {} ".format(term))

            if not ident.url:
                err("Didn't get a git URL for reference: {} ".format(term))

            args.terms = [ident.url]
            return source_get(args, st, rc)

                
def source_new(args, st, rc):
    '''Clone one or more registered source packages ( via sync ) into the source directory '''
    from ..source.repository import new_repository
    from ..identity import DatasetNumber, Identity
    from ..identity import NumberServer
    from requests.exceptions import HTTPError
    from collections import OrderedDict

    repo = new_repository(rc.sourcerepo(args.name))  

    nsconfig = rc.group('numbers')

    if args.key:
        nsconfig['key'] = args.key

    ns = NumberServer(**nsconfig)

    d = vars(args)
    d['revision'] = 1

    try:
        d['id'] = str(ns.next())
    except HTTPError as e:
        warn("Failed to get number from number server. Config = {}: {}".format(nsconfig, e.message))
        warn("Using self-generated number. There is no problem with this, but they are longer than centrally generated numbers.")
        d['id'] = str(DatasetNumber())

    ident = Identity.from_dict(d)

    bundle_dir =  os.path.join(repo.dir, ident.source_path)

    if not os.path.exists(bundle_dir):
        os.makedirs(bundle_dir)
    elif not os.path.isdir(bundle_dir):
        raise IOError("Directory already exists: "+bundle_dir)

    try:
        ambry_account = rc.group('accounts').get('ambry', {})
    except:
        ambry_account = None

    if not ambry_account:
        err("Failed to get an accounts.ambry entry from the configuration. ( It's usually in {}. ) ".format(rc.USER_ACCOUNTS))

    if not ambry_account.get('name') or not ambry_account.get('email'):
        from ambry.run import RunConfig as rc
        err("Must set accounts.ambry.email and accounts.ambry.name, usually in {}".format(rc.USER_ACCOUNTS))


    config ={
        'identity':{
             'id': str(DatasetNumber()),
             'source': args.source,
             'creator': args.creator,
             'dataset':args.dataset,
             'subset': args.subset,
             'variation': args.variation,
             'revision': args.revision,
             'version': '0.0.1'
         },
        'about': {
            'author': ambry_account.get('name'),
            'author_email': ambry_account.get('email'),
            'description': "**include**", # Can't get YAML to write this properly
            'groups': ['group1','group2'],
            'homepage': "https://civicknowledge.org",
            'license': "other-open",
            'maintainer': ambry_account.get('name'),
            'maintainer_email': ambry_account.get('email'),
            'tags': ['tag1','tag2'],
            'title': "Bundle title"
        }
    }
    
    os.makedirs(os.path.join(bundle_dir, 'meta'))
    
    file_ = os.path.join(bundle_dir, 'bundle.yaml-in')
    
    yaml.dump(config, file(file_, 'w'), indent=4, default_flow_style=False)

    # Need to edit the YAML file because the !include line is special metadata
    # that is hard ( or impossible ) to write through serialization
    
    with file(file_, 'r') as f_in:
        with file(os.path.join(bundle_dir, 'bundle.yaml'), 'w') as f_out:
            f_out.write(f_in.read().replace("'**include**'", "!include 'meta/about.description.md'"))
        
    os.remove(file_)
        
    p = lambda x : os.path.join(os.path.dirname(__file__),'..','support',x)

    shutil.copy(p('bundle.py'),bundle_dir)
    shutil.copy(p('README.md'),bundle_dir)
    shutil.copy(p('schema.csv'), os.path.join(bundle_dir, 'meta')  )
    shutil.copy(p('about.description.md'), os.path.join(bundle_dir, 'meta')  )

    st.sync_bundle(bundle_dir)

    prt("CREATED: {}",bundle_dir)


def source_build(args, st, rc):
    '''Build a single bundle, or a set of bundles in a directory. The build process
    will build all dependencies for each bundle before buildng the bundle. '''
    
    
    from ambry.identity import Identity
    from ..source.repository import new_repository
    
    repo = new_repository(rc.sourcerepo(args.name))   
       
    dir_ = None
    name = None
    
    if args.dir:
        if os.path.exists(args.dir):
            dir_ = args.dir
            name = None
        else:
            name = args.dir
            try: 
                Identity.parse_name(name)
            except:  
                err("Argument '{}' must be either a bundle name or a directory".format(name))
                return
            
    if not dir_:
        dir_ = rc.sourcerepo.dir
        
    
        
    def build(bundle_dir):
        from ambry.library import new_library

        
        # Stash must happen before pull, and pull must happen
        # before the class is loaded in load_bundle, otherwize the class
        # can't be updated by the pull. And, we have to use the GitShell
        # sevice directly, because thenew_repository route will ooad the bundle
        
        gss = GitShellService(bundle_dir)
        
        if args.stash:
            prt("{} Stashing ", bundle_dir)
            gss.stash()
            
        if args.pull:
            prt("{} Pulling ", bundle_dir)
            gss.pull()

        # Import the bundle file from the directory

        bundle_class = load_bundle(bundle_dir)
        bundle = bundle_class(bundle_dir)

        l = new_library(rc.library(args.library))

        if l.get(bundle.identity.vid)  and not args.force:
            prt("{} Bundle is already in library", bundle.identity.name)
            return
        elif bundle.is_built and not args.force and not args.clean:
            prt("{} Bundle is already built",bundle.identity.name)
            return
        else:

            if args.dryrun:
                prt("{} Would build but in dry run ", bundle.identity.name)
                return

            repo.bundle = bundle
             
            if args.clean: 
                bundle.clean()
                
            # Re-create after cleaning is important for something ... 

            bundle = bundle_class(bundle_dir)
                

            prt("{} Building ", bundle.identity.name)

            if not bundle.run_prepare():
                err("{} Prepare failed", bundle.identity.name)
            
            if not bundle.run_build():
                err("{} Build failed", bundle.identity.name)
            
        if args.install and not args.dryrun:
            if not bundle.run_install(force=True):
                err('{} Install failed', bundle.identity.name)
            

    build_dirs = {}
    
    # Find all of the dependencies for the named bundle, and make those first. 
    for root, _, files in os.walk(rc.sourcerepo.dir):
        if 'bundle.yaml' in files:
            bundle_class = load_bundle(root)
            bundle = bundle_class(root)      
            build_dirs[bundle.identity.name] = root 


    if name:
        deps = repo.bundle_deps(name)
        deps.append(name)
        
    else:

        deps = []

        # Walk the subdirectory for the files to build, and
        # add all of their dependencies
        for root, _, files in os.walk(dir_):
            if 'bundle.yaml' in files:

                bundle_class = load_bundle(root)
                bundle = bundle_class(root)

                for dep in repo.bundle_deps(bundle.identity.name):
                    if dep not in deps:
                        deps.append(dep)

                deps.append(bundle.identity.name)
    

    for n in deps:
        try:
            dir_ = build_dirs[n]
        except KeyError:
            err("Failed to find directory for bundle {}".format(n))

        prt('')
        prt("{} Building in {}".format(n, dir_))
        build(dir_)

            
def source_run(args, st, rc):
    from ambry.run import import_file
    from ambry.source.repository.git import GitRepository

    dir_ = args.dir

    if args.python:
        import inspect
        mod = import_file(args.python)

        run_args = inspect.getargspec(mod.run)

    else:
        mod = None

    if not dir_:
        dir_ = rc.sourcerepo.dir

    for root, dirs, files in os.walk(dir_):

        # Yes! can edit dirs in place!
        dirs[:] = [d for d in dirs if not d.startswith('_')]

        if 'bundle.yaml' in files:
            repo = GitRepository(None, root)
            repo.bundle_dir = root

            if args.python:
                a = {}

                if 'bundle_dir' in run_args.args:
                    a['bundle_dir'] = root

                if 'repo' in run_args.args:
                    a['repo'] = repo

                if 'args' in run_args.args:
                    a['args'] = args.shell_command

                if 'bundle' in run_args.args:
                    rp = os.path.join(root, 'bundle.py')
                    bundle_mod = import_file(rp)
                    dir_ = os.path.dirname(rp)
                    a['bundle'] = bundle_mod.Bundle(dir_)

                mod.run(**a)

            elif args.repo_command == 'commit' and repo.needs_commit():
                prt("--- {} {}",args.repo_command, root)
                repo.commit(' '.join(args.message))
                
            elif args.repo_command == 'push' and repo.needs_push():
                prt("--- {} {}",args.repo_command, root)
                repo.push()
                
            elif args.repo_command == 'pull':
                prt("--- {} {}",args.repo_command, root)
                repo.pull()
                
            elif args.repo_command == 'install':
                prt("--- {} {}",args.repo_command, root)    
                bundle_class = load_bundle(root)
                bundle = bundle_class(root)
        
                bundle.run_install()
        
        
            elif args.shell_command:
                
                cmd = ' '.join(args.shell_command)
                
                saved_path = os.getcwd()
                os.chdir(root)   
                prt('----- {}', root)
                prt('----- {}', cmd)
        
                os.system(cmd)
                prt('')
                os.chdir(saved_path)         
       
def source_find(args, st, rc):
    from ..source.repository.git import GitRepository
    from ..identity import Identity

    dir_ = args.dir

    prtf=prt

    if not dir_:
        dir_ = rc.sourcerepo.dir   


    if args.terms:

        identities = _find(args, st.library._library, rc, False)

        if args.plain:
            for ident in identities:

                ident = st.library.resolve(ident['identity']['vid'])

                plain_prt('{}'.format(ident.sname))

        else:

            _print_bundle_list([ st.library.resolve(i['identity']['vid']) for i in identities])

            #_print_find(identities, prtf=prtf)

    else:
        for root, _, files in os.walk(dir_):
            if 'bundle.yaml' in files:

                repo = GitRepository(None, root)
                repo.bundle_dir = root
                if args.commit:
                    if repo.needs_commit():
                        prtf(root)
                elif args.push:
                    if repo.needs_push():
                        prtf(root)
                elif args.init:
                    if repo.needs_init():
                        prtf(root)
                elif args.all:
                    prtf(root)
                else:
                    err("Must specify either --push. --init or --commit")

   
         
def source_init(args, st, rc):
    from ..source.repository import new_repository

    dir_ = args.dir
    
    if not dir_:
        dir_ = os.getcwd()
    
    repo = new_repository(rc.sourcerepo(args.name))
    repo.bundle_dir = dir_

    repo.delete_remote()
    import time
    time.sleep(3)
    repo.init()
    repo.init_remote()
    
    repo.push()

    st.sync_bundle(dir_)

    
def source_sync(args, st, rc):
    '''Synchronize all of the repositories with the local library'''
    from ..source.repository.git import GitShellService

    st.sync(rc.sourcerepo.list)

def source_deps(args, st, rc):
    """Produce a list of dependencies for all of the source bundles"""

    from ..util import toposort
    from ..source.repository import new_repository

    repo = new_repository(rc.sourcerepo(args.name))        


    if args.ref:

        deps = repo.bundle_deps(args.ref, reverse=bool(args.direction == 'r'))

        if args.detail:
            source_list(args,rc, names=deps)
        else:
            for b in deps:
                prt(b)    

        
    else:

        graph = toposort(repo.dependencies)
    
        for i,level in enumerate(graph):
            for j, name in enumerate(level):
                prt("{:3d} {:3d} {}",i,j,name)




def source_watch(args, st, rc):

    st.watch()





