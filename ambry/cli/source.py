"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt



"""



from ..cli import prt, plain_prt, fatal, warn, _find, _print_find, _print_bundle_entry

from ..cli import  load_bundle, _print_bundle_list
from ..source import SourceTree

import os
import yaml
import shutil

def source_command(args, rc):
    from ..library import new_library
    from . import logger


    l = new_library(rc.library(args.name))
    l.logger = logger

    st = l.source


    globals()['source_'+args.subcommand](args, l, st, rc)

def source_parser(cmd):
    import argparse
    
    src_p = cmd.add_parser('source', help='Manage bundle source files')
    src_p.set_defaults(command='source')
    src_p.add_argument('-n','--name',  default='default',  help='Select the name for the repository. Defaults to "default" ')
    asp = src_p.add_subparsers(title='source commands', help='command help')  
   

    sp = asp.add_parser('new', help='Create a new bundle')
    sp.set_defaults(subcommand='new')
    sp.set_defaults(revision=1) # Needed in Identity.name_parts
    sp.add_argument('-s','--source', required=True, help='Source, usually a domain name') 
    sp.add_argument('-d','--dataset',  required=True, help='Name of the dataset') 
    sp.add_argument('-b','--subset',  default=None, help='Name of the subset')
    sp.add_argument('-t','--time', default=None, help='Time period. Use ISO Time intervals where possible. ')
    sp.add_argument('-p', '--space', default=None, help='Spatial extent name')
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
    sp.add_argument('-P', '--plain', default=False, action='store_true',
                    help='Plain output; just print the bundle id, with no logging decorations')

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
 

    sp = asp.add_parser('edit', help='Run the editor defined in the EDITOR env var on the bundle directory')
    sp.set_defaults(subcommand='edit')
    sp.add_argument('term', type=str, help='Name or ID of the bundle or partition to print information for')


    sp = asp.add_parser('run', help='Run a shell command in source directories passed in on stdin')
    sp.set_defaults(subcommand='run')

    sp.add_argument('-P','--python', default=None, help=
                    'Path to a python class file to run. Loads as module and calls run(). The '+
                    'run() function can have any combination of arguments of these names: bundle_dir,'+
                    ' bundle, repo')
    sp.add_argument('-m','--message', nargs='+', default='.', help='Directory to start recursing from ')
    sp.add_argument('terms',nargs=argparse.REMAINDER, type=str,help='Bundle refs to run command on')

    group = sp.add_mutually_exclusive_group()
    group.add_argument('-c', '--commit',  default=False, dest='repo_command',   action='store_const', const='commit', help='Commit')
    group.add_argument('-p', '--push',  default=False, dest='repo_command',   action='store_const', const='push', help='Push to origin/master')    
    group.add_argument('-l', '--pull',  default=False, dest='repo_command',   action='store_const', const='pull', help='Pull from upstream')  
    group.add_argument('-i', '--install',  default=False, dest='repo_command',   action='store_const', const='install', help='Install the bundle')
    group.add_argument('-s', '--shell', default=False, dest='repo_command', action='store_const', const='shell',
                       help='Run a shell command')

    sp = asp.add_parser('watch', help='Watch the source directory for changes')
    sp.set_defaults(subcommand='watch')

def source_info(args, l, st, rc):
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

        ident = l.resolve(term, location=None)

        if not ident:
            fatal("Didn't find source for term '{}'. (Maybe need to run 'source sync')", term)

        try:
            bundle = st.resolve_bundle(ident.id_)
            _print_bundle_info(bundle=bundle)
        except ImportError:
            ident = l.resolve(term)
            _print_bundle_info(ident=ident)


def source_list(args, l, st, rc, names=None):
    '''List all of the source packages'''
    from collections import defaultdict
    import ambry.library as library

    l = library.new_library(rc.library(args.library_name))

    d = {}

    if args.plain:
        from . import plain_prt
        prtf = plain_prt
    else:
        prtf = prt


    l_list = l.list(datasets=d)

    s_lst =  st.list(datasets=d)

    if args.plain:
        for v in d.values():
            prtf(str(v.id_))
    else:
        _print_bundle_list(d.values(), subset_names=names, prtf=prtf)

def source_get(args, l, st, rc):
    '''Clone one or more registered source packages ( via sync ) into the source directory '''
    import ambry.library as library
    from ..dbexceptions import ConflictError
    from ..orm import Dataset


    for term in args.terms:
        from ..dbexceptions import ConflictError
        if term.startswith('http'):
            prt("Loading bundle from {}".format(term))
            try:
                bundle = st.clone(term)
                if bundle:
                    prt("Loaded {} into {}".format(bundle.identity.sname, bundle.bundle_dir))
            except ConflictError as e:
                fatal(e.message)

        else:
            ident = l.resolve(term, location = Dataset.LOCATION.SREPO)

            if not ident:
                fatal("Could not find bundle for term: {} ".format(term))

            f = l.files.query.type(Dataset.LOCATION.SREPO).ref(ident.vid).one

            if not f.source_url:
                fatal("Didn't get a git URL for reference: {} ".format(term))

            args.terms = [f.source_url]
            return source_get(args, l, st, rc)

                
def source_new(args, l, st, rc):
    '''Clone one or more registered source packages ( via sync ) into the source directory '''
    from ..source.repository import new_repository
    from ..identity import DatasetNumber, Identity
    from ..identity import NumberServer
    from requests.exceptions import HTTPError
    from collections import OrderedDict
    from ..dbexceptions import ConflictError

    repo = new_repository(rc.sourcerepo(args.name))  

    nsconfig = rc.group('numbers')

    if args.key:
        nsconfig['key'] = args.key

    ns = NumberServer(**nsconfig)

    d = vars(args)
    d['revision'] = 1

    d['btime'] = d.get('time',None)
    d['bspace'] = d.get('space', None)




    try:
        d['id'] = str(ns.next())
        prt("Got number from number server: {}".format(d['id']))
    except HTTPError as e:
        warn("Failed to get number from number server. Config = {}: {}".format(nsconfig, e.message))
        warn("Using self-generated number. There is no problem with this, but they are longer than centrally generated numbers.")
        d['id'] = str(DatasetNumber())

    ident = Identity.from_dict(d)

    bundle_dir =  os.path.join(repo.dir, ident.source_path)

    if not os.path.exists(bundle_dir):
        os.makedirs(bundle_dir)
    elif os.path.isdir(bundle_dir):
        fatal("Directory already exists: "+bundle_dir)

    try:
        ambry_account = rc.group('accounts').get('ambry', {})
    except:
        ambry_account = None

    if not ambry_account:
        fatal("Failed to get an accounts.ambry entry from the configuration. ( It's usually in {}. ) ".format(rc.USER_ACCOUNTS))

    if not ambry_account.get('name') or not ambry_account.get('email'):
        from ambry.run import RunConfig as rc
        fatal("Must set accounts.ambry.email and accounts.ambry.name, usually in {}".format(rc.USER_ACCOUNTS))

    config ={
        'identity':ident.ident_dict,
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
            f_out.write(f_in.read().replace("'**include**'", "!include 'README.md'"))
        
    os.remove(file_)
        
    p = lambda x : os.path.join(os.path.dirname(__file__),'..','support',x)

    shutil.copy(p('bundle.py'),bundle_dir)
    shutil.copy(p('README.md'),bundle_dir)
    shutil.copy(p('schema.csv'), os.path.join(bundle_dir, 'meta')  )
    #shutil.copy(p('about.description.md'), os.path.join(bundle_dir, 'meta')  )

    try:
        st.sync_bundle(bundle_dir)
    except ConflictError as e:
        from ..util import rm_rf
        rm_rf(bundle_dir)
        fatal("Failed to sync bundle at {}  ; {}".format(bundle_dir, e.message))
    else:
        prt("CREATED: {}, {}",ident.fqname, bundle_dir)


def source_build(args, l, st, rc):
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
                fatal("Argument '{}' must be either a bundle name or a directory".format(name))
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

        l = new_library(rc.library(args.library_name))

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
                fatal("{} Prepare failed", bundle.identity.name)
            
            if not bundle.run_build():
                fatal("{} Build failed", bundle.identity.name)
            
        if args.install and not args.dryrun:
            if not bundle.run_install(force=True):
                fatal('{} Install failed', bundle.identity.name)
            

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
            fatal("Failed to find directory for bundle {}".format(n))

        prt('')
        prt("{} Building in {}".format(n, dir_))
        build(dir_)

            
def source_run(args, l, st, rc):

    from ..orm import Dataset

    import sys

    if args.terms and args.repo_command != 'shell':
        def yield_term():
            for t in args.terms:
                yield t
    else:
        def yield_term():
            for line in sys.stdin.readlines():
                yield line.strip()

    for term in yield_term():

        ident = l.resolve(term, Dataset.LOCATION.SOURCE)

        if not ident:
            warn("Didn't get source bundle for term '{}'; skipping ".format(term))
            continue

        do_source_run(ident, args, l, st, rc)


def do_source_run(ident, args, l, st, rc):
    from ambry.run import import_file
    from ambry.source.repository.git import GitRepository

    root = ident.bundle_path

    repo = GitRepository(None, root)
    repo.bundle_dir = root

    if args.python:

        import inspect

        try:
            mod = import_file(args.python)
        except ImportError:
            import ambry.cli.source_run as sr

            f = os.path.join(os.path.dirname(sr.__file__), args.python+".py" )
            try:
                mod = import_file(f)
            except ImportError:
                raise
                fatal("Could not get python file neither '{}', nor '{}'".format(args.python, f))



        run_args = inspect.getargspec(mod.run)

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


    elif args.repo_command == 'shell':


        cmd = ' '.join(args.terms)

        saved_path = os.getcwd()
        os.chdir(root)
        prt('----- {}', root)
        prt('----- {}', cmd)

        os.system(cmd)
        prt('')
        os.chdir(saved_path)



def source_init(args, l, st, rc):
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


def source_deps(args, l, st, rc):
    """Produce a list of dependencies for all of the source bundles"""

    from ..util import toposort
    from ..source.repository import new_repository
    from ..identity import Identity
    from collections import defaultdict

    sources = l.files.query.type(l.files.TYPE.SOURCE).all

    errors = defaultdict(set)
    deps = defaultdict(set)

    ident_map = {}

    import pprint

    for source in sources:

        if not ('dependencies' in source.data
                and source.data['dependencies']
                and source.data['identity']):
            continue

        bundle_ident = Identity.from_dict(source.data['identity'])

        if not bundle_ident:

            warn("Failed to resolve bundle: {}, {} ".format(source.ref, source.path))
            continue

        for v in source.data['dependencies'].values():
            try:
                ident = l.resolve(v, location=None)
            except:
                ident = None


            if not ident:
                errors[bundle_ident.sname].add(v)
                continue

            deps[ident.id_].add(ident)


    print "DEPS"
    print deps

    print "ERROR"
    for name, errors in errors.items():
        print '=',name
        for e in errors:
            print '    ', e

    return

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




def source_watch(args, l, st, rc):

    st.watch()

def source_edit(args, l, st, rc):
    from ambry.orm import Dataset
    from os import environ
    from subprocess import Popen

    if not args.term:
        fatal("Must supply a bundle term")

    term = args.term

    editor = environ['EDITOR']

    try:
        ident = l.resolve(term, Dataset.LOCATION.SOURCE)
    except ValueError:
        ident = None

    if not ident:
        fatal("Didn't find a source bundle for term: {} ".format(term))

    root = ident.bundle_path

    prt("Running: {} {}".format(editor, root))
    prt("Build with: ambry bundle -d {} build".format(ident.sname))
    prt("Directory : {}".format(ident.bundle_path))
    Popen(['env',editor,root])





