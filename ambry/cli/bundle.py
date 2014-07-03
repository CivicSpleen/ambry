"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""



from ..cli import prt, fatal, warn, err
from ..cli import  _source_list, load_bundle, _print_bundle_list
from ..source import SourceTree

import os
import yaml
import shutil



def bundle_command(args, rc):
    import os
    from ..run import import_file
    from ..dbexceptions import  DependencyError
    from ..library import new_library
    from . import global_logger
    from ..orm import Dataset

    l = new_library(rc.library(args.library_name))
    l.logger = global_logger

    if not args.bundle_dir:
        bundle_file = os.path.join(os.getcwd(),'bundle.py')
    else:
        st = l.source
        ident = l.resolve(args.bundle_dir)

        if ident:

            bundle_file = os.path.join(ident.bundle_path, 'bundle.py')

            if not os.path.exists(bundle_file):
                from ..dbexceptions import ConflictError
                # The bundle exists in the source repo, but is not local
                fatal("Ghost bundle {}; in library but not in source tree".format(ident.vname))



        elif args.bundle_dir == '-':
            # Run run for each line of input
            import sys

            for line in sys.stdin.readlines():
                args.bundle_dir = line.strip()
                prt('====== {}',args.bundle_dir)
                bundle_command(args,rc)

            return

        elif args.bundle_dir[0] != '/':
            bundle_file = os.path.join(os.getcwd(), args.bundle_dir, 'bundle.py')

        else:
            bundle_file = os.path.join(args.bundle_dir, 'bundle.py')



    if not os.path.exists(bundle_file):
        fatal("Bundle code file does not exist: {}".format(bundle_file) )

    bundle_dir = os.path.dirname(bundle_file)

    config_file = os.path.join(bundle_dir, 'bundle.yaml')

    if not os.path.exists(config_file):
        fatal("Bundle config file does not exist: {}".format(bundle_file) )

    # Import the bundle file from the
    rp = os.path.realpath(bundle_file)
    mod = import_file(rp)

    dir_ = os.path.dirname(rp)
    b = mod.Bundle(dir_)

    # In case the bundle lock is hanging around from a previous run
    b.database.break_lock()

    b.set_args(args)

    b.library  = l

    def getf(f):
        return globals()['bundle_'+f]

    ph = {
          'meta': ['clean'],
          'prepare': ['clean'],
          'build' : ['clean', 'prepare'],
          'update' : ['clean', 'prepare'],
          'install' : ['clean', 'prepare', 'build'],
          'submit' : ['clean', 'prepare', 'build'],
          'extract' : ['clean', 'prepare', 'build']
          }

    phases = []

    if hasattr(args,'clean') and args.clean:
        # If the clean arg is set, then we need to run  clean, and all of the
        # earlier build phases.

        phases += ph[args.subcommand]

    phases.append(args.subcommand)

    if args.debug:
        from ..util import  debug
        warn('Entering debug mode. Send USR1 signal (kill -USR1 ) to break to interactive prompt')
        debug.listen()

    try:
        for phase in phases:
            getf(phase)(args, b, st, rc)
    except DependencyError as e:
        if b:
            st.set_bundle_state(b.identity, 'error:dependency')
        fatal("{}: Phase {} failed: {}", b.identity.name, phase, e.message)
    except Exception as e:
        l.close()
        if b:
            err("{}: Phase {} failed: {}", b.identity.name, phase, e.message)
            b.close()
            st.set_bundle_state(b.identity, 'error:'+phase)
        raise
    finally:
        import lockfile
        try:
            if b:
                b.close()
        except lockfile.NotMyLock as e:
            warn("Got logging error: {}".format(str(e)))

def bundle_parser(cmd):
    import argparse, multiprocessing

    parser = cmd.add_parser('bundle', help='Manage bundle files')
    parser.set_defaults(command='bundle')
    parser.add_argument('-d','--bundle-dir', required=False,   help='Path to the bundle .py file')
    parser.add_argument('-D', '--debug', required=False, default=False, action="store_true",
                                        help='URS1 signal will break to interactive prompt')
    parser.add_argument('-t','--test',  default=False, action="store_true", help='Enable bundle-specific test behaviour')
    parser.add_argument('-m','--multi',  type = int,  nargs = '?',
                        default = 1,
                        const = multiprocessing.cpu_count(),
                        help='Run the build process on multiple processors, if the  method supports it')


    sub_cmd = parser.add_subparsers(title='commands', help='command help')

    command_p = sub_cmd.add_parser('config', help='Operations on the bundle configuration file')
    command_p.set_defaults(subcommand='config')

    asp = command_p.add_subparsers(title='Config subcommands', help='Subcommand for operations on a bundle file')

    # Config/Rewrite Command
    sp = asp.add_parser('rewrite', help='Re-write the bundle file, updating the formatting')     
    sp.set_defaults(subsubcommand='rewrite')

    # Config/Dump Command
    sp = asp.add_parser('dump', help='dump the configuration')     
    sp.set_defaults(subsubcommand='dump')

    # Config/Schema Command
    sp = asp.add_parser('schema', help='Print the schema')     
    sp.set_defaults(subsubcommand='schema')

    # Config/Incver Command
    sp = asp.add_parser('incver', help='Increment the version number')
    sp.set_defaults(subsubcommand='incver')

    # Info command
    command_p = sub_cmd.add_parser('info', help='Print information about the bundle')
    command_p.set_defaults(subcommand='info')
    command_p.set_defaults(subcommand='info')
    command_p.add_argument('-s','--schema',  default=False,action="store_true",
                           help='Dump the schema as a CSV. The bundle must have been prepared')
    command_p.add_argument('-d', '--dep', default=False, help='Report information about a dependency')

    #
    # Clean Command
    #
    command_p = sub_cmd.add_parser('clean', help='Return bundle to state before build, prepare and extracts')
    command_p.set_defaults(subcommand='clean')
    
    #
    # Meta Command
    #
    command_p = sub_cmd.add_parser('meta', help='Build or install metadata')
    command_p.set_defaults(subcommand='meta')
    
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')     
                     
    #
    # Prepare Command
    #
    command_p = sub_cmd.add_parser('prepare', help='Prepare by creating the database and schemas')
    command_p.set_defaults(subcommand='prepare')
    
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
    command_p.add_argument('-r','--rebuild', default=False,action="store_true", help='Rebuild the schema, but dont delete built files')
    
    #
    # Build Command
    #
    command_p = sub_cmd.add_parser('build', help='Build the data bundle and partitions')
    command_p.set_defaults(subcommand='build')
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
    command_p.add_argument('-f', '--force', default=False, action="store_true", help='Force build. ( --clean is usually preferred ) ')
    
    command_p.add_argument('-o','--opt', action='append', help='Set options for the build phase')
    
    #
    # Update Command
    #
    command_p = sub_cmd.add_parser('update', help='Build the data bundle and partitions from an earlier version')
    command_p.set_defaults(subcommand='update')
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')

    #
    # Extract Command
    #
    command_p = sub_cmd.add_parser('extract', help='Extract data into CSV and TIFF files. ')
    command_p.set_defaults(subcommand='extract')
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
    command_p.add_argument('-n','--name', default=None,action="store", help='Run only the named extract, and its dependencies')
    command_p.add_argument('-f','--force', default=False,action="store_true", help='Ignore done_if clauses; force all extracts')
    
    
    #
    # Submit Command
    #
    command_p = sub_cmd.add_parser('submit', help='Submit extracts to the repository ')
    command_p.set_defaults(subcommand='submit')
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')   
    command_p.add_argument('-r','--repo',  default=None, help='Name of the repository, defined in the config file')
    command_p.add_argument('-n','--name', default=None,action="store", help='Run only the named extract, and its dependencies')
    command_p.add_argument('-f','--force', default=False,action="store_true", help='Ignore done_if clauses; force all extracts')
    
    #
    # Install Command
    #
    command_p = sub_cmd.add_parser('install', help='Install bundles and partitions to the library')
    command_p.set_defaults(subcommand='install')
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
    command_p.add_argument('-l','--library',  help='Name of the library, defined in the config file')
    command_p.add_argument('-f','--force', default=False,action="store_true", help='Force storing the file')
    
    
    #
    # run Command
    #
    command_p = sub_cmd.add_parser('run', help='Run a method on the bundle')
    command_p.set_defaults(subcommand='run')
    command_p.add_argument('method', metavar='Method', type=str, 
                   help='Name of the method to run')    
    command_p.add_argument('args',  nargs='*', type=str,help='additional arguments')
    

     
    #
    # repopulate
    #
    command_p = sub_cmd.add_parser('repopulate', help='Load data previously submitted to the library back into the build dir')
    command_p.set_defaults(subcommand='repopulate')
    
    
    #
    # Source Commands
    #
    
    command_p = sub_cmd.add_parser('commit', help='Commit the source')
    command_p.set_defaults(subcommand='commit', command_group='source')
    command_p.add_argument('-m','--message', default=None, help='Git commit message')
    
    command_p = sub_cmd.add_parser('push', help='Commit and push to the git origin')
    command_p.set_defaults(subcommand='push', command_group='source')
    command_p.add_argument('-m','--message', default=None, help='Git commit message')
    
    command_p = sub_cmd.add_parser('pull', help='Pull from the git origin')
    command_p.set_defaults(subcommand='pull', command_group='source')

def bundle_info(args, b, st, rc):
    from ..dbexceptions import DatabaseMissingError

    if args.dep:
        #
        # Get the dependency and then re-run to display it.
        #
        dep = b.library.dep(args.dep)
        if not dep:
            fatal("Didn't find dependency for {}".format(args.dep))

        ns = vars(args)
        ns['dep'] = None

        bundle_info(args.__class__(**ns), dep, st, rc)

    elif args.schema:
        b.schema.as_csv()
    else:
        b.log("----Info: ".format(b.identity.sname))
        b.log("VID  : "+b.identity.vid)
        b.log("Name : "+b.identity.sname)
        b.log("VName: "+b.identity.vname)
        b.log("DB   : " + b.database.path)

        if b.database.exists():

            process = b.get_value_group('process')

            # Older bundles have the dbcreated values assigned to the root dataset vid ('a0/001')
            # instead of the bundles dataset vid
            from ambry.orm import Config
            root_db_created = b.database.get_config_value(Config.ROOT_CONFIG_NAME_V, 'process', 'dbcreated')

            b.log('Created   : ' + process.get('dbcreated', root_db_created.value))
            b.log('Prepared  : ' + process.get('prepared', ''))
            b.log('Built     : ' + process.get('built', ''))

            if process.get('buildtime', False):
                b.log('Build time: ' + str(round(float(process['buildtime']), 2)) + 's')

            b.log("Parts: {}".format(b.partitions.count))

            if b.partitions.count < 5 and b.partitions.count > 0:
                b.log("---- Partitions ---")
                for partition in b.partitions:
                    b.log("    "+partition.identity.sname)

        deps = None
        if b.config.build and b.config.build.get('dependencies', False):
            # For source bundles
            deps = b.config.build.dependencies.items()
        else:
            # for built bundles
            try:
                deps = b.odep.items()
            except AttributeError:
                deps = None
            except DatabaseMissingError:
                deps = None

        if deps:
            b.log("---- Dependencies ---")
            for k, v in deps:
                b.log("{}: {}".format(k, v))


def bundle_clean(args, b, st, rc):
    b.log("---- Cleaning ---")
    # Only clean the meta phases when it is explicityly specified.
    #b.clean(clean_meta=('meta' in phases))
    b.database.enable_delete = True
    b.clean()



def bundle_meta(args, b, st, rc):

    # The meta phase does not require a database, and should write files
    # that only need to be done once.
    if b.pre_meta():
        b.log("---- Meta ----")
        if b.meta():
            b.post_meta()
            b.log("---- Done Meta ----")
        else:
            b.log("---- Meta exited with failure ----")
            return False
    else:
        b.log("---- Skipping Meta ---- ")

def bundle_prepare(args, b, st, rc):
    return b.do_prepare()


def bundle_build(args, b, st, rc):
    return b.do_build()


def bundle_install(args, b, st, rc):

    force = args.force

    if b.pre_install():
        b.log("---- Install ---")
        if b.install(force=force):
            b.post_install()
            b.log("---- Done Installing ---")
        else:
            b.log("---- Install exited with failure ---")
            return False
    else:
        b.log("---- Skipping Install ---- ")

    return True

def bundle_run(args, b, st, rc):

    #
    # Run a method on the bundle. Can be used for testing and development.
    try:
        f = getattr(b,str(args.method))
    except AttributeError as e:
        b.error("Could not find method named '{}': {} ".format(args.method, e))
        b.error("Available methods : {} ".format(dir(b)))

        return

    if not callable(f):
        raise TypeError("Got object for name '{}', but it isn't a function".format(args.method))


    return f(*args.args)

def bundle_submit(args, b, st, rc):

    if b.pre_submit():
        b.log("---- Submit ---")
        if b.submit():
            b.post_submit()
            b.log("---- Done Submitting ---")
        else:
            b.log("---- Submit exited with failure ---")
    else:
        b.log("---- Skipping Submit ---- ")

def bundle_extract(args, b, st, rc):
    if b.pre_extract():
        b.log("---- Extract ---")
        if b.extract():
            b.post_extract()
            b.log("---- Done Extracting ---")
        else:
            b.log("---- Extract exited with failure ---")
    else:
        b.log("---- Skipping Extract ---- ")

def bundle_update(args, b, st, rc):

    if b.pre_update():
        b.log("---- Update ---")
        if b.update():
            b.post_update()
            b.log("---- Done Updating ---")
        else:
            b.log("---- Update exited with failure ---")
            return False
    else:
        b.log("---- Skipping Update ---- ")

def bundle_config(args, b, st, rc):

    if args.subsubcommand == 'rewrite':
        b.log("Rewriting the config file")
        with b.session:
            b.update_configuration()
    elif args.subsubcommand == 'dump':
        print b.config.dump()
    elif args.subsubcommand == 'schema':
        print b.schema.as_markdown()
    elif args.subsubcommand == 'incver':
        from ..identity import Identity
        identity = b.identity

        identity.on.revision = identity.on.revision + 1

        identity = Identity.from_dict(identity.ident_dict)

        b.update_configuration(identity=identity)

    else:
        err("Unknown args: {}".format(args))



def bundle_repopulate(args, b, st, rc):
    return b.repopulate()