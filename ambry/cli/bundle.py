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
    from ..identity import  LocationRef

    l = new_library(rc.library(args.library_name))
    l.logger = global_logger

    if not args.bundle_dir:
        bundle_file = os.path.join(os.getcwd(),'bundle.py')

    else:
        st = l.source
        ident = l.resolve(args.bundle_dir, location = LocationRef.LOCATION.SOURCE)

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
          'install' : ['clean', 'prepare', 'build']
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
            b.close()

    except DependencyError as e:
        if b:
            st.set_bundle_state(b.identity, 'error:dependency')
        fatal("{}: Phase {} failed: {}", b.identity.name, phase, e.message)
    except Exception as e:

        l.close()
        if b:
            err("{}: Phase {} failed: {}", b.identity.name, phase, e)
            b.close()
            st.set_bundle_state(b.identity, 'error:'+phase)
        raise
    finally:
        import lockfile
        from sqlalchemy.exc import InvalidRequestError
        try:
            if b:
                try:
                    b.close()
                except InvalidRequestError:
                    pass

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


    sp = asp.add_parser('rewrite', help='Re-write the bundle file, updating the formatting')     
    sp.set_defaults(subsubcommand='rewrite')

    #
    sp = asp.add_parser('dump', help='dump the configuration')     
    sp.set_defaults(subsubcommand='dump')

    #
    sp = asp.add_parser('schema', help='Print the schema')     
    sp.set_defaults(subsubcommand='schema')

    #
    sp = asp.add_parser('incver', help='Increment the version number')
    sp.set_defaults(subsubcommand='incver')

    #
    sp = asp.add_parser('newnum', help='Get a new dataset number')
    sp.set_defaults(subsubcommand='newnum')
    sp.add_argument('-k', '--key', default=False, help="Set the number server key, or 'self' for self assignment ")

    #
    sp = asp.add_parser('s3urls', help='Add all of the URLS below an S3 prefix as sources. ')
    sp.set_defaults(subsubcommand='s3urls')
    sp.add_argument('term', type=str, nargs=1, help='S3url with buckets and prefix')

    #
    sp = asp.add_parser('scrape', help='Scrape all of the links from the page references in external_documentation.download')
    sp.set_defaults(subsubcommand='scrape')
    sp.add_argument('-r', '--regex', default=False, help="Select entries where the UR matches the regular expression")

    # Info command
    command_p = sub_cmd.add_parser('info', help='Print information about the bundle')
    command_p.set_defaults(subcommand='info')
    command_p.set_defaults(subcommand='info')
    command_p.add_argument('-s','--schema',  default=False,action="store_true",
                           help='Dump the schema as a CSV. The bundle must have been prepared')
    command_p.add_argument('-d', '--dep', default=False, help='Report information about a dependency')
    command_p.add_argument('-S', '--stats',default=False,action="store_true",  help='Also report column stats')

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
    command_p.add_argument('-f', '--fast', default=False, action="store_true",
                           help='Load the schema faster by not checking for extant columns')

    #
    # Prepare Command
    #
    command_p = sub_cmd.add_parser('prepare', help='Prepare by creating the database and schemas')
    command_p.set_defaults(subcommand='prepare')
    
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
    command_p.add_argument('-r','--rebuild', default=False,action="store_true", help='Rebuild the schema, but dont delete built files')
    command_p.add_argument('-F','--fast', default=False,action="store_true", help='Load the schema faster by not checking for extant columns')
    command_p.add_argument('-f', '--force', default=False, action="store_true",
                           help='Force build. ( --clean is usually preferred ) ')
    #
    # Build Command
    #
    command_p = sub_cmd.add_parser('build', help='Build the data bundle and partitions')
    command_p.set_defaults(subcommand='build')
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
    command_p.add_argument('-f', '--force', default=False, action="store_true", help='Force build. ( --clean is usually preferred ) ')
    command_p.add_argument('-i', '--install', default=False, action="store_true", help='Install after building')
    command_p.add_argument('-o','--opt', action='append', help='Set options for the build phase')
    
    #
    # Update Command
    #
    command_p = sub_cmd.add_parser('update', help='Build the data bundle and partitions from an earlier version')
    command_p.set_defaults(subcommand='update')
    command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')


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
    import json
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
            from ambry.library.database import ROOT_CONFIG_NAME_V
            root_db_created = b.database.get_config_value(ROOT_CONFIG_NAME_V, 'process', 'dbcreated')

            b.log('Created   : ' + process.get('dbcreated', root_db_created.value))
            b.log('Prepared  : ' + process.get('prepared', ''))
            b.log('Built     : ' + process.get('built', ''))

            if process.get('buildtime', False):
                b.log('Build time: ' + str(round(float(process['buildtime']), 2)) + 's')

            b.log("Parts: {}".format(b.partitions.count))

            b.log("---- Partitions ---")
            for i, partition in enumerate(b.partitions):
                b.log("    "+partition.identity.sname)

                if i > 10:
                    b.log("    ... and {} more".format(b.partitions.count - 10))
                    break



        deps = None
        if b.metadata.dependencies:
            # For source bundles
            deps = b.metadata.dependencies.items()
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

        if args.stats:
            for p in b.partitions.all:
                b.log("--- Stats for {}: ".format(p.identity))
                b.log("{:20.20s} {:>7s} {:>7s} {:s}".format("Col name", "Count", 'Uniq', 'Sample Values'))
                for col_name, s in p.stats.__dict__.items():

                    b.log("{:20.20s} {:7d} {:7d} {:s}".format(col_name, s.count, s.nuniques, ','.join(s.uvalues.keys()[:5])))




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
    r =  b.do_build()

    # Closing is generally important. In this case, if the bundle isn't closed, and the bundle is installed below,
    # the config table won't have metadata. No idea why.
    b.close()

    if args.install:
        bundle_install(args, b, st, rc)

    return r

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
    import sys

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

    # Install the python directory for Ambry builds so we can use bundle defined imports
    python_dir = b.config.python_dir()

    if python_dir and python_dir not in sys.path:
        sys.path.append(python_dir)

    r =  f(*args.args)

    print "RETURN: ", r



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
        from datetime import datetime
        identity = b.identity

        # Get the latest installed version of this dataset
        prior_ident = b.library.resolve(b.identity.name)

        if prior_ident:
            prior_version = prior_ident.on.revision
        else:
            prior_version = identity.on.revision

        # If the source bundle is already incremented past the installed versions
        # use that instead.
        if b.identity.on.revision > prior_version:
            prior_version = b.identity.on.revision
            b.close()

        b.clean()
        b.prepare()
        b.close()

        # Now, update this version to be one more.
        ident = b.identity

        identity.on.revision = prior_version + 1

        identity = Identity.from_dict(identity.ident_dict)

        b.update_configuration(identity=identity)

        # Create a new revision entry
        md = b.metadata
        md.load_all()
        md.versions[identity.on.revision] = {
            'description': raw_input("Revision Description: "),
            'version': md.identity.version,
            'date': datetime.now().isoformat()
        }

        md.write_to_dir()

        print identity.fqname




    elif args.subsubcommand == 's3urls':
        return bundle_config_s3urls(args, b, st, rc)

    elif args.subsubcommand == 'newnum':

        from ..identity import NumberServer
        from requests.exceptions import HTTPError
        from ..identity import DatasetNumber, Identity

        nsconfig = rc.service('numbers')

        if args.key:
            nsconfig['key'] = args.key

        ns = NumberServer(**nsconfig)

        d = b.identity.dict

        if args.key in ('rand', 'self'):
            d['id'] = str(DatasetNumber())

        else:
            try:
                d['id'] = str(ns.next())
                prt("Got number from number server: {}".format(d['id']))
            except HTTPError as e:
                warn("Failed to get number from number server. Config = {}: {}".format(nsconfig, e.message))
                warn(
                    "Using self-generated number. There is no problem with this, but they are longer than centrally generated numbers.")
                d['id'] = str(DatasetNumber())

        ident = Identity.from_dict(d)

        b.metadata.identity = ident.ident_dict
        b.metadata.names = ident.names_dict
        b.metadata.write_to_dir(write_all=True)

        prt("New object number: {}".format(ident.id_))

    elif args.subsubcommand == 'scrape':
        return bundle_config_scrape(args, b, st, rc)

    else:
        err("Unknown subsubcommand for 'config' subcommand: {}".format(args))


def bundle_config_scrape(args, b, st, rc):

    from bs4 import BeautifulSoup
    import urllib2, urlparse
    import os

    page_url = b.metadata.external_documentation.download.url

    if not page_url:
        page_url = b.metadata.external_documentation.dataset.url

    if not page_url:
        fatal("Didn't get URL in either the external_documentation.download nor external_documentation.dataset config ")

    parts = list(urlparse.urlsplit(page_url))

    parts[2] = ''
    root_url = urlparse.urlunsplit(parts)

    html_page = urllib2.urlopen(page_url)
    soup = BeautifulSoup(html_page)

    d = dict(external_documentation={}, sources = {} )

    for link in soup.findAll('a'):


        if not link:
            continue;

        if link.string:
            text = str(link.string.encode('ascii','ignore'))
        else:
            text = 'None'

        url = link.get('href')

        if not url:
            continue

        if 'javascript' in url:
            continue;

        orig_url = url

        if url.startswith('http'):
            pass
        elif url.startswith('/'):
            url = os.path.join(root_url, url)
        else:
            url = os.path.join(page_url, url)

        base = os.path.basename(url)

        if '#' in base:
            continue

        try:
            fn, ext = base.split('.',1)
        except ValueError:
            fn = base
            ext = 'html'

        if ext.lower() in ('zip', 'csv','xls','xlsx','xlsm', 'txt'): # xlsm is a bug that adss 'm' to the end of the url. No idea.
            d['sources'][fn] = dict(
                url = url,
                description = text
            )
        elif ext.lower() in ('pdf','html'):
            d['external_documentation'][fn] = dict(
                url=url,
                description=text,
                title=text
            )
        else:

            pass


    import yaml
    print yaml.dump(d, default_flow_style=False)



def bundle_config_s3urls(args, b, st, rc):
    from ..cache import new_cache, parse_cache_string
    import urllib
    import os, binascii


    cache = new_cache(urllib.unquote_plus(args.term[0]), run_config=rc)

    def has_url(url):
        for k,v in b.metadata.sources.items():

            if v and v.url == url:
                return True

        else:
            return False


    for e, v in cache.list().items():

        url = cache.s3path(e)
        if not has_url(url):
            rand_name = binascii.b2a_hex(os.urandom(6))
            b.metadata.sources[rand_name].url = url
            prt("Adding: {}".format(url))

    b.metadata.write_to_dir()

def bundle_repopulate(args, b, st, rc):
    return b.repopulate()


