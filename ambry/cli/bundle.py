# coding: utf-8
"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""

from ..cli import prt, fatal, warn, err
from ..orm import File

def bundle_command(args, rc):

    from ..library import new_library
    from . import global_logger
    l = new_library(rc)
    l.logger = global_logger

    if args.debug:
        from ..util import debug
        warn('Entering debug mode. Send USR1 signal (kill -USR1 ) to break to interactive prompt')
        debug.listen()

    globals()['bundle_' + args.subcommand](args, l, rc)

def get_bundle_ref(args,l):
    """ Use a variety of methods to determine which bundle to use

    :param args:
    :return:
    """
    import os

    cwd_bundle = os.path.join(os.getcwd(), 'bundle.yaml')
    if os.path.exists(cwd_bundle):
        import yaml
        with open(cwd_bundle) as f:
            config =  yaml.load(f)
            id_ = config['identity']['id']
            return (id_, 'directory')

    try:
        if args.term:
         return (args.term, 'argument')
    except AttributeError:
        pass

    if args.id:
        return (args.id, '-i argument')
    elif 'AMBRY_BUNDLE' in os.environ:
        return (os.environ['AMBRY_BUNDLE'], 'environment')
    else:
        history = l.edit_history()

        if history:
            return (history[0].d_vid, 'history')

    return None, None

def using_bundle(args,l):

    ref, frm = get_bundle_ref(args,l)

    prt('Using bundle ref {}, referenced from {}'.format(ref, frm))

    b = l.bundle(ref)

    return b

def bundle_parser(cmd):
    import multiprocessing
    import argparse

    from ambry.bundle.files import BuildSourceFile

    parser = cmd.add_parser('bundle', help='Manage bundle files')
    parser.set_defaults(command='bundle')

    parser.add_argument('-i', '--id', required=False, help='Bundle ID')
    parser.add_argument('-D', '--debug', required=False, default=False, action="store_true",
                        help='URS1 signal will break to interactive prompt')
    parser.add_argument('-t', '--test', default=False, action="store_true",
                        help='Enable bundle-specific test behaviour')
    parser.add_argument('-m', '--multi', type=int, nargs='?', default=1, const=multiprocessing.cpu_count(),
                        help='Run the build process on multiple processors, if the  method supports it')

    sub_cmd = parser.add_subparsers(title='commands', help='command help')

    sp = sub_cmd.add_parser('new', help='Create a new bundle')
    sp.set_defaults(subcommand='new')
    sp.set_defaults(revision=1)  # Needed in Identity.name_parts
    sp.add_argument('-s', '--source', required=True, help='Source, usually a domain name')
    sp.add_argument('-d', '--dataset', required=True, help='Name of the dataset')
    sp.add_argument('-b', '--subset', default=None, help='Name of the subset')
    sp.add_argument('-t', '--time', default=None, help='Time period. Use ISO Time intervals where possible. ')
    sp.add_argument('-p', '--space', default=None, help='Spatial extent name')
    sp.add_argument('-v', '--variation', default=None, help='Name of the variation')
    sp.add_argument('-c', '--creator', required=False, help='Id of the creator')
    sp.add_argument('-n', '--dryrun', action="store_true", default=False, help='Dry run')
    sp.add_argument('-k', '--key', help='Number server key. Use \'self\' for a random, self-generated key.')
    sp.add_argument('args', nargs=argparse.REMAINDER)  # Get everything else.

    #
    # Config sub commands

    command_p = sub_cmd.add_parser('config', help='Operations on the bundle configuration file')
    command_p.set_defaults(subcommand='config')

    asp = command_p.add_subparsers(title='Config subcommands', help='Subcommand for operations on a bundle file')

    # config rewrite
    sp = asp.add_parser('rewrite', help='Re-write the bundle file, updating the formatting')
    sp.set_defaults(subsubcommand='rewrite')

    # config doc
    sp = asp.add_parser('doc', help='Display some of the bundle documentation')
    sp.set_defaults(subsubcommand='doc')


    # config incver
    sp = asp.add_parser('incver', help='Increment the version number')
    sp.set_defaults(subsubcommand='incver')
    sp.add_argument('-m', '--message', default=False, help="Message ")

    # config newnum
    sp = asp.add_parser('newnum', help='Get a new dataset number')
    sp.set_defaults(subsubcommand='newnum')
    sp.add_argument('-k', '--key', default=False, help="Set the number server key, or 'self' for self assignment ")

    # config scrape
    sp = asp.add_parser('scrape',
                        help='Scrape all of the links from the page references in external_documentation.download')
    sp.set_defaults(subsubcommand='scrape')
    sp.add_argument('-r', '--regex', default=False, help="Select entries where the UR matches the regular expression")


    # config doc
    command_p = sub_cmd.add_parser('dump', help="Dump records from the bundle database")
    command_p.set_defaults(subcommand='dump')
    group = command_p.add_mutually_exclusive_group()
    group.add_argument('-c', '--configf', default=False, action="store_const", const = 'configs', dest='table',
                            help='Dump configs')
    group.add_argument('-f', '--files', default=False, action="store_const", const='files', dest='table',
                            help='Dump files')
    group.add_argument('-s', '--sources', default=False, action="store_const", const='datasources', dest='table',
                       help='Dump sources')
    group.add_argument('-T', '--source_tables', default=False, action="store_const", const='sourcetables', dest='table',
                       help='Dump source tables')
    group.add_argument('-p', '--partitions', default=False, action="store_const", const='partitions', dest='partitions',
                       help='Dump partitions')
    command_p.add_argument('term', nargs='?', type=str, help='Bundle reference')

    # Info command
    command_p = sub_cmd.add_parser('info', help='Print information about the bundle')
    command_p.set_defaults(subcommand='info')
    command_p.add_argument('-w', '--which', default=False, action="store_true",
                           help='Report the reference of the bundles that will be accessed by other commands')
    command_p.add_argument('-s', '--schema', default=False, action="store_true",
                           help='Dump the schema as a CSV. The bundle must have been prepared')
    command_p.add_argument('-d', '--dep', default=False, help='Report information about a dependency')
    command_p.add_argument('-S', '--stats', default=False, action="store_true",
                           help='Also report column stats for partitions')
    command_p.add_argument('-P', '--partitions', default=False, action="store_true",
                           help='Also report partition details')

    command_p.add_argument('term', nargs='?', type=str, help='Bundle source directory or file')

    #
    # Sync Command
    #
    command_p = sub_cmd.add_parser('sync', help='Sync with a source representation')
    command_p.set_defaults(subcommand='sync')
    group = command_p.add_mutually_exclusive_group()
    group.add_argument('-s', '--from-source', default=False, action="store_const",
                            const = BuildSourceFile.SYNC_DIR.FILE_TO_RECORD, dest='sync_dir',
                            help='Force sync from source to database')
    group.add_argument('-d', '--from-database', default=False, action="store_const",
                            const=BuildSourceFile.SYNC_DIR.RECORD_TO_FILE, dest='sync_dir',
                            help='Source sync from database to source')
    command_p.add_argument('term', nargs='?', type=str, help='Bundle reference')


    #
    # Clean Command
    #
    command_p = sub_cmd.add_parser('clean', help='Return bundle to state before build, prepare and extracts')
    command_p.set_defaults(subcommand='clean')

    #
    # Download Command
    #
    command_p = sub_cmd.add_parser('download', help='Download all of the soruce files and referenced bundles')
    command_p.set_defaults(subcommand='download')

    #
    # Meta Command
    #
    command_p = sub_cmd.add_parser('meta', help='Build or install metadata')
    command_p.set_defaults(subcommand='meta')

    command_p.add_argument('-c', '--clean', default=False, action="store_true", help='Clean first')
    command_p.add_argument('-f', '--fast', default=False, action="store_true",
                           help='Load the schema faster by not checking for extant columns')

    #
    # Prepare Command
    #
    command_p = sub_cmd.add_parser('prepare', help='Prepare by creating the database and schemas')
    command_p.set_defaults(subcommand='prepare')

    command_p.add_argument('-c', '--clean', default=False, action="store_true", help='Clean first')
    command_p.add_argument('-s', '--sync', default=False, action="store_true",
                           help='Syncrhonize before building')

    command_p.add_argument('term', nargs='?', type=str, help='bundle reference')

    #
    # Build Command
    #
    command_p = sub_cmd.add_parser('build', help='Build the data bundle and partitions')
    command_p.set_defaults(subcommand='build')
    command_p.add_argument('-s', '--sync', default=False, action="store_true", help='Sync with build source files')
    command_p.add_argument('-c', '--clean', default=False, action="store_true", help='Clean first')
    command_p.add_argument('-f', '--force', default=False, action="store_true",
                           help='Force build. ( --clean is usually preferred ) ')
    command_p.add_argument('-i', '--install', default=False, action="store_true",
                           help='Install after building')
    command_p.add_argument('-o', '--opt', action='append', help='Set options for the build phase')

    #
    # Update Command
    #
    command_p = sub_cmd.add_parser('update', help='Build the data bundle and partitions from an earlier version')
    command_p.set_defaults(subcommand='update')
    command_p.add_argument('-c', '--clean', default=False, action="store_true", help='Clean first')
    command_p.add_argument('-f', '--force', default=False, action="store_true",
                           help='Force build. ( --clean is usually preferred ) ')

    #
    # Install Command
    #
    command_p = sub_cmd.add_parser('install', help='Install bundles and partitions to the library')
    command_p.set_defaults(subcommand='install')
    command_p.add_argument('-c', '--clean', default=False, action="store_true", help='Clean first')
    command_p.add_argument('-l','--library',help='Name of the library, defined in the config file')
    command_p.add_argument('-f' '--force', default=False,action="store_true",help='Force storing the file')

    #
    # run Command
    #

    command_p = sub_cmd.add_parser('run', help='Run a method on the bundle')
    command_p.set_defaults(subcommand='run')
    command_p.add_argument('method', metavar='Method', type=str, help='Name of the method to run')
    command_p.add_argument('args', nargs='*', type=str, help='additional arguments')

    #
    # repopulate
    #
    command_p = sub_cmd.add_parser('repopulate',
                                   help='Load data previously submitted to the library back into the build dir')
    command_p.set_defaults(subcommand='repopulate')


    command_p = sub_cmd.add_parser('edit', help='Edit a bundle file')
    command_p.set_defaults(subcommand='edit')

    group = command_p.add_mutually_exclusive_group()
    group.add_argument('-b', '--bundle',  default=False, action='store_const', const=File.BSFILE.BUILD, dest='file_const',
                       help="Edit the code file")
    group.add_argument('-m', '--meta', default=False, action='store_const', const=File.BSFILE.META, dest='file_const',
                       help="Edit the metadata")
    group.add_argument('-c', '--colmap', default=False, action='store_const', const=File.BSFILE.COLMAP, dest='file_const',
                       help="Edit the column map")
    group.add_argument('-r', '--sources', default=False, action='store_const', const=File.BSFILE.SOURCES, dest='file_const',
                       help="Edit the sources")
    group.add_argument('-s', '--schema', default=False, action='store_const', const=File.BSFILE.SCHEMA, dest='file_const',
                   help="Edit the schema")
    group.add_argument('-d', '--documentation', default=False, action='store_const', const=File.BSFILE.DOC, dest='file_const',
                   help="Edit the documentation")
    command_p.add_argument('term', nargs='?', type=str, help='bundle reference')


    command_p = sub_cmd.add_parser('import', help='Import a source bundle. ')
    command_p.set_defaults(subcommand='import')
    command_p.add_argument('source', nargs='?', type=str, help='Bundle source directory or file')

    command_p = sub_cmd.add_parser('export', help='Export a source bundle. ')
    command_p.set_defaults(subcommand='export')
    command_p.add_argument('-a', '--append', default=False, action="store_true", help='Append the source and bundle name to the path')
    command_p.add_argument('-d', '--defaults', default=False, action="store_true",
                           help='Write default files when there is no other content for file. ')

    command_p.add_argument('source', nargs='?', type=str, help='Bundle source directory or file')

def bundle_info(args, l, rc):
    from  textwrap import wrap
    from ambry.util.datestimes import compress_years
    from tabulate import tabulate

    ref, frm = get_bundle_ref(args,l)

    if args.which:
        prt('Will use bundle ref {}, referenced from {}'.format(ref, frm))
        return

    b = l.bundle(ref)

    info = [list(), list()]
    def inf(column,k,v):
        info[column].append((k, v))

    def color(v):
        return "\033[1;34m{}\033[0m".format(v)

    def trunc(v):
        return v[:25] + (v[25:] and '..')

    def join(*sets):

        l = max(len(x) for x in sets)

        for i in range(l):
            row = []
            for set in sets:
                try:
                    row += [color(set[i][0]), set[i][1]]
                except IndexError:
                    row += [None, None]
            yield row


    inf(0,'Title', trunc(b.metadata.about.title))
    inf(0,'Summary', trunc(b.metadata.about.summary))
    inf(0,"VID", b.identity.vid)
    inf(0,"VName", b.identity.vname)

    inf(1, 'Build State', b.dataset.config.build.state.current)
    try:
        inf(1, 'Geo cov', str(list(b.metadata.coverage.geo)))
        inf(1, 'Grain cov', str(list(b.metadata.coverage.grain)))
        inf(1, 'Time cov', compress_years(b.metadata.coverage.time))
    except KeyError:
        pass


    print '----'
    print tabulate(join(*info), tablefmt='plain')

    info = [list()]
    inf(0, "Source FS",str(b.source_fs))
    inf(0, "Build  FS", str(b.build_fs))

    print '----'
    print tabulate(join(info[0]), tablefmt='plain')

    from ambry.bundle.etl.stats import text_hist
    from textwrap import wrap
    from terminaltables import  SingleTable, AsciiTable

    for p in b.partitions:
        rows = ['Column LOM Count Uniques Values'.split()]
        for k, v in  p.stats_dict.items():

            rows.append([
                str(k), str(v.lom), str(v.count), str(v.nuniques),
                text_hist(int(x) for x in v.hist) if v.lom == 'i' else (
                    '\n'.join(wrap(', '.join(sorted(str(x) for x in v.uvalues.keys()[:10])), 50)))
            ])

        #print tabulate(row, tablefmt='plain')
        print SingleTable(rows, title="Stats for "+str(p.identity.name)).table

    if False:

        wrapper = textwrap.TextWrapper()

        lprt("Stats", '')
        wprt('Col Name', "{:>7s} {:>7s} {:>10s} {:70s}".format(
            "Count", 'Uniq', 'Mean', 'Sample Values'))
        for col_name, s in p.stats.__dict__.items():

            if s.uvalues:
                vals = (u'\n' + u' ' * 49).join(wrapper.wrap(u','.join(s.uvalues.keys()[:5])))

            wprt(col_name, u"{:>7s} {:>7s} {:>10s} {:70s}".format(
                str(s.count) if s.count else '',
                str(s.nuniques) if s.nuniques else '',
                '{:10.2e}'.format(s.mean) if s.mean else '',
                vals
            ))

    return

    if False:

        if b.database.exists():

            process = b.get_value_group('process')

            # Older bundles have the dbcreated values assigned to the root dataset vid ('a0/001')
            # instead of the bundles dataset vid
            from ambry.library.database import ROOT_CONFIG_NAME_V
            root_db_created = b.database.get_config_value(ROOT_CONFIG_NAME_V, 'process', 'dbcreated')

            lprt('Created', process.get('dbcreated', root_db_created.value))
            lprt('Prepared', process.get('prepared', ''))
            lprt('Built ', process.get('built', ''))

            if process.get('buildtime', False):
                lprt('Build time', str(round(float(process['buildtime']), 2)) +'s')

            lprt("Parts", b.partitions.count)

            hprt("Partitions")
            for i, partition in enumerate(b.partitions):
                lprt(indent,partition.identity.sname)

                if i > 6:
                    lprt(indent, "... and {} more".format(b.partitions.count - 6))
                    break

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
            hprt("Dependencies")
            for k, v in deps:
                lprt(k,v)

        if args.stats or args.partitions:
            for p in b.partitions.all:
                hprt("Partition {}".format(p.identity))
                if args.partitions:
                    p.record.data

                    def bl(k, v):
                        lprt(k, p.record.data.get(v, ''))

                    lprt('# Rows', p.record.count)
                    bl('g cov', 'geo_coverage')
                    bl('g grain', 'geo_grain')
                    bl('t cov', 'time_coverage')


def bundle_clean(args, l, rc):
    from ambry.bundle import Bundle
    b = using_bundle(args, l).cast_to_build_subclass()
    b.do_clean()
    b.set_last_access(Bundle.STATES.NEW)

def bundle_download(args, l, rc):
    from ambry.bundle import Bundle
    b = using_bundle(args, l).cast_to_build_subclass()
    b.download()
    b.set_last_access(Bundle.STATES.DOWNLOADED)

def bundle_sync(args, l, rc):
    from ambry.bundle import Bundle
    from tabulate import tabulate

    b = using_bundle(args,l).cast_to_build_subclass()

    prt("Bundle source filesystem: {}".format(b.source_fs))
    prt("Sync direction: {}".format(args.sync_dir if args.sync_dir else 'latest'))

    syncs =  b.do_sync(args.sync_dir if args.sync_dir else None)

    print tabulate(syncs, headers="Key Direction".split())

    b.set_last_access(Bundle.STATES.SYNCED)

def bundle_meta(args, l, rc):
    from ambry.bundle import Bundle

    b = using_bundle(args, l).cast_to_meta_subclass()

    if args.clean:
        b.do_clean()

    b.do_sync()

    # Get the bundle again, to handle the case when the sync updated bundle.py or meta.py
    b = using_bundle(args, l).cast_to_meta_subclass()
    b.do_meta()
    b.set_last_access(Bundle.STATES.META)


def bundle_prepare(args, l, rc):
    from ambry.bundle import Bundle

    if args.clean or args.sync:
        b = using_bundle(args, l).cast_to_build_subclass()
        if args.clean:
            b.do_clean()
        if args.sync:
            b.do_sync()

    b = using_bundle(args, l).cast_to_build_subclass()
    b.do_prepare()
    b.set_last_access(Bundle.STATES.PREPARED)


def bundle_build(args, l, rc):
    from ambry.bundle import Bundle

    if args.clean or args.sync:
        b = using_bundle(args, l).cast_to_build_subclass()
        if args.clean:
            b.do_clean()
        if args.sync:
            b.do_sync()

    b = using_bundle(args, l).cast_to_build_subclass()

    if args.clean:
        if not b.do_clean():
            return False

        if not b.do_prepare():
            return False

    b.do_build(force = args.force)
    b.set_last_access(Bundle.STATES.BUILT)

def bundle_install(args, b, st, rc):
    raise NotImplementedError()

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
    raise NotImplementedError()
    import sys

    #
    # Run a method on the bundle. Can be used for testing and development.
    try:
        f = getattr(b, str(args.method))
    except AttributeError as e:
        b.error("Could not find method named '{}': {} ".format(args.method, e))
        b.error("Available methods : {} ".format(dir(b)))

        return

    if not callable(f):
        raise TypeError(
            "Got object for name '{}', but it isn't a function".format(
                args.method))

    # Install the python directory for Ambry builds so we can use bundle
    # defined imports
    python_dir = b.config.python_dir()

    if python_dir and python_dir not in sys.path:
        sys.path.append(python_dir)

    r = f(*args.args)

    print "RETURN: ", r


def bundle_update(args, b, st, rc):
    raise NotImplementedError()
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


def bundle_config(args, l, rc):



    raise NotImplementedError()

    if args.subsubcommand == 'rewrite':
        b.log("Rewriting the config file")
        with b.session:
            b.update_configuration()
    elif args.subsubcommand == 'dump':
        print b.config.dump()

    elif args.subsubcommand == 'schema':
        print b.schema.as_markdown()

    elif args.subsubcommand == 'incver':

        description = raw_input("Revision Description: ")

        identity = b.increment_revision(description)

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
                warn("Using self-generated number. "
                     "There is no problem with this, but they are longer than centrally generated numbers.")
                d['id'] = str(DatasetNumber())

        ident = Identity.from_dict(d)

        b.metadata.identity = ident.ident_dict
        b.metadata.names = ident.names_dict
        b.metadata.write_to_dir(write_all=True)

        prt("New object number: {}".format(ident.id_))

    elif args.subsubcommand == 'scrape':
        return bundle_config_scrape(args, b, st, rc)

    elif args.subsubcommand == 'doc':
        f = "{:10s} {}"
        prt(f, 'title', b.metadata.about.title)

    else:
        err("Unknown subsubcommand for 'config' subcommand: {}".format(args))


def bundle_dump(args, l, rc):
    import tabulate
    import datetime

    ref, frm = get_bundle_ref(args,l)

    b = l.bundle(ref)

    prt("Dumping configs for {}\n".format(b.identity.fqname))

    def trunc(v,l):
        return v[:l] + (v[l:] and '..')

    if args.table == 'configs' or not args.table:

        records = []
        headers = 'Id Type Group Parent Key Value Modified'.split()
        for row in b.dataset.configs:
            records.append((
                row.id,
                row.type,
                row.group,
                row.parent_id,
                row.key,
                trunc(str(row.value), 22),
                datetime.datetime.fromtimestamp(row.modified).isoformat(),)
            )

        records = sorted(records, key=lambda row: row[0])

    elif args.table == 'files':

        records = []
        headers = 'Path Major Minor State Size Modified'.split()
        for row in b.dataset.files:
            records.append((
                row.path,
                row.major_type,
                row.minor_type,
                row.state,
                row.size,
                datetime.datetime.fromtimestamp(float(row.modified)).isoformat() if row.modified else ''
                )
            )
        records = sorted(records, key=lambda row: (row[0], row[1], row[2]) )

    elif args.table == 'partitions':

        records = []
        headers = 'Name Count'.split()
        for row in b.dataset.partitions:
            records.append((
                row.name,
                row.stats_dict.id.count
            )
            )
        records = sorted(records, key=lambda row: (row[0]))

    elif args.table == 'datasources':

        records = []
        for i, row in enumerate(b.dataset.sources):
            if not records:
                records.append(row.dict.keys())

            records.append(row.dict.values())

        # Transpose, remove empty columns, transpose back
        records = zip(*[row for row in zip(*records) if bool(filter(bool, row[1:]))])

        if records:
            headers, records = records[0], records[1:]
        else:
            headers, records = [], []

        records =  sorted(records, key=lambda row: (row[0]))

    elif args.table == 'sourcetables':


        records = []
        for t in b.dataset.source_tables:
            for c in t.columns:
                if not records:
                    records.append(c.row.keys())

                records.append(c.row.values())

        if records:
            headers, records = records[0], records[1:]
        headers = []

    print tabulate.tabulate(records, headers = headers)

def bundle_config_scrape(args, b, st, rc):

    raise NotImplementedError

    from bs4 import BeautifulSoup
    import urllib2
    import urlparse
    import os

    page_url = b.metadata.external_documentation.download.url

    if not page_url:
        page_url = b.metadata.external_documentation.dataset.url

    if not page_url:
        fatal(
            "Didn't get URL in either the external_documentation.download nor external_documentation.dataset config ")

    parts = list(urlparse.urlsplit(page_url))

    parts[2] = ''
    root_url = urlparse.urlunsplit(parts)

    html_page = urllib2.urlopen(page_url)
    soup = BeautifulSoup(html_page)

    d = dict(external_documentation={}, sources={})

    for link in soup.findAll('a'):

        if not link:
            continue

        if link.string:
            text = str(link.string.encode('ascii', 'ignore'))
        else:
            text = 'None'

        url = link.get('href')

        if not url:
            continue

        if 'javascript' in url:
            continue

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
            fn, ext = base.split('.', 1)
        except ValueError:
            fn = base
            ext = 'html'

        # xlsm is a bug that adss 'm' to the end of the url. No idea.
        if ext.lower() in ('zip', 'csv', 'xls', 'xlsx', 'xlsm', 'txt'):
            d['sources'][fn] = dict(
                url=url,
                description=text
            )
        elif ext.lower() in ('pdf', 'html'):
            d['external_documentation'][fn] = dict(
                url=url,
                description=text,
                title=text
            )
        else:

            pass

    import yaml
    print yaml.dump(d, default_flow_style=False)




def bundle_repopulate(args, b, st, rc):
    raise NotImplementedError()
    return b.repopulate()



def bundle_new(args, l, rc):
    """Clone one or more registered source packages ( via sync ) into the
    source directory."""

    from ambry.orm.exc import ConflictError


    d = dict(
         dataset= args.dataset,
         revision=args.revision,
         source=args.source,
         bspace=args.space,
         subset=args.subset,
         btime=args.time,
         variation=args.variation)

    try:
        ambry_account = rc.group('accounts').get('ambry', {})
    except:
        ambry_account = None

    if not ambry_account:
        fatal("Failed to get an accounts.ambry entry from the configuration. ( It's usually in {}. ) ".format(
                rc.USER_ACCOUNTS))

    if not ambry_account.get('name') or not ambry_account.get('email'):
        from ambry.run import RunConfig as rc

        fatal("Must set accounts.ambry.email and accounts.ambry.name, usually in {}".format(rc.USER_ACCOUNTS))

    try:
        b = l.new_bundle(**d)

    except ConflictError:
        fatal("Can't create dataset; one with a conflicting name already exists")

    print b.identity.fqname


def bundle_import(args, l, rc):
    from fs.opener import fsopendir
    import yaml
    from ambry.orm.exc import NotFoundError

    if args.source:
        source_dir = args.source
    else:
        import os
        source_dir = os.getcwd()

    fs = fsopendir(source_dir)

    config = yaml.load(fs.getcontents('bundle.yaml'))

    bid =  config['identity']['id']

    try:
        b = l.bundle(bid)
    except NotFoundError:
        b = l.new_from_bundle_config(config)

    b.set_file_system(source_url=args.source)

    b.sync()

    prt("Loaded bundle: {}".format(b.identity.fqname))

def bundle_export(args, l, rc):
    from fs.opener import fsopendir
    import os

    b = using_bundle(args,l)

    if args.source:
        source_dir = args.source
    else:
        import os
        source_dir = os.getcwd()

    if args.append:
        source_dir = os.path.join(source_dir,b.identity.source_path)

    fs = fsopendir(source_dir, create_dir = True)

    b.set_file_system(source_url=source_dir)

    b.sync(force='rtf', defaults = args.defaults)

    prt("Exported bundle: {}".format(source_dir))

file_const_map = dict(
    b=File.BSFILE.BUILD,
    d=File.BSFILE.DOC,
    m=File.BSFILE.META,
    s=File.BSFILE.SCHEMA,
    c=File.BSFILE.COLMAP,
    r=File.BSFILE.SOURCES)


def bundle_edit(args, l, rc):
    import sys, os
    import subprocess

    from ..util import getch
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    from Queue import Queue
    import threading

    b = using_bundle(args,l)

    prt("Found bundle {}".format(b.identity.fqname))

    EDITOR = os.environ.get('EDITOR', 'vim')  # that easy!

    b.sync()

    prt('Commands: q=quit, {}'.format(  ', '.join( k+'='+v for k,v in file_const_map.items())))

    def edit(const):

        bf = b.build_source_files.file(const)
        bf.prepare_to_edit()

        file_path =  bf.path

        prt("Editing {}".format(file_path))

        _, ext = os.path.splitext(file_path)

        if sys.platform.startswith('darwin'):
            subprocess.call(('open', file_path))
        elif os.name == 'nt':
            os.startfile(file_path)
        elif os.name == 'posix':
            subprocess.call(('xdg-open', file_path))

    if args.file_const:
        edit(args.file_const)

    queue = Queue()
    class EditEventHandler(FileSystemEventHandler):
        def on_modified(self, event):
            queue.put(('change',event.src_path))

    observer = Observer()
    observer.schedule(EditEventHandler(), os.path.dirname(b.source_fs.getsyspath('/')))
    observer.start()

    # Thread to get commands from the user. Using a thread so that the char input can block on input
    # and the main thread can still process change events.
    def get_chars():
        while True:
            char = getch()

            if char == 'q' or ord(char) == 3: # Crtl-c
                queue.put(('quit', None))
                break
            if char in ('p','B'):
                queue.put(('build', char))
            elif char in file_const_map:
                queue.put(('edit', file_const_map[char]))
            else:
                queue.put(('unknown', char))

    get_chars_t = threading.Thread(target=get_chars)
    get_chars_t.start()

    # Look, in this thread, that executed the commands.
    while True:
        try:
            command, arg = queue.get(True)

            # On OS X Terminal, the printing moves the cursor down a lone, but not to the start, so these
            # ANSI sequences fix the cursor positiing. No idea why ...
            print "\033[0G\033[1F"

            if command == 'quit':
                observer.stop()
                break

            elif command == 'edit':
                edit(arg)

            elif command == 'change':
                prt("Changed: {}".format(arg))
                b.sync()

            elif command == 'build':
                bc = b.cast_to_build_subclass()
                if arg == 'p':
                    bc.do_clean()
                    bc.do_prepare()

                elif arg == 'B':
                    bc.do_clean()
                    bc.do_build()

            elif command == 'unknown':
                warn('Unknown command char: {} '.format(arg))
        except Exception as e:
            import traceback
            print(traceback.format_exc())

    observer.join()
    get_chars_t.join()