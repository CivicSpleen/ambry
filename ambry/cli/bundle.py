# coding: utf-8
"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""
from collections import OrderedDict
import os
import sys
import yaml

from fs.opener import fsopendir

from tabulate import tabulate

from ambry.identity import NotObjectNumberError
from ambry.bundle import Bundle
from ambry.orm.exc import NotFoundError

from ..cli import prt, fatal, warn, err
from ..orm import File

def bundle_command(args, rc):

    from ..library import new_library
    from . import global_logger
    from ambry.orm.exc import ConflictError

    l = new_library(rc)
    l.logger = global_logger

    if args.debug:
        from ..util import debug
        warn('Entering debug mode. Send USR1 signal (kill -USR1 ) to break to interactive prompt')
        debug.listen()

    try:
        globals()['bundle_' + args.subcommand](args, l, rc)
    except ConflictError as e:
        fatal(str(e))

def get_bundle_ref(args, l):
    """ Use a variety of methods to determine which bundle to use

    :param args:
    :return:
    """

    if args.id:
        return (args.id, '-i argument')

    try:
        if args.term:
            l.bundle(args.term)  # Exception if not exists
            return (args.term, 'argument')
    except (AttributeError, NotFoundError, NotObjectNumberError):
        pass

    if 'AMBRY_BUNDLE' in os.environ:
        return (os.environ['AMBRY_BUNDLE'], 'environment')

    cwd_bundle = os.path.join(os.getcwd(), 'bundle.yaml')

    if os.path.exists(cwd_bundle):

        with open(cwd_bundle) as f:
            config = yaml.load(f)
            id_ = config['identity']['id']
            return (id_, 'directory')

    history = l.edit_history()

    if history:
        return (history[0].d_vid, 'history')

    return None, None


def using_bundle(args, l, print_loc=True):

    ref, frm = get_bundle_ref(args, l)

    if not ref:
        fatal("Didn't get a bundle ref from the -i option, history, environment or argument")

    if print_loc:
        prt('Using bundle ref {}, referenced from {}'.format(ref, frm))

    b = l.bundle(ref)

    return b


def bundle_parser(cmd):
    import multiprocessing
    import argparse

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
    sp.add_argument('-n', '--dryrun', action="store_true", default=False, help='Dry run')
    sp.add_argument('-k', '--key', default='self',
                    help='Number server key. Use \'self\' for a random, self-generated key.')
    sp.add_argument('args', nargs=argparse.REMAINDER)  # Get everything else.

    #
    # Config sub commands

    command_p = sub_cmd.add_parser('config', help='Operations on the bundle configuration file')
    command_p.set_defaults(subcommand='config')

    asp = command_p.add_subparsers(title='Config subcommands',
                                   help='Subcommand for operations on a bundle file')

    # Dump
    command_p = sub_cmd.add_parser('dump', help="Dump records from the bundle database")
    command_p.set_defaults(subcommand='dump')
    group = command_p.add_mutually_exclusive_group()
    group.add_argument('-c', '--config', default=False, action='store_const', const='configs', dest='table',
                       help='Dump configs')
    group.add_argument('-f', '--files', default=False, action='store_const', const='files', dest='table',
                       help='Dump files')
    group.add_argument('-s', '--sources', default=False, action='store_const',
                       const='datasources', dest='table',
                       help='Dump sources')
    group.add_argument('-T', '--source_tables', default=False, action='store_const',
                       const='sourcetables', dest='table',
                       help='Dump source tables')
    group.add_argument('-p', '--partitions', default=False, action='store_const',
                       const='partitions', dest='table',
                       help='Dump partitions')
    group.add_argument('-t', '--dest_tables', default=False, action='store_const',
                       const='tables', dest='table',
                       help='Dump destination tables')
    command_p.add_argument('term', nargs='?', type=str, help='Bundle reference')

    # Set
    command_p = sub_cmd.add_parser('set', help='Set configuration and state values')
    command_p.set_defaults(subcommand='set')
    group = command_p.add_mutually_exclusive_group()
    group.add_argument('-s', '--state', default=None, help='Set the build state')

    # Info command
    command_p = sub_cmd.add_parser('info', help='Print information about the bundle')
    command_p.set_defaults(subcommand='info')
    command_p.add_argument('-w', '--which', default=False, action='store_true',
                           help='Report the reference of the bundles that will be accessed by other commands')
    command_p.add_argument('-s', '--source_dir', default=False, action='store_true',
                           help='Display the source directory')
    command_p.add_argument('-S', '--stats', default=False, action='store_true',
                           help='Also report column stats for partitions')
    command_p.add_argument('-P', '--partitions', default=False, action='store_true',
                           help='Also report partition details')

    command_p.add_argument('term', nargs='?', type=str, help='Bundle source directory or file')

    #
    # Sync Command
    #
    command_p = sub_cmd.add_parser('sync', help='Sync with a source representation')
    command_p.set_defaults(subcommand='sync')
    command_p.add_argument('term', nargs='?', type=str, help='Bundle reference')

    #
    #     duplicate Command
    #
    command_p = sub_cmd.add_parser('duplicate',
                                   help='Increment a bundles version number and create a new bundle')
    command_p.set_defaults(subcommand='duplicate')

    #
    # Clean Command
    #
    command_p = sub_cmd.add_parser('clean', help='Return bundle to state before build, prepare and extracts')
    command_p.add_argument('-f', '--force', default=False, action='store_true',
                           help='Force cleaning a built or finalized bundle')
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

    command_p.add_argument('-c', '--clean', default=False, action='store_true', help='Clean first')
    command_p.add_argument('-p', '--print_pipe', default=False, action='store_true',
                           help='Print out the pipeline as it runs')

    #
    # Prepare Command
    #
    command_p = sub_cmd.add_parser('prepare', help='Prepare by creating the database and schemas')
    command_p.set_defaults(subcommand='prepare')

    command_p.add_argument('-c', '--clean', default=False, action='store_true', help='Clean first')
    command_p.add_argument('-s', '--sync', default=False, action='store_true',
                           help='Syncrhonize before building')

    command_p.add_argument('term', nargs='?', type=str, help='bundle reference')

    #
    # Build Command
    #
    command_p = sub_cmd.add_parser('build', help='Build the data bundle and partitions')
    command_p.set_defaults(subcommand='build')
    #command_p.add_argument('-s', '--sync', default=False, action='store_true', help='Sync with build source files')
    command_p.add_argument('-c', '--clean', default=False, action='store_true', help='Clean first')
    command_p.add_argument('-f', '--force', default=False, action='store_true',
                           help='Build even built or finalized bundles')

    command_p.add_argument('sources', nargs='*', type=str,
                           help='Sources to run, instead of running all sources')


    #
    # Phase Command
    #
    command_p = sub_cmd.add_parser('phase', help='Run a phase')
    command_p.set_defaults(subcommand='phase')

    command_p.add_argument('-c', '--clean', default=False, action='store_true', help='Clean first')
    command_p.add_argument('-s', '--sync', default=False, action='store_true',
                           help='Syncrhonize before building')

    command_p.add_argument('phase', nargs='?', type=str, help='Name of phase')
    command_p.add_argument('sources', nargs='*', type=str, help='Sources to run, instead of running all sources')

    #
    # Finalize Command
    #
    command_p = sub_cmd.add_parser('finalize', help='Finalize the bundle, preventing further changes')
    command_p.set_defaults(subcommand='finalize')

    #
    # Checkin Command
    #
    command_p = sub_cmd.add_parser('checkin', help='Commit the bundle to the remote store')
    command_p.set_defaults(subcommand='checkin')

    #
    # Update Command
    #
    command_p = sub_cmd.add_parser('update',
                                   help='Build the data bundle and partitions from an earlier version')
    command_p.set_defaults(subcommand='update')
    command_p.add_argument('-c', '--clean', default=False, action='store_true', help='Clean first')
    command_p.add_argument('-f', '--force', default=False, action='store_true',
                           help='Force build. ( --clean is usually preferred ) ')

    #
    # Install Command
    #
    command_p = sub_cmd.add_parser('install', help='Install bundles and partitions to the library')
    command_p.set_defaults(subcommand='install')
    command_p.add_argument('-c', '--clean', default=False, action='store_true', help='Clean first')
    command_p.add_argument('-l', '--library', help='Name of the library, defined in the config file')
    command_p.add_argument('-f' '--force', default=False, action='store_true', help='Force storing the file')

    #
    # run Command
    #

    command_p = sub_cmd.add_parser('run', help='Run a method on the bundle')
    command_p.set_defaults(subcommand='run')
    command_p.add_argument('-c', '--clean', default=False, action='store_true', help='Clean first')
    command_p.add_argument('-f', '--force', default=False, action='store_true',
                           help='Force running on a built or finalized bundle')
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
    group.add_argument('-b', '--bundle',  default=False, action='store_const',
                       const=File.BSFILE.BUILD, dest='file_const',
                       help='Edit the code file')
    group.add_argument('-m', '--meta', default=False, action='store_const',
                       const=File.BSFILE.META, dest='file_const',
                       help='Edit the metadata')
    group.add_argument('-c', '--colmap', default=False, action='store_const',
                       const=File.BSFILE.COLMAP, dest='file_const',
                       help='Edit the column map')
    group.add_argument('-r', '--sources', default=False, action='store_const',
                       const=File.BSFILE.SOURCES, dest='file_const',
                       help='Edit the sources')
    group.add_argument('-s', '--schema', default=False, action='store_const',
                       const=File.BSFILE.SCHEMA, dest='file_const',
                       help='Edit the schema')
    group.add_argument('-d', '--documentation', default=False, action='store_const',
                       const=File.BSFILE.DOC, dest='file_const',
                       help='Edit the documentation')
    command_p.add_argument('term', nargs='?', type=str, help='bundle reference')

    command_p = sub_cmd.add_parser('import', help='Import a source bundle. ')
    command_p.set_defaults(subcommand='import')
    command_p.add_argument('source', nargs='?', type=str, help='Bundle source directory or file')

    command_p = sub_cmd.add_parser('export', help='Export a source bundle. ')
    command_p.set_defaults(subcommand='export')
    command_p.add_argument('-a', '--append', default=False, action='store_true',
                           help='Append the source and bundle name to the path')
    command_p.add_argument('-d', '--defaults', default=False, action='store_true',
                           help='Write default files when there is no other content for file.')
    command_p.add_argument('source', nargs='?', type=str, help='Bundle source directory or file')

    command_p = sub_cmd.add_parser('extract', help='Extract data from a bundle')
    command_p.set_defaults(subcommand='extract')
    command_p.add_argument('-l', '--limit', type=int, default=None, help='Limit on number of rwos per file')
    command_p.add_argument('partition', nargs='?', metavar='partition',  type=str, help='Partition to export')
    command_p.add_argument('directory', nargs='?', metavar='directory', help='Output directory')


def bundle_info(args, l, rc):
    from ambry.util.datestimes import compress_years

    if args.which:
        ref, frm = get_bundle_ref(args, l)
        b = using_bundle(args, l, print_loc=False)
        prt('Will use bundle ref {}, {}, referenced from {}'.format(ref, b.identity.vname, frm))
        return

    b = using_bundle(args, l, print_loc=False)

    b.set_last_access(Bundle.STATES.INFO)

    if args.source_dir:
        print b.source_fs.getsyspath('/')
        return

    def inf(column, k, v):
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

    info = [list()]
    inf(0, 'Title', b.metadata.about.title)
    inf(0, 'Summary', b.metadata.about.summary)
    print tabulate(join(*info), tablefmt='plain')

    info = [list(), list()]

    inf(0, 'VID', b.identity.vid)
    inf(0, 'VName', b.identity.vname)

    inf(1, 'Build State', b.dataset.config.build.state.current)
    try:
        inf(1, 'Geo cov', str(list(b.metadata.coverage.geo)))
        inf(1, 'Grain cov', str(list(b.metadata.coverage.grain)))
        inf(1, 'Time cov', compress_years(b.metadata.coverage.time))
    except KeyError:
        pass

    print tabulate(join(*info), tablefmt='plain')

    info = [list()]

    inf(0, 'Build  FS', str(b.build_fs))
    inf(0, 'Source FS', str(b.source_fs))

    print tabulate(join(info[0]), tablefmt='plain')

    # DIsplay info about a partition, if one was provided
    if args.term:
        try:
            p = b.partition(args.term)

            info = [list(), list()]

            inf(0, 'ID', p.identity.id_)
            inf(0, 'VID', p.identity.vid)
            inf(1, 'Name', p.identity.name)
            inf(1, 'VName', p.identity.vname)

            inf(0, 'Location', p.location)
            inf(1, 'State', p.state)

            print '\nPartition'
            print tabulate(join(*info), tablefmt='plain')

        except (NotFoundError, AttributeError):
            pass

    if args.stats:

        from ambry.etl.stats import text_hist
        from textwrap import wrap
        from terminaltables import SingleTable

        for p in b.partitions:
            rows = ['Column LOM Count Uniques Values'.split()]
            keys = [k for k, v in p.stats_dict.items()]
            d = p.stats_dict
            for c in p.table.columns:
                if c.name in keys:
                    k = c.name
                    v = d[c.name]

                    rows.append([
                        str(k), str(v.lom), str(v.count), str(v.nuniques),
                        text_hist(int(x) for x in v.hist) if v.lom == 'i' else (
                            '\n'.join(wrap(', '.join(sorted(str(x) for x in v.uvalues.keys()[:10])), 50)))
                    ])

            #print tabulate(row, tablefmt='plain')
            print SingleTable(rows, title='Stats for ' + str(p.identity.name)).table

    elif args.partitions:

        rows = []
        for p in b.partitions:
            rows.append([p.vid, p.vname, p.table.name])
        print '\nPartitions'
        print tabulate(rows)


def check_built(b):
    """Exit if the bundle is built or finalized"""
    if b.is_built or b.is_finalized:
        fatal("Can't perform operation; locked state = '{}'. Call bambry clean explicity".format(b.state))


def bundle_duplicate(args, l, rc):

    b = using_bundle(args, l)

    if not b.is_finalized:
        fatal("Can't increments a bundle unless it is finalized")

    nb = l.duplicate(b)

    nb.set_last_access(Bundle.STATES.NEW)

    prt("New Bundle: {} ".format(nb.identity.vname))


def bundle_finalize(args, l, rc):
    b = using_bundle(args, l)
    b.finalize()
    b.set_last_access(Bundle.STATES.FINALIZED)


def bundle_clean(args, l, rc):
    b = using_bundle(args, l).cast_to_subclass()
    b.clean_all(force=args.force)
    b.set_last_access(Bundle.STATES.NEW)


def bundle_download(args, l, rc):
    b = using_bundle(args, l).cast_to__subclass()
    b.download()
    b.set_last_access(Bundle.STATES.DOWNLOADED)


def bundle_sync(args, l, rc):
    from ambry.dbexceptions import BundleError

    b = using_bundle(args, l)

    try:
        b = b.cast_to_subclass()
    except BundleError as e:
        err("Failed to load bundle code file: {}".format(e))

    b.sync_in()
    b.sync_out()
    b.set_last_access(Bundle.STATES.SYNCED)


def bundle_meta(args, l, rc):

    b = using_bundle(args, l).cast_to_subclass()

    if args.clean:
        b.clean()
        b.set_last_access(Bundle.STATES.CLEANED)

    b.sync_in()

    # Get the bundle again, to handle the case when the sync updated bundle.py or meta.py
    b = using_bundle(args, l).cast_to_subclass()
    b.meta()
    b.set_last_access(Bundle.STATES.META)

    b.sync_out()


def bundle_build(args, l, rc):

    b = using_bundle(args, l)

    if not args.force:
        check_built(b)

    if args.clean:
        if not b.clean():
            b.error("Clean failed, not building")
            return False
        b.set_last_access(Bundle.STATES.CLEANED)
        b.commit()

    b.sync_in()

    b = b.cast_to_subclass()

    b.build(sources=args.sources)
    b.sync_out()
    b.set_last_access(Bundle.STATES.BUILT)


def bundle_phase(args, l, rc):

    b = using_bundle(args, l).cast_to_subclass()

    check_built(b)

    if args.clean:
        if not b.clean():
            b.error("Clean failed, not building")
            return False
        b.set_last_access(Bundle.STATES.META)
        b.commit()

    b.sync_in()
    b.run_phase(args.phase, sources = args.sources)
    b.sync_out()

    b.set_last_access(Bundle.STATES.META)
    b.commit()


def bundle_install(args, l, rc):
    raise NotImplementedError()


def bundle_run(args, l, rc):

    b = using_bundle(args, l)

    if not args.force:
        check_built(b)

    b.sync()

    b = b.cast_to_subclass()

    if args.clean:
        b.clean()

    b.load_requirements()

    # Run a method on the bundle. Can be used for testing and development.
    try:
        f = getattr(b, str(args.method))
    except AttributeError as e:
        b.error("Could not find method named '{}': {} ".format(args.method, e))
        b.error("Available methods : {} ".format(dir(b)))

        return

    if not callable(f):
        raise TypeError("Got object for name '{}', but it isn't a function".format(args.method))

    b.logger.info("Running: {}({})".format(str(args.method), ','.join(args.args)))

    r = f(*args.args)
    b.sync_out()

    print "RETURN: ", r


def bundle_checkin(args, l, rc):

    ref, frm = get_bundle_ref(args, l)

    b = l.bundle(ref)

    remote, path = b.checkin()

    if path:
        b.log("Checked in to remote '{}' path '{}'".format(remote, path))


def bundle_set(args, l, rc):

    ref, frm = get_bundle_ref(args, l)

    b = l.bundle(ref)

    if args.state:
        prt("Setting state to {}".format(args.state))
        b.state = args.state
        b.commit()


def bundle_dump(args, l, rc):
    import datetime

    ref, frm = get_bundle_ref(args, l)

    b = l.bundle(ref)

    prt("Dumping {} for {}\n".format(args.table, b.identity.fqname))

    def trunc(v, l):
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
        records = sorted(records, key=lambda row: (row[0], row[1], row[2]))

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

        records = sorted(records, key=lambda row: (row[0]))

    elif args.table == 'sourcetables':

        records = []
        for t in b.dataset.source_tables:
            for c in t.columns:
                if not records:
                    records.append(c.row.keys())

                records.append(c.row.values())

        if records:
            headers, records = records[0], records[1:]
        else:
            headers = []

    elif args.table == 'tables':
        records = []
        headers = []
        for t in b.dataset.tables:
            for i, c in enumerate(t.columns):
                row = OrderedDict((k, v) for k, v in c.row.items() if k in
                                  ['table', 'name', 'id', 'datatype', 'caster', 'description'])

                if i == 0:
                    records.append(row.keys())  # once for each table

                records.append(row.values())

        records = zip(*[r for r in zip(*records) if bool(filter(bool, row[1:]))])

        if records:
            headers, records = records[0], records[1:]
        else:
            headers = []

    print tabulate(records, headers=headers)


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

    print yaml.dump(d, default_flow_style=False)


def bundle_repopulate(args, b, st, rc):
    raise NotImplementedError()
    # return b.repopulate()


def bundle_new(args, l, rc):
    """Clone one or more registered source packages ( via sync ) into the
    source directory."""

    from ambry.orm.exc import ConflictError

    d = dict(
        dataset=args.dataset,
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
        b = l.new_bundle(assignment_class=args.key, **d)

    except ConflictError:
        fatal("Can't create dataset; one with a conflicting name already exists")

    print b.identity.fqname


def bundle_import(args, l, rc):

    if args.source:
        source_dir = args.source
    else:
        source_dir = os.getcwd()

    source_dir = os.path.abspath(source_dir)

    fs = fsopendir(source_dir)

    config = yaml.load(fs.getcontents('bundle.yaml'))

    if not config:
        fatal("Failed to get a valid bundle configuration from '{}'".format(source_dir))

    bid = config['identity']['id']

    try:
        b = l.bundle(bid)
    except NotFoundError:
        b = l.new_from_bundle_config(config)

    b.set_file_system(source_url=source_dir)

    b.sync()

    prt("Loaded bundle: {}".format(b.identity.fqname))


def bundle_export(args, l, rc):

    b = using_bundle(args, l)

    if b.is_finalized:
        fatal("Can't export a finalized bundle: state =  {}".format(b.state))

    if args.source:
        source_dir = os.path.abspath(args.source)

        if args.append:
            source_dir = os.path.join(source_dir, b.identity.source_path)

        b.set_file_system(source_url=source_dir)

    b.sync(force='rtf', defaults=args.defaults)

    prt("Exported bundle: {}".format(b.source_fs))

file_const_map = dict(
    b=File.BSFILE.BUILD,
    d=File.BSFILE.DOC,
    m=File.BSFILE.META,
    s=File.BSFILE.SCHEMA,
    S=File.BSFILE.SOURCESCHEMA,
    r=File.BSFILE.SOURCES)


def bundle_edit(args, l, rc):
    import subprocess

    from ..util import getch
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    from Queue import Queue
    import threading

    b = using_bundle(args, l)

    prt("Found bundle {}".format(b.identity.fqname))

    EDITOR = os.environ.get('EDITOR', 'vim')  # that easy!

    b.sync()

    prt('Commands: q=quit, {}'.format(', '.join(k + '=' + v for k, v in file_const_map.items())))

    def edit(const):

        bf = b.build_source_files.file(const)
        bf.prepare_to_edit()

        file_path = bf.path

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
            queue.put(('change', event.src_path))

    observer = Observer()
    observer.schedule(EditEventHandler(), os.path.dirname(b.source_fs.getsyspath('/')))
    observer.start()

    # Thread to get commands from the user. Using a thread so that the char input can block on input
    # and the main thread can still process change events.
    def get_chars():
        while True:
            char = getch()

            if char == 'q' or ord(char) == 3:  # Crtl-c
                queue.put(('quit', None))
                break
            if char in ('p', 'B'):
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
                if b.is_buildable:
                    b.sync()
                else:
                    err("Bundle is not in a buildable state; did not sync")

            elif command == 'build':
                bc = b.cast_to_subclass()
                if b.is_buildable:
                    if arg == 'B':
                        bc.clean()
                        bc.build()
                else:
                    err("Bundle is not in a buildable state; not building")

            elif command == 'unknown':
                warn('Unknown command char: {} '.format(arg))
        except Exception:
            import traceback
            print(traceback.format_exc())

    observer.join()
    get_chars_t.join()


def bundle_extract(args, l, rc):
    import csv

    b = using_bundle(args, l)

    b.build_fs.makedir('extract', allow_recreate=True, recursive=True)
    bfs = b.build_fs.opendir('extract')

    limit = args.limit

    for p in b.partitions:
        b.logger.info('Extracting: {} '.format(p.name))
        with bfs.open(p.name + '.csv', 'wb') as f:
            w = csv.writer(f)
            for i, row in enumerate(p.datafile.reader()):

                if limit and i > limit:
                    break

                w.writerow(row)

    b.logger.info('Extracted to: {}'.format(bfs.getsyspath('/')))
