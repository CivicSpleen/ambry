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

from six import iteritems, iterkeys, callable as six_callable
from six.moves import queue as six_queue

from ambry.identity import NotObjectNumberError
from ambry.bundle import Bundle
from ambry.orm.exc import NotFoundError
from ambry.util import drop_empty

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
        if args.ref:
            l.bundle(args.ref)  # Exception if not exists
            return (args.ref, 'argument')
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

    if args.test:
        b.test = True

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
                    help="Number server key. One of 'self', 'unregistered', 'registered', 'authority' "
                         ' Use \'self\' for a random, self-generated key.'
                    )
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
                       help='Dump bundle definition files')
    group.add_argument('-i', '--ingested', default=False, action='store_const', const='ingested', dest='table',
                       help='List ingested data sources')
    group.add_argument('-s', '--sources', default=False, action='store_const',
                       const='datasources', dest='table',
                       help='Dump data source records')
    group.add_argument('-T', '--source_tables', default=False, action='store_const',
                       const='sourcetables', dest='table',
                       help='Dump source tables')
    group.add_argument('-p', '--partitions', default=False, action='store_const',
                       const='partitions', dest='table',
                       help='Dump partitions')
    group.add_argument('-t', '--dest_tables', default=False, action='store_const',
                       const='tables', dest='table',
                       help='Dump destination tables')
    group.add_argument('-P', '--pipes', default=False, action='store_const',
                       const='pipes', dest='table',
                       help='Dump destination tables')
    command_p.add_argument('ref', nargs='?', type=str, help='Bundle reference')

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
    command_p.add_argument('-q', '--quiet', default=False, action='store_true',
                           help='Just report the minimum information, ')

    command_p.add_argument('ref', nargs='?', type=str, help='Bundle reference')

    #
    # Sync Command
    #
    command_p = sub_cmd.add_parser('sync', help='Sync with a source representation')
    command_p.set_defaults(subcommand='sync')
    command_p.add_argument('ref', nargs='?', type=str, help='Bundle reference')
    command_p.add_argument('-f', '--files', default=False, action='store_true',
                           help='Sync from files to records')
    command_p.add_argument('-r', '--records', default=False, action='store_true',
                           help='Sync from records to files')

    #
    #     duplicate Command
    #
    command_p = sub_cmd.add_parser('duplicate',
                                   help='Increment a bundles version number and create a new bundle')
    command_p.set_defaults(subcommand='duplicate')
    command_p.add_argument('ref', nargs='?', type=str, help='Bundle reference')

    #
    # Clean Command
    #
    command_p = sub_cmd.add_parser('clean', help='Return bundle to state before build, prepare and extracts')
    command_p.add_argument('-a', '--all', default=False, action='store_true',
                           help='Clean everything: metadata, partitions, tables, config, everything. ')
    command_p.add_argument('-S', '--source', default=False, action='store_true',
                           help='Clean the source tables schema, but not ingested source files.  ')
    command_p.add_argument('-f', '--files', default=False, action='store_true',
                           help='Clean the ingested files')
    command_p.add_argument('-t', '--tables', default=False, action='store_true',
                           help='Clean destination tables')
    command_p.add_argument('-p', '--partitions', default=False, action='store_true',
                           help='Clean any built partitions')
    command_p.add_argument('-b', '--build', default=False, action='store_true',
                           help='Clean the build directory')
    command_p.set_defaults(subcommand='clean')
    command_p.add_argument('ref', nargs='?', type=str, help='Bundle reference')


    #
    # Ingest Command
    #
    command_p = sub_cmd.add_parser('ingest', help='Build or install download and convert data to internal file format')
    command_p.set_defaults(subcommand='ingest')

    command_p.add_argument('-c', '--clean-tables', default=False, action='store_true', help='Clean and rebuild source tables')
    command_p.add_argument('-C', '--clean-files', default=False, action='store_true', help='Delete and re-download files')
    command_p.add_argument('-f', '--force', default=False, action='store_true',
                           help='Re-ingest already ingested files')
    command_p.add_argument('-s', '--sync', default=False, action='store_true',
                           help='Syncrhonize before and after')
    command_p.add_argument('-t', '--table', action='append',
                           help='Only run the schema for the named tables. ')
    command_p.add_argument('-S', '--source',  action='append',
                           help='Sources to ingest, instead of running all sources')
    command_p.add_argument('ref', nargs='?', type=str, help='Bundle reference')

    #
    # Schema Command
    #
    command_p = sub_cmd.add_parser('meta', help='Generate the source and ddestination schemas')
    command_p.set_defaults(subcommand='meta')

    command_p.add_argument('-c', '--clean', default=False, action='store_true', help='Remove all columns from existing tavbles')
    command_p.add_argument('-f', '--force', default=False, action='store_true',
                           help='Re-run the schema, even if it already exists.')
    command_p.add_argument('-s', '--sync', default=False, action='store_true',
                           help='Synchronize before and after')
    command_p.add_argument('-t', '--table', action='append',
                           help='Only run the schema for the named tables. ')
    command_p.add_argument('-S', '--source', action='append',
                           help='Sources to ingest, instead of running all sources')
    command_p.add_argument('ref', nargs='?', type=str, help='Bundle reference')


    #
    # Build Command
    #
    command_p = sub_cmd.add_parser('build', help='Build the data bundle and partitions')
    command_p.set_defaults(subcommand='build')
    #command_p.add_argument('-s', '--sync', default=False, action='store_true', help='Sync with build source files')
    command_p.add_argument('-c', '--clean', default=False, action='store_true', help='Delete partitions before building')
    command_p.add_argument('-f', '--force', default=False, action='store_true',
                           help='Build even built or finalized bundles')
    command_p.add_argument('-m', '--meta', default=False, action='store_true',
                          help='Run the meta process before building')
    command_p.add_argument('-s', '--sync', default=False, action='store_true',
                           help='Syncrhonize before building')

    command_p.add_argument('-S', '--source', action='append',
                           help='Sources to build, instead of running all sources')
    command_p.add_argument('-t', '--table', action='append',
                           help='Build only sources that output to these destination tables')

    command_p.add_argument('ref', nargs='?', type=str, help='Bundle reference')

    #
    # Phase Command
    #
    command_p = sub_cmd.add_parser('phase', help='Run a phase')
    command_p.set_defaults(subcommand='phase')

    command_p.add_argument('-c', '--clean', default=False, action='store_true', help='Clean first')
    command_p.add_argument('-s', '--sync', default=False, action='store_true',
                           help='Synchronize before and after')
    command_p.add_argument('-f', '--force', default=False, action='store_true',
                           help='Build even built or finalized bundles')

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
    command_p.add_argument('-s', '--sync', default=False, action='store_true',
                           help='Syncrhonize before and after')
    command_p.add_argument('method', metavar='Method', type=str, help='Name of the method to run')
    command_p.add_argument('args', nargs='*', type=str, help='additional arguments')

    #
    # repopulate
    #
    command_p = sub_cmd.add_parser('repopulate',
                                   help='Load data previously submitted to the library back into the build dir')
    command_p.set_defaults(subcommand='repopulate')

    #
    # edit
    #

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

    command_p = sub_cmd.add_parser('cluster', help='Cluster sources by similar headers')
    command_p.set_defaults(subcommand='cluster')

def bundle_info(args, l, rc):
    from ambry.util.datestimes import compress_years

    if args.which:
        ref, frm = get_bundle_ref(args, l)
        b = using_bundle(args, l, print_loc=False)
        if args.quiet:
            prt(ref)
        else:
            prt('Will use bundle ref {}, {}, referenced from {}'.format(ref, b.identity.vname, frm))
        return

    b = using_bundle(args, l, print_loc=False)

    b.set_last_access(Bundle.STATES.INFO)

    if args.source_dir:
        print(b.source_fs.getsyspath('/'))
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
    print(tabulate(join(*info), tablefmt='plain'))

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

    print(tabulate(join(*info), tablefmt='plain'))

    info = [list()]

    inf(0, 'Build  FS', str(b.build_fs))
    inf(0, 'Source FS', str(b.source_fs))

    print(tabulate(join(info[0]), tablefmt='plain'))

    # DIsplay info about a partition, if one was provided
    if args.ref:
        try:
            p = b.partition(args.ref)

            info = [list(), list()]

            inf(0, 'ID', p.identity.id_)
            inf(0, 'VID', p.identity.vid)
            inf(1, 'Name', p.identity.name)
            inf(1, 'VName', p.identity.vname)

            inf(0, 'Location', p.location)
            inf(1, 'State', p.state)

            print('\nPartition')
            print(tabulate(join(*info), tablefmt='plain'))

        except (NotFoundError, AttributeError):
            pass

    if args.stats:

        from textwrap import wrap
        from terminaltables import SingleTable
        from itertools import islice

        def cast_str(x):
            try:
                return unicode(x)
            except:
                return str(x)

        for p in b.partitions:
            rows = ['Column LOM Count Uniques Values'.split()]
            d = p.stats_dict
            keys = [k for k, v in iteritems(d)]

            for c in p.table.columns:
                if c.name in keys:
                    k = c.name
                    v = d[c.name]

                    if v.lom == 'i':
                        values = v.text_hist
                    else:
                        values = '\n'.join(wrap(', '.join(islice(sorted(cast_str(x)
                                            for x in iterkeys(v.uvalues)), None, 10)), 50))

                    rows.append([ cast_str(k), cast_str(v.lom), cast_str(v.count), cast_str(v.nuniques), values])

            #print tabulate(row, tablefmt='plain')
            print(SingleTable(rows, title='Stats for ' + str(p.identity.name)).table)

    elif args.partitions:

        rows = []
        for p in b.partitions:
            rows.append([p.vid, p.vname, p.table.name,
                         ', '.join(str(e) for e in p.time_coverage[:5]), ', '.join(p.space_coverage[:5]),
                         ', '.join(p.grain_coverage[:5])])
        print('\nPartitions')
        print(tabulate(rows, headers="Vid Name Table Time Space Grain".split()))


def check_built(b):
    """Exit if the bundle is built or finalized"""
    if b.is_built or b.is_finalized:
        fatal("Can't perform operation; locked state = '{}'. Call bambry clean explicity".format(b.state))


def bundle_duplicate(args, l, rc):

    b = using_bundle(args, l)

    if not b.is_finalized:
        fatal("Can't increment a bundle unless it is finalized")

    nb = l.duplicate(b)

    nb.set_last_access(Bundle.STATES.NEW)

    prt("New Bundle: {} ".format(nb.identity.vname))


def bundle_finalize(args, l, rc):
    b = using_bundle(args, l)
    b.finalize()
    b.set_last_access(Bundle.STATES.FINALIZED)


def bundle_clean(args, l, rc):

    b = using_bundle(args, l).cast_to_subclass()

    if args.source or args.all:
        prt('Clean sources')
        b.clean_sources()

    if args.files or args.all:
        prt('Clean files')
        b.clean_files()

    if args.tables or args.all:
        prt('Clean tables and partitions')
        b.dataset.delete_tables_partitions()

    elif args.partitions or args.all:
        prt('Clean partitions')
        b.clean_partitions()

    if args.build or args.all:
        pass

    b.set_last_access(Bundle.STATES.NEW)
    b.commit()

def bundle_download(args, l, rc):
    b = using_bundle(args, l).cast_to__subclass()
    b.download()
    b.set_last_access(Bundle.STATES.DOWNLOADED)


def bundle_sync(args, l, rc):
    from ambry.dbexceptions import BundleError

    b = using_bundle(args, l)

    sync_out = (not args.records and not args.files) or args.records
    sync_in = (not args.records and not args.files) or args.files

    try:
        b = b.cast_to_subclass()
    except BundleError as e:
        err("Failed to load bundle code file: {}".format(e))

    if sync_in:
        prt("Sync in")
        b.sync_in()

    if sync_out:
        prt("Sync out")
        b.sync_out()

    b.set_last_access(Bundle.STATES.SYNCED)


def bundle_ingest(args, l, rc):

    b = using_bundle(args, l).cast_to_subclass()

    if args.sync:
        b.sync_in()

    if args.clean_tables:
        b.dataset.source_tables[:] = []
        b.commit()

    # Get the bundle again, to handle the case when the sync updated bundle.py or meta.py
    b = using_bundle(args, l, print_loc=False).cast_to_subclass()
    b.ingest(sources=args.source, force=args.force, clean_files=args.clean_files)
    b.set_last_access(Bundle.STATES.INGEST)

    if args.sync:
        b.sync_out()


def bundle_meta(args, l, rc):

    b = using_bundle(args, l).cast_to_subclass()

    if args.sync:
        b.sync_in()

    # Get the bundle again, to handle the case when the sync updated bundle.py or meta.py
    b = using_bundle(args, l, print_loc=False).cast_to_subclass()
    b.ingest(tables=args.table, force=args.force)
    b.schema(tables=args.table, force=args.force, clean=args.clean)
    b.set_last_access(Bundle.STATES.INGEST)

    if args.sync:
        b.sync_out()


def bundle_build(args, l, rc):

    b = using_bundle(args, l)

    if not args.force:
        check_built(b)
    else:
        b.state = Bundle.STATES.PREPARED

    if args.meta:
        bundle_meta(args,l,rc)


    if args.clean:
        b.dataset.delete_partitions()

    if args.sync:
        b.sync_in()
    else:
        b.sync_code()

    b = b.cast_to_subclass()

    if args.table:
        sources = list(s for s in b.sources if s.dest_table_name in args.tables)
    else:
        sources = args.source

    b.build(sources=sources, force=args.force)

    if args.sync:
        b.sync_out()

    b.set_last_access(Bundle.STATES.BUILT)


def bundle_phase(args, l, rc):

    b = using_bundle(args, l).cast_to_subclass()

    if not args.force:
        check_built(b)

    if args.clean:
        if not b.clean():
            b.error("Clean failed, not building")
            return False
        b.set_last_access(Bundle.STATES.META)
        b.commit()

    if args.sync:
        b.sync_in()
    else:
        b.sync_code()

    b.run_phase(args.phase, sources = args.sources)

    if args.sync:
        b.sync_out()

    b.set_last_access(Bundle.STATES.META)
    b.commit()


def bundle_install(args, l, rc):
    raise NotImplementedError()


def bundle_run(args, l, rc):

    b = using_bundle(args, l)

    if not args.force:
        check_built(b)

    if args.clean:
        b.clean()

    if args.sync:
        b.sync_in()
    else:
        b.sync_code()

    b = b.cast_to_subclass()

    b.load_requirements()

    # Run a method on the bundle. Can be used for testing and development.
    try:
        f = getattr(b, str(args.method))
    except AttributeError as e:
        b.error("Could not find method named '{}': {} ".format(args.method, e))
        b.error("Available methods : {} ".format(dir(b)))

        return

    if not six_callable(f):
        raise TypeError("Got object for name '{}', but it isn't a function".format(args.method))

    b.logger.info('Running: {}({})'.format(str(args.method), ','.join(args.args)))

    r = f(*args.args)

    if args.sync:
        b.sync_out()

    print("RETURN: ", r)


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
        headers = 'Vid Name State'.split()
        for row in b.dataset.partitions:
            records.append((
                row.vid,
                row.name,
                row.state
            )
            )
        records = sorted(records, key=lambda row: (row[0]))

    elif args.table == 'datasources':

        records = []
        for i, row in enumerate(b.dataset.sources):
            if not records:
                records.append(list(row.dict.keys()))

            records.append(list(row.dict.values()))

        records = drop_empty(records)

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
                    records.append(['vid'] + list(c.row.keys()))

                records.append([c.vid] + list(c.row.values()))

        # Transpose, remove empty columns, transpose back
        records = zip(*[row for row in zip(*records) if bool(filter(bool, row[1:]))])

        if records:
            headers, records = records[0], records[1:]
        else:
            headers = []

    elif args.table == 'tables':
        records = []
        headers = []
        for t in b.dataset.tables:
            for i, c in enumerate(t.columns):
                row = OrderedDict((k, v) for k, v in iteritems(c.row) if k in
                                  ['table', 'column', 'id', 'datatype', 'caster', 'description'])

                if i == 0: # Add the table headers
                    records.append(' ' * len(h) for h in row.keys())
                    records.append(list(row.keys()))  # once for each table
                    records.append( '-'*len(h) for h in row.keys())

                records.append(list(row.values()))

        records = drop_empty(records)
        if records:
            _, records = records[0], records[1:]
        else:
            headers = []

    elif args.table == 'pipes':
        terms = args.ref

        b.import_lib()

        if len(terms) == 2:
            phase, source = terms
        else:
            phase = terms
            source = None

        pl = b.pipeline(phase, source)

        print pl

        records = None

    elif args.table == 'ingested':
        terms = args.ref

        headers = 'name path'.split()
        records = []
        for s in b.sources:
            records.append((s.name, s.datafile.syspath))

    if records:
        print(tabulate(records, headers=headers))


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

    if args.dryrun:
        from ..identity import Identity
        d['revision'] = 1
        d['id'] = 'dXXX'
        print(str(Identity.from_dict(d)))
        return

    try:
        b = l.new_bundle(assignment_class=args.key, **d)

    except ConflictError:
        fatal("Can't create dataset; one with a conflicting name already exists")

    print(b.identity.fqname)

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
    import threading

    b = using_bundle(args, l)

    prt("Found bundle {}".format(b.identity.fqname))

    EDITOR = os.environ.get('EDITOR', 'vim')  # that easy!

    b.sync()

    prt('Commands: q=quit, {}'.format(', '.join(k + '=' + v for k, v in list(file_const_map.items()))))

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

    queue = six_queue.Queue()

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
            print("\033[0G\033[1F")

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
    import unicodecsv as csv

    b = using_bundle(args, l)

    b.build_fs.makedir('extract', allow_recreate=True, recursive=True)
    bfs = b.build_fs.opendir('extract')

    limit = args.limit

    for p in b.partitions:
        b.logger.info('Extracting: {} '.format(p.name))
        with bfs.open(p.name + '.csv', 'wb') as f, p.datafile.reader as r:
            w = csv.writer(f)
            w.writerow(r.headers)
            if limit:
                from itertools import islice
                w.writerows(islice(r.rows,None, limit))
            else:
                w.writerows(r.rows)


    b.logger.info('Extracted to: {}'.format(bfs.getsyspath('/')))

def bundle_cluster(args, l, rc):

    from ambry.etl import ClusterHeaders
    import yaml

    b = using_bundle(args, l)

    ch = ClusterHeaders()

    for t in b.dataset.source_tables:
        ch.add_header(t.name, sorted([c.source_header for c in t.columns ]))


    print yaml.safe_dump({'source_sets': ch.cluster()}, indent=4, default_flow_style=False)

