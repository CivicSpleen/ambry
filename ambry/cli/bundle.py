# coding: utf-8
"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""
import yaml
from six import iteritems, iterkeys, callable as six_callable, text_type, binary_type
from tabulate import tabulate

import os
from ambry.bundle import Bundle
from ambry.identity import NotObjectNumberError
from ambry.orm.exc import NotFoundError
from ambry.util import drop_empty
from . import get_docker_links, docker_client
from fs.opener import fsopendir
from ..cli import prt, fatal, warn, prt_no_format
from ..orm import File


def bundle_command(args, rc):

    from ..library import Library
    from . import global_logger
    from ambry.orm.exc import ConflictError
    from ambry.dbexceptions import LoggedException

    if args.test_library:
        rc.set_library_database('test')

    l = Library(rc, echo=args.echo)

    global global_library

    global_library = l

    l.logger = global_logger

    l.sync_config()

    if args.debug:
        from ..util import debug
        warn('Entering debug mode. Send USR1 signal (kill -USR1 ) to break to interactive prompt')
        debug.listen()

    if args.processes:
        args.multi = args.processes
        l.processes = args.processes

    try:
        globals()['bundle_' + args.subcommand](args, l, rc)
    except ConflictError as e:
        fatal(str(e))
    except LoggedException as e:
        exc = e.exc
        b = e.bundle
        b.fatal(str(e.message))


def get_bundle_ref(args, l, use_history=False):
    """ Use a variety of methods to determine which bundle to use

    :param args:
    :return:
    """

    if not use_history:
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
                try:
                    id_ = config['identity']['id']
                    return (id_, 'directory')
                except KeyError:
                    pass

    history = l.edit_history()

    if history:
        return (history[0].d_vid, 'history')

    return None, None


def using_bundle(args, l, print_loc=True, use_history=False):

    ref, frm = get_bundle_ref(args, l, use_history=use_history)

    if not ref:
        fatal("Didn't get a bundle ref from the -i option, history, environment or argument")

    if print_loc:
        prt('Using bundle ref {}, referenced from {}'.format(ref, frm))

    b = l.bundle(ref, True)

    b.multi = args.multi
    b.capture_exceptions = not args.exceptions

    if args.debug:
        warn('Bundle debug mode. Send USR2 signal (kill -USR2 ) to displaya stack trace')
        l.init_debug()

    if args.limited_run:
        b.limited_run = True
    if print_loc:  # Try to only do this once
        b.log_to_file('==============================')
    return b


def bundle_parser(cmd):
    import argparse

    parser = cmd.add_parser('bundle', help='Manage bundle files')
    parser.set_defaults(command='bundle')

    parser.add_argument('-i', '--id', required=False, help='Bundle ID')
    parser.add_argument('-D', '--debug', required=False, default=False, action='store_true',
                        help='URS1 signal will break to interactive prompt')
    parser.add_argument('-L', '--limited-run', default=False, action='store_true',
                        help='Enable bundle-specific behavior to reduce number of rows processed')
    parser.add_argument('-e', '--echo', required=False, default=False, action='store_true',
                        help='Echo database queries.')
    parser.add_argument('-E', '--exceptions', default=False, action='store_true',
                        help="Don't capture and reformat exceptions; show the traceback on the console")
    parser.add_argument('-m', '--multi', default=False, action='store_true',
                        help='Run in multiprocessing mode')
    parser.add_argument('-p', '--processes',  type=int,
                        help='Number of multiprocessing processors. implies -m')

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
    sp.add_argument('-n', '--dryrun', action='store_true', default=False, help='Dry run')
    sp.add_argument('-k', '--key', default='self',
                    help="Number server key. One of 'self', 'unregistered', 'registered', 'authority' "
                         ' Use \'self\' for a random, self-generated key.'
                    )
    sp.add_argument('args', nargs=argparse.REMAINDER)  # Get everything else.

    # Config sub commands
    #

    command_p = sub_cmd.add_parser('config', help='Operations on the bundle configuration file')
    command_p.set_defaults(subcommand='config')

    asp = command_p.add_subparsers(title='Config subcommands',
                                   help='Subcommand for operations on a bundle file')

    # Dump command
    command_p = sub_cmd.add_parser('dump', help='Dump records from the bundle database')
    command_p.set_defaults(subcommand='dump')
    group = command_p.add_mutually_exclusive_group()
    group.add_argument('-c', '--config', default=False, action='store_const', const='configs', dest='table',
                       help='Dump configs')
    group.add_argument('-t', '--dest_tables', default=False, action='store_const',
                       const='tables', dest='table',
                       help='Dump destination tables, but not the table columns')
    group.add_argument('-C', '--dest_table_columns', default=False, action='store_const',
                       const='columns', dest='table',
                       help='Dump destination tables along with their columns')
    group.add_argument('-f', '--files', default=False, action='store_const', const='files', dest='table',
                       help='Dump bundle definition files')
    group.add_argument('-i', '--ingested', default=False, action='store_const',
                       const='ingested', dest='table',
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
    group.add_argument('-P', '--pipes', default=False, action='store_const',
                       const='pipes', dest='table',
                       help='Dump destination tables')
    group.add_argument('-m', '--metadata', default=False, action='store_const',
                       const='metadata', dest='table',
                       help='Dump metadata as json')
    command_p.add_argument('ref', nargs='?', type=str, help='Bundle reference')

    # Set command
    #
    command_p = sub_cmd.add_parser('set', help='Set configuration and state values')
    command_p.set_defaults(subcommand='set')
    group = command_p.add_mutually_exclusive_group()
    group.add_argument('-s', '--state', default=None, help='Set the build state')

    # Info command
    #
    command_p = sub_cmd.add_parser('info', help='Print information about the bundle')
    command_p.set_defaults(subcommand='info')
    command_p.add_argument('-w', '--which', default=False, action='store_true',
                           help='Report the reference of the bundles that will be accessed by other commands')
    command_p.add_argument('-s', '--source_dir', default=False, action='store_true',
                           help='Display the source directory')
    command_p.add_argument('-b', '--build_dir', default=False, action='store_true',
                           help='Display the build directory')
    command_p.add_argument('-S', '--stats', default=False, action='store_true',
                           help='Also report column stats for partitions')
    command_p.add_argument('-p', '--partitions', default=False, action='store_true',
                           help='Also report partition details')
    command_p.add_argument('-q', '--quiet', default=False, action='store_true',
                           help='Just report the minimum information, ')
    command_p.add_argument('-H', '--history', default=False, action='store_true',
                           help='When locating a bundle for --which, use only the edit history.')
    command_p.add_argument('-T', '--touch', default=False, action='store_true',
                           help='Update the edit history to this bundle')

    command_p.add_argument('ref', nargs='?', type=str, help='Bundle reference')

    # Sync Command
    #
    command_p = sub_cmd.add_parser('sync', help='Sync with a source representation')
    command_p.set_defaults(subcommand='sync')
    command_p.add_argument('ref', nargs='?', type=str, help='Bundle reference')
    command_p.add_argument('-i', '--in', default=False, action='store_true',
                           help='Sync from files to records')
    command_p.add_argument('-o', '--out', default=False, action='store_true',
                           help='Sync from records to files')
    command_p.add_argument('-c', '--code', default=False, action='store_true',
                           help='Sync bundle.py, bundle.yaml and other code files in, but not sources or schemas.')

    #     duplicate Command
    #
    command_p = sub_cmd.add_parser('duplicate',
                                   help='Increment a bundles version number and create a new bundle')
    command_p.set_defaults(subcommand='duplicate')
    command_p.add_argument('ref', nargs='?', type=str, help='Bundle reference')

    # Clean Command
    #
    command_p = sub_cmd.add_parser('clean', help='Return bundle to state before build, prepare and extracts')
    command_p.add_argument('-a', '--all', default=False, action='store_true',
                           help='Clean everything: metadata, partitions, tables, config, everything. ')
    command_p.add_argument('-s', '--source', default=False, action='store_true',
                           help='Clean the source tables schema, but not ingested source files.  ')
    command_p.add_argument('-F', '--files', default=False, action='store_true',
                           help='Clean build source files')
    command_p.add_argument('-i', '--ingested', default=False, action='store_true',
                           help='Clean the ingested files')
    command_p.add_argument('-t', '--tables', default=False, action='store_true',
                           help='Clean destination tables')
    command_p.add_argument('-p', '--partitions', default=False, action='store_true',
                           help='Clean any built partitions')
    command_p.add_argument('-b', '--build', default=False, action='store_true',
                           help='Clean the build directory')
    command_p.add_argument('-B', '--build-state', default=False, action='store_true',
                           help='Clean the build state configuration')
    command_p.add_argument('-S', '--sync', default=False, action='store_true',
                           help='Sync in after cleaning')
    command_p.add_argument('-f', '--force', default=False, action='store_true',
                           help='Clean even built and finalized bundles')
    command_p.set_defaults(subcommand='clean')
    command_p.add_argument('ref', nargs='?', type=str, help='Bundle reference')

    # Ingest Command
    #
    command_p = sub_cmd.add_parser('ingest',
                                   help='Build or install download and convert data to internal file format')
    command_p.set_defaults(subcommand='ingest')

    command_p.add_argument('-f', '--force', default=False, action='store_true',
                           help='Force ingesting already ingested files')
    command_p.add_argument('-c', '--clean', default=False, action='store_true',
                           help='Clean ingested files first')
    command_p.add_argument('-p', '--purge-tables', default=False, action='store_true',
                           help='Completely remove all source tables. Equivalent to deleting source_scheama.csv'
                           ' and syncing.')
    command_p.add_argument('-t', '--table', action='append',
                           help='Only run the schema for the named tables. ')
    command_p.add_argument('-s', '--source',  action='append',
                           help='Sources to ingest, instead of running all sources')
    command_p.add_argument('-S', '--stage', help='Ingest sources at this stage')
    command_p.add_argument('ref', nargs='?', type=str, help='Bundle reference')


    # Schema Command
    #
    command_p = sub_cmd.add_parser('schema', help='Generate destination schemas from the source schemas')
    command_p.set_defaults(subcommand='schema')

    command_p.add_argument('-b', '--build', action='store_true',
                           help='For the destination schema, use the build process to '
                                'determine the schema, not the source tables ')

    command_p.add_argument('-c', '--clean', default=False, action='store_true',
                           help='Remove all columns from existing tables')

    command_p.add_argument('-f', '--force', default=False, action='store_true',
                           help='Re-run the schema, even if it already exists.')
    command_p.add_argument('-t', '--table', action='append',
                           help='Only run the schema for the named tables. ')
    command_p.add_argument('-s', '--source', action='append',
                           help='Sources to ingest, instead of running all sources')

    command_p.add_argument('ref', nargs='?', type=str, help='Bundle reference')

    #
    # Run Command
    #
    command_p = sub_cmd.add_parser('run', help='Ingest, crate a schema, and build.')
    command_p.set_defaults(subcommand='run')

    command_p.add_argument('-f', '--force', default=False, action='store_true',
                           help='Build even built or finalized bundles')

    command_p.add_argument('-c', '--clean', default=False, action='store_true',
                           help='Clean and synchronize before running')

    command_p.add_argument('-y', '--sync', default=False, action='store_true',
                           help='Synchronize before and after')

    command_p.add_argument('-q', '--quick', default=False, action='store_true',
                           help="Just rebuild; don't clean beforehand")

    command_p.add_argument('-s', '--source', action='append',
                           help='Sources to build, instead of running all sources')
    command_p.add_argument('-t', '--table', action='append',
                           help='Build only sources that output to these destination tables')
    command_p.add_argument('-S', '--stage', help='Ingest sources at this stage')

    command_p.add_argument('ref', nargs='?', type=str, help='Bundle reference')

    # Build Command
    #
    command_p = sub_cmd.add_parser('build', help='Build the data bundle and partitions')
    command_p.set_defaults(subcommand='build')

    command_p.add_argument('-f', '--force', default=False, action='store_true',
                           help='Build even built or finalized bundles')

    command_p.add_argument('-y', '--sync', default=False, action='store_true',
                           help='Synchronize before and after')

    command_p.add_argument('-q', '--quick', default=False, action='store_true',
                           help="Just rebuild; don't clean beforehand")

    command_p.add_argument('-s', '--source', action='append',
                           help='Sources to build, instead of running all sources')
    command_p.add_argument('-t', '--table', action='append',
                           help='Build only sources that output to these destination tables')
    command_p.add_argument('-S', '--stage', help='Ingest sources at this stage')

    command_p.add_argument('ref', nargs='?', type=str, help='Bundle reference')

    # Finalize Command
    #
    command_p = sub_cmd.add_parser('finalize', help='Finalize the bundle, preventing further changes')
    command_p.set_defaults(subcommand='finalize')

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
    # Exec Command
    #

    command_p = sub_cmd.add_parser('exec', help='Execute a method on the bundle')
    command_p.set_defaults(subcommand='exec')
    command_p.add_argument('-c', '--clean', default=False, action='store_true', help='Clean first')
    command_p.add_argument('-f', '--force', default=False, action='store_true',
                           help='Force running on a built or finalized bundle')
    command_p.add_argument('-s', '--sync', default=False, action='store_true',
                           help='Syncrhonize before and after')
    command_p.add_argument('method', metavar='Method', type=str, help='Name of the method to run')
    command_p.add_argument('args', nargs='*', type=str, help='additional arguments')

    #
    # Ampr
    #

    command_p = sub_cmd.add_parser('view', help='View the datafile for a source or partition, using the ampr command')
    command_p.set_defaults(subcommand='view')
    from ambry_sources.cli import make_arg_parser
    make_arg_parser(command_p)

    #
    # repopulate
    #
    command_p = sub_cmd.add_parser('repopulate',
                                   help='Load data previously submitted to the library back into the build dir')
    command_p.set_defaults(subcommand='repopulate')



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
    command_p.add_argument('-l', '--limit', type=int, default=None, help='Limit on number of rows per file')
    command_p.add_argument('partition', nargs='?', metavar='partition',  type=str, help='Partition to extract')
    command_p.add_argument('directory', nargs='?', metavar='directory', help='Output directory')

    # Colmap
    #

    command_p = sub_cmd.add_parser('colmap', help='Create or load a column map')
    command_p.set_defaults(subcommand='colmap')
    group = command_p.add_mutually_exclusive_group(required=True)
    group.add_argument('-c', '--create', default=False, action='store_true',
                       help='Create the individual table maps')
    group.add_argument('-b', '--build', default=False, action='store_true',
                       help='Build the combined map from the table maps')
    group.add_argument('-l', '--load', default=False, action='store_true',
                       help='Load the combined map into the source tables')

    #
    # Test Command
    #
    command_p = sub_cmd.add_parser('test', help='Run the bundle test code')
    command_p.set_defaults(subcommand='test')
    command_p.add_argument('tests', nargs='*', type=str, help='Tests to run')

    # Docker
    #

    command_p = sub_cmd.add_parser('docker', help='Run bambry commands in a docker container')
    command_p.set_defaults(subcommand='docker')
    command_p.add_argument('-i', '--docker_id', default=False, action='store_true',
                           help='Print the id of the running container')
    command_p.add_argument('-c', '--container', help='Use a specific container id')
    command_p.add_argument('-k', '--kill', default=False, action='store_true',
                           help='Kill the running container')
    command_p.add_argument('-n', '--docker_name', default=False, action='store_true',
                           help='Print the name of the running container')
    command_p.add_argument('-l', '--logs', default=False, action='store_true',
                           help='Get the logs (stdout) from a runnings container')
    command_p.add_argument('-s', '--shell', default=False, action='store_true',
                           help='Run a shell on the currently running container')
    command_p.add_argument('-S', '--stats', default=False, action='store_true',
                           help='Report stats from the currently running container')
    command_p.add_argument('-v', '--version', default=False, action='store_true',
                           help='Select a docker version that is the same as this Ambry installation')

    command_p.add_argument('args', nargs='*', type=str, help='additional arguments')

    #
    # Log
    #

    # Set command
    #
    command_p = sub_cmd.add_parser('log', help='Print out various logs')
    command_p.set_defaults(subcommand='log')

    command_p.add_argument('-e', '--exceptions', default=None, action='store_true',
                       help='Print exceptions from the progress log')
    command_p.add_argument('-p', '--progress', default=None, action='store_true',
                       help='Display progress logs')
    command_p.add_argument('-s', '--stats', default=None, action='store_true',
                       help='Display states and counts of partitions and sources')
    command_p.add_argument('-a', '--all', default=None, action='store_true',
                           help='Display all records')



def bundle_info(args, l, rc):
    from ambry.util.datestimes import compress_years

    if args.which:
        ref, frm = get_bundle_ref(args, l, use_history=args.history)

        b = l.bundle(ref)

        if args.quiet:
            prt(ref)

        else:
            prt('Will use bundle ref {}, {}, referenced from {}'.format(ref, b.identity.vname, frm))

        if args.touch:
            b.set_last_access(Bundle.STATES.INFO)
            b.commit()

        return

    b = using_bundle(args, l, print_loc=False, use_history=args.history)

    if args.touch:
        b.set_last_access(Bundle.STATES.INFO)
        b.commit()

    # Just print the directory. Used in bambrycd to change
    # directory in the user's shell
    if args.source_dir:
        print(b.source_fs.getsyspath('/'))
        return
    elif args.build_dir:
        print(b.build_fs.getsyspath('/'))
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

    inf(1, 'Build State', b.buildstate.state.current)
    inf(1, 'Build Time', b.buildstate.build_duration_pretty)
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
                return text_type(x)
            except:
                return binary_type(x)

        for p in b.partitions:
            rows = ['Column LOM Count Uniques Values'.split()]
            # FIXME! This is slow. Some of the stats should be loaded into the partitions, such as the
            # number of rows.
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

                    rows.append(
                        [cast_str(k), cast_str(v.lom), cast_str(v.count), cast_str(v.nuniques), values])

            # print tabulate(row, tablefmt='plain')
            print(SingleTable(rows, title='Stats for ' + str(p.identity.name)).table)

    elif args.partitions:
        from ambry.orm import Partition
        from sqlalchemy.orm import lazyload, joinedload

        rows = []
        for p in (b.dataset.query(Partition).filter(Partition.d_vid == b.identity.vid)
                          .options(lazyload('*'), joinedload(Partition.table))
                  ).all():


            rows.append([p.vid, p.vname, p.table.name,  p.count,
                         p.identity.time, p.identity.space, p.identity.grain,
                         '({}) '.format(len(p.time_coverage))+', '.join(str(e) for e in p.time_coverage[:5]),
                         '({}) '.format(len(p.space_coverage))+', '.join(p.space_coverage[:5]),
                         '({}) '.format(len(p.grain_coverage))+', '.join(p.grain_coverage[:5])])
        print('\nPartitions')
        rows = ['Vid Name Table Rows Time Space Grain TimeCov GeoCov GeoGrain'.split()] + rows
        rows = drop_empty(rows)
        if rows:
            print(tabulate(rows[1:], headers=rows[0]))


def check_built(b):
    """Exit if the bundle is built or finalized"""
    if b.is_built or b.is_finalized:
        fatal("Can't perform operation; state = '{}'. "
              "Call `bambry clean` explicitly or build with -f option".format(b.state))


def bundle_duplicate(args, l, rc):

    b = using_bundle(args, l)

    if not b.is_finalized:
        fatal("Can't increment a bundle unless it is finalized")

    nb = l.duplicate(b)

    nb.set_last_access(Bundle.STATES.NEW)

    prt('New Bundle: {} '.format(nb.identity.vname))


def bundle_finalize(args, l, rc):
    b = using_bundle(args, l)
    b.finalize()
    b.set_last_access(Bundle.STATES.FINALIZED)


def bundle_clean(args, l, rc):

    b = using_bundle(args, l)

    if not args.force:
        check_built(b)

    if not any((args.source, args.files, args.tables, args.partitions, args.ingested,
                args.build_state)):
        args.all = True

    if args.source or args.all:
        prt('Clean sources')
        b.clean_sources()

    if args.files or args.all:
        prt('Clean files')
        b.clean_files()

    if args.tables or args.all:
        prt('Clean tables and partitions')
        b.dataset.delete_tables_partitions()

    if args.partitions or args.all:
        prt('Clean partitions')
        b.clean_partitions()

    if args.build or args.all:
        prt('Clean build')
        b.clean_build()

    if args.ingested or args.all:
        prt('Clean ingested')
        b.clean_ingested()

    if args.build_state or args.all:
        prt('Clean build_state')
        b.clean_build_state()

    if args.sync:
        b.sync_in()
        b.set_last_access(Bundle.STATES.SYNCED)
        b.state = Bundle.STATES.SYNCED
    else:
        b.set_last_access(Bundle.STATES.CLEANED)
        b.state = Bundle.STATES.CLEANED

    b.commit()


def bundle_download(args, l, rc):
    b = using_bundle(args, l).cast_to__subclass()
    b.download()
    b.set_last_access(Bundle.STATES.DOWNLOADED)


def bundle_sync(args, l, rc):

    b = using_bundle(args, l)

    sync_in = getattr(args, 'in') or not any((getattr(args, 'in'), args.code,  args.out))

    if sync_in:
        prt('Sync in')
        b.sync_in()

    if args.code:
        synced = b.sync_code()
        prt('Synced {} files'.format(synced))

    if args.out:
        prt('Sync out')
        b.sync_out()

    b.set_last_access(Bundle.STATES.SYNCED)
    b.commit()


def bundle_ingest(args, l, rc):

    b = using_bundle(args, l).cast_to_subclass()
    b.set_last_access(Bundle.STATES.SYNCED)
    b.commit()

    b.clean_progress()

    if not args.force and not args.table and not args.source:
        check_built(b)

    b.sync_code()

    if b.sync_sources() > 0:
        b.log("Source file changed, automatically cleaning")
        args.clean = True

    if args.clean:
        b.clean_ingested()

    if args.purge_tables:
        b.build_source_files.file(File.BSFILE.SOURCESCHEMA).remove()
        b.dataset.source_tables[:] = []
        b.commit()

    b.ingest(tables=args.table, sources=args.source, force=args.force)

    b.sync_out()

    b.set_last_access(Bundle.STATES.INGESTED)


def bundle_schema(args, l, rc):

    b = using_bundle(args, l).cast_to_subclass()
    b.set_last_access(Bundle.STATES.SYNCED)
    b.clean_progress()
    b.commit()

    b.sync_code()

    b.ingest(sources=args.source, tables=args.table, force=args.force)

    b.schema(sources=args.source, tables=args.table, clean=args.clean, use_pipeline=args.build)

    b.set_last_access(Bundle.STATES.SCHEMA)

    b.sync_out()

    prt("Created destination schema")


def bundle_build(args, l, rc):

    b = using_bundle(args, l)
    b.set_last_access(Bundle.STATES.SYNCED)
    b.clean_progress()
    b.commit()

    args.force = True if args.quick else args.force

    if not args.force and not args.table and not args.source:
        check_built(b)

    if args.sync:
        b.sync_in()
    else:
        b.sync_code()
        b.sync_sources()
        b.sync_schema()

    b = b.cast_to_subclass()

    b.build(sources=args.source, tables=args.table, stage=args.stage, force=args.force)

    b.set_last_access(Bundle.STATES.BUILT)

def bundle_run(args, l, rc):

    b = using_bundle(args, l)

    b.clean_progress()

    args.force = True if args.quick else args.force

    if not args.force and not args.table and not args.source:
        check_built(b)

    if args.clean:
        b.clean()
        args.sync = True

    if args.sync:
        b.sync_in()
    else:
        b.sync_code()
        b.sync_sources()

    b = b.cast_to_subclass()

    b.run(sources=args.source, tables=args.table, stage=args.stage, force=args.force)

    b.set_last_access(Bundle.STATES.BUILT)


def bundle_install(args, l, rc):
    raise NotImplementedError()


def bundle_exec(args, l, rc):

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

    prt('RETURN: ', r)


def bundle_checkin(args, l, rc):

    ref, frm = get_bundle_ref(args, l)

    b = l.bundle(ref, True)

    remote_name = l.resolve_remote(b)
    remote = l.remote(remote_name)

    remote, path = b.checkin()

    if path:
        b.log("Checked in to remote '{}' path '{}'".format(remote, path))


def bundle_set(args, l, rc):

    ref, frm = get_bundle_ref(args, l)

    b = l.bundle(ref, True)

    if args.state:
        prt('Setting state to {}'.format(args.state))
        b.state = args.state
        b.commit()


def bundle_dump(args, l, rc):
    import datetime
    from six import text_type

    ref, frm = get_bundle_ref(args, l)

    b = l.bundle(ref, True)

    prt('Dumping {} for {}\n'.format(args.table, b.identity.fqname))

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
                trunc(text_type(row.value), 22),
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
        headers = 'Vid Name State Type'.split()
        for row in b.dataset.partitions:
            records.append((
                row.vid,
                row.name,
                row.state,
                row.type
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
        records = drop_empty(records)

        if records:
            headers, records = records[0], records[1:]
        else:
            headers = []

    elif args.table == 'columns':

        for t in b.dataset.tables:

            print('---{}'.format(t.name))

            def record_gen():
                for i, row in enumerate([c.row for c in t.columns]):
                    if i == 0:
                        yield row.keys()
                    yield row.values()

            records = list(record_gen())

            records = drop_empty(records)

            print(tabulate(records[1:], headers=records[0]))

        return  # Can't follow the normal process b/c we have to run each table seperately.

    elif args.table == 'tables':

        records = []

        for table in b.dataset.tables:
            row = table.row
            if not records:
                records.append(row.keys())
            records.append(row.values())

        records = drop_empty(records)

        if records:
            headers, records = records[0], records[1:]
        else:
            headers, records = [], []

        records = sorted(records, key=lambda row: (row[0]))

    elif args.table == 'pipes':
        terms = args.ref

        b.import_lib()

        if len(terms) == 2:
            phase, source = terms
        else:
            phase = terms
            source = None

        pl = b.pipeline(phase, source)

        print(pl)

        records = None

    elif args.table == 'ingested':
        from fs.errors import ResourceNotFoundError
        import zlib

        terms = args.ref

        headers = ('name', 'state', 'hdr', 'st', 'rows', 'path', 'first 3 headers')
        records = []

        for s in b.sources:
            if s.is_downloadable:
                df = s.datafile

                try:
                    info = df.info
                except (ResourceNotFoundError, zlib.error, IOError):
                    continue

                records.append((s.name,
                                s.state,
                                info.get('header_rows'),
                                info.get('data_start_row'),
                                info.get('rows'),
                                df.syspath.replace(b.build_fs.getsyspath('/'), '.../'),
                                ','.join(df.headers[:3])

                                ))

        records = sorted(records, key=lambda r: (r[4], r[0]))

    elif args.table == 'metadata':

        for key, value in b.metadata.kv:
            print key, value

        return

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
        ambry_account = rc.accounts.get('ambry', {})
    except:
        ambry_account = None

    if not ambry_account:
        fatal("Failed to get an accounts.ambry entry from the configuration. ")

    if not ambry_account.get('name') or not ambry_account.get('email'):
        fatal('Must set accounts.ambry.email and accounts.ambry.name n account config file')

    if args.dryrun:
        from ..identity import Identity
        d['revision'] = 1
        d['id'] = 'dXXX'
        print(str(Identity.from_dict(d)))
        return

    try:
        b = l.new_bundle(assignment_class=args.key, **d)
        b.metadata.contacts.wrangler.email = 'non@eample.com'

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
        b = l.bundle(bid, True)
    except NotFoundError:
        b = l.new_from_bundle_config(config)

    b.set_file_system(source_url=source_dir)

    b.sync()

    prt('Loaded bundle: {}'.format(b.identity.fqname))

    b.set_last_access(Bundle.STATES.SYNCED)
    b.commit()


def bundle_export(args, l, rc):

    b = using_bundle(args, l)

    if args.source:

        source_dir = os.path.abspath(args.source)

        if args.append:
            source_dir = os.path.join(source_dir, b.identity.source_path)
            if not os.path.exists(source_dir):
                os.makedirs(source_dir)

        b.set_file_system(source_url=source_dir)

    b.sync(force='rtf', defaults=args.defaults)

    b.sync_out()

    prt('Exported bundle: {}'.format(b.source_fs))

    b.set_last_access(Bundle.STATES.SYNCED)
    b.commit()

file_const_map = dict(
    b=File.BSFILE.BUILD,
    d=File.BSFILE.DOC,
    m=File.BSFILE.META,
    s=File.BSFILE.SCHEMA,
    S=File.BSFILE.SOURCESCHEMA,
    r=File.BSFILE.SOURCES)




def bundle_extract(args, l, rc):
    import unicodecsv as csv

    b = using_bundle(args, l)

    b.build_fs.makedir('extract', allow_recreate=True, recursive=True)
    bfs = b.build_fs.opendir('extract')

    limit = args.limit

    for p in b.partitions:

        if args.partition and args.partition != p.identity.vid:
            continue

        b.logger.info('Extracting: {} '.format(p.name))
        with bfs.open(p.name + '.csv', 'wb') as f, p.datafile.reader as r:
            w = csv.writer(f)
            w.writerow(r.headers)
            if limit:
                from itertools import islice
                w.writerows(islice(r.rows, None, limit))
            else:
                w.writerows(r.rows)

    b.logger.info('Extracted to: {}'.format(bfs.getsyspath('/')))


def bundle_view(args, l, rc):
    from ambry_sources.cli import main

    b = using_bundle(args, l)

    arg = args.path[0]
    df = None
    if os.path.exists(arg):
        path = arg

    if not df:
        try:
            source = b.source(arg)
            df = source.datafile
        except:
            pass

    # Maybe it is a partition
    if not df:
        try:
            p = b.partition(arg)
            df = p.datafile
        except:
            pass

    if not df:
        try:
            p = l.partition(arg)
            df = p.datafile
        except:
            pass

    if not df:
        fatal("Didn't get a path to an MPR file, nor a reference to a soruce or partition")

    args.path = [df]

    main(args)


def bundle_colmap(args, l, rc):
    """

    Generally, colmap editing has these steps:

    - Ingest, to setup the source tables
    - bambry colmap -c, to create the table colmaps
    - edit the table colmaps
    - bambry colmap -b, to create the combined column map
    - Review the combined column map
    - bambry colmap -l, to load the combined colman into the source schema.
    - bambry sync -r, to sync from the source schema records back out to files.

    """
    from itertools import groupby
    from operator import attrgetter
    from collections import OrderedDict
    import csv

    b = using_bundle(args, l)

    mapfile_name = 'colmap.csv'

    if args.create:
        # Create the colmap tables, one per destination table

        keyfunc = attrgetter('dest_table')
        for dest_table, sources in groupby(sorted(b.sources, key=keyfunc), keyfunc):

            sources = list(sources)

            n_sources = len(sources)

            columns = []

            for c in dest_table.columns:
                if c.name not in columns:
                    columns.append(c.name)

            for source in sources:
                for c in source.source_table.columns:
                    if c.name not in columns:
                        columns.append(c.name)

            columns = OrderedDict((c, [''] * n_sources) for c in columns)

            for i, source in enumerate(sources):
                source_cols = [c.name for c in source.source_table.columns]

                for c_name, row in columns.items():
                    if c_name in source_cols:
                        row[i] = c_name

            fn = 'colmap_{}.csv'.format(dest_table.name)

            with b.source_fs.open(fn, 'wb') as f:

                w = csv.writer(f)

                # FIXME This should not produce entries for non-table sources.
                w.writerow([dest_table.name] +
                           [s.source_table.name for s in sources if s.dest_table])

                for col_name, cols in columns.items():
                    w.writerow([col_name] + cols)

                prt('Wrote {}'.format(fn))

    elif args.build:
        # Coalesce the individual table maps into one colmap
        dest_tables = set([s.dest_table_name for s in b.sources
                           if s.dest_table])

        with b.source_fs.open(mapfile_name, 'wb') as cmf:

            w = csv.writer(cmf)

            w.writerow(('table', 'source', 'dest'))

            count = 0

            for dest_table_name in dest_tables:
                fn = 'colmap_{}.csv'.format(dest_table_name)

                if not b.source_fs.exists(fn):
                    continue

                with b.source_fs.open(fn, 'rb') as f:

                    r = csv.reader(f)

                    source_tables = next(r)
                    dtn = source_tables.pop(0)
                    assert dtn == dest_table_name

                    for row in r:
                        dest_col = row.pop(0)
                        for source, source_col in zip(source_tables, row):

                            if source_col and dest_col != source_col:
                                count += 1
                                w.writerow((source, source_col, dest_col))

            prt('Wrote {} mappings to {}'.format(count, mapfile_name))

    elif args.load:
        # Load the single colmap into the database.
        with b.source_fs.open(mapfile_name, 'rb') as cmf:
            r = csv.DictReader(cmf)

            for row in r:

                st = b.source_table(row['table'])

                c = st.column(row['source'])

                if c.dest_header != row['dest']:
                    prt('{}: {} -> {}'.format(st.name, c.dest_header, row['dest']))
                    c.dest_header = row['dest']

        b.build_source_files.file(File.BSFILE.SOURCESCHEMA).objects_to_record()
        b.sync_out()

        b.commit()

    else:
        fatal('No option given')


def bundle_test(args, l, rc):
    b = using_bundle(args, l).cast_to_subclass()

    b.run_tests(args.tests)


def bundle_docker(args, l, rc):
    import os
    import sys
    from docker.errors import NotFound, NullResource

    username, dsn, volumes_c, db_c, envs = get_docker_links(rc)

    b = using_bundle(args, l, print_loc=False)
    client = docker_client()

    if args.container:
        last_container = args.container
    else:
        try:
            last_container = b.buildstate.docker.last_container
        except KeyError:
            last_container = None

    try:
        inspect = client.inspect_container(last_container)

    except NotFound:
        # OK; the last_container is dead
        b.buildstate.docker.last_container = None
        b.buildstate.commit()
        inspect  = None
    except NullResource:
        inspect = None
        pass  # OK; no container specified in the last_container value

    #
    # Command args
    #

    bambry_cmd = ' '.join(args.args).strip()

    def run_container(bambry_cmd=None):
        """Run a new docker container"""


        if bambry_cmd:

            if last_container:
                fatal("Bundle already has a running container: {}\n{}".format(inspect['Name'], inspect['Id']))

            bambry_cmd_args = []

            if args.limited_run:
                bambry_cmd_args.append('-L')

            if args.multi:
                bambry_cmd_args.append('-m')

            if args.processes:
                bambry_cmd_args.append('-p' + str(args.processes))

            envs['AMBRY_COMMAND'] = 'bambry -i {} {} {}'.format(
                                    b.identity.vid, ' '.join(bambry_cmd_args), bambry_cmd)

            detach = True
        else:
            detach = False

        if args.limited_run:
            envs['AMBRY_LIMITED_RUN'] = '1'

        try:
            image_tag = rc.docker.ambry_image
        except KeyError:
            image_tag = 'civicknowledge/ambry'

        if args.version:
            import ambry._meta
            image = '{}:{}'.format(image_tag,ambry._meta.__version__)
        else:
            image = image_tag

        try:
            volumes_from = [rc.docker.volumes_from]
        except KeyError:
            volumes_from = []

        volumes_from.append(volumes_c)

        host_config = client.create_host_config(
            volumes_from=volumes_from,
            links={
                db_c:'db'
            }
        )

        kwargs = dict(
            image=image,
            detach=detach,
            tty=not detach,
            stdin_open=not detach,
            environment=envs,
            host_config=host_config
        )

        prt('Starting container with image {} '.format(image))

        r = client.create_container(**kwargs)

        client.start(r['Id'])

        return r['Id']

    if args.kill:

        if last_container:

            if inspect and inspect['State']['Running']:
                client.kill(last_container)

            client.remove_container(last_container)

            prt('Killed {}', last_container)

            b.buildstate.docker.last_container = None
            b.buildstate.commit()
            last_container = None
        else:
            warn('No container to kill')

    if bambry_cmd:
        # If there is command, run the container first so the subsequent arguments can operate on it
        last_container = run_container(bambry_cmd)
        b.buildstate.docker.last_container = last_container
        b.buildstate.commit()

    if args.docker_id:
        if last_container:
            prt(last_container)

        return

    elif args.docker_name:

        if last_container:
            prt(inspect['Name'])

        return

    elif args.logs:

        if last_container:
            for line in client.logs(last_container, stream=True):
                print line,
        else:
            fatal('No running container')

    elif args.stats:

        for s in client.stats(last_container, decode=True):

            sys.stderr.write("\x1b[2J\x1b[H")
            prt_no_format(s.keys())
            prt_no_format(s['memory_stats'])
            prt_no_format(s['cpu_stats'])

    elif args.shell:
        # Run a shell on a container
        # This is using execlp rather than the docker API b/c we want to entirely replace the
        # current process to get a good tty.
        os.execlp('docker', 'docker', 'exec', '-t','-i', last_container, '/bin/bash')

    elif not bambry_cmd and not args.kill:
        # Run a container and then attach to it.
        cid = run_container()

        os.execlp('docker', 'docker', 'attach', cid)


def bundle_log(args, l, rc):
    b = using_bundle(args, l)
    from ambry.util import drop_empty
    from itertools import groupby
    from collections import defaultdict
    from ambry.orm import Partition
    from tabulate import tabulate


    if args.exceptions:
        print '=== EXCEPTIONS ===='
        for pr in b.progress.exceptions:
            prt_no_format('===== {} ====='.format(str(pr)))
            prt_no_format(pr.exception_trace)

    elif args.progress:
        print '=== PROGRESS ===='
        from ambry.orm import Process
        import time
        from collections import OrderedDict
        from sqlalchemy.sql import and_


        records = []

        def append(pr, edit=None):

            if not isinstance(pr, dict):
                pr = pr.dict

            d = OrderedDict((k, str(v).strip()[:60]) for k, v in pr.items() if k in
                            ['id', 'group', 'state', 'd_vid', 's_vid', 'hostname', 'pid',
                             'phase', 'stage', 'modified', 'item_count',
                             'message'])

            d['modified'] = round(float(d['modified']) - time.time(),1)


            if edit:
                for k, v in edit.items():
                    d[k] = v(d[k])

            if not records:
                records.append(d.keys())

            records.append(d.values())

        q = b.progress.query.order_by(Process.modified.desc())

        for pr in q.all():
            # Don't show reports that are done or older than 2 minutes.
            if args.all or (pr.state != 'done' and pr.modified > time.time() - 120):
                append(pr)

        # Add old running rows, which may indicate a dead process.
        q = (b.progress.query.filter(Process.s_vid != None)
             .filter(and_(Process.state == 'running',Process.modified < time.time() - 60)))

        for pr in q.all():
            append(pr, edit={'modified': lambda e: (str(e)+' (dead?)') })

        records = drop_empty(records)

        if records:
            prt_no_format(tabulate(sorted(records[1:], key=lambda x: x[5]),records[0]))

    if args.stats:
        print '=== STATS ===='
        ds = b.dataset
        key_f = key=lambda e: e.state
        states = set()
        d = defaultdict(lambda: defaultdict(int))

        for state, sources in groupby(sorted(ds.sources, key=key_f), key_f):
            d['Sources'][state] = sum(1 for _ in sources) or None
            states.add(state)

        key_f = key = lambda e: (e.state, e.type)

        for (state, type), partitions in groupby(sorted(ds.partitions, key=key_f), key_f):
            states.add(state)
            if type == Partition.TYPE.UNION:
                d['Partitions'][state] = sum(1 for _ in partitions) or None
            else:
                d['Segments'][state] = sum(1 for _ in partitions) or None

        headers = sorted(states)
        rows = []

        for r in ('Sources','Partitions','Segments'):
            row = [r]
            for state in headers:
                row.append(d[r].get(state,''))
            rows.append(row)

        if rows:
            prt_no_format(tabulate(rows, headers))








