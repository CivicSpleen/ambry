# coding: utf-8
"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""


from ..cli import prt, fatal, warn, err


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




def bundle_parser(cmd):
    import multiprocessing
    import argparse

    parser = cmd.add_parser('bundle', help='Manage bundle files')
    parser.set_defaults(command='bundle')

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

    # Info command
    command_p = sub_cmd.add_parser('info', help='Print information about the bundle')
    command_p.set_defaults(subcommand='info')
    command_p.set_defaults(subcommand='info')
    command_p.add_argument('-s', '--schema', default=False, action="store_true",
                           help='Dump the schema as a CSV. The bundle must have been prepared')
    command_p.add_argument('-d', '--dep', default=False, help='Report information about a dependency')
    command_p.add_argument('-S', '--stats', default=False, action="store_true",
                           help='Also report column stats for partitions')
    command_p.add_argument('-P', '--partitions', default=False, action="store_true",
                           help='Also report partition details')

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

    command_p.add_argument('-c', '--clean', default=False, action="store_true", help='Clean first')
    command_p.add_argument('-f', '--fast', default=False, action="store_true",
                           help='Load the schema faster by not checking for extant columns')

    #
    # Prepare Command
    #
    command_p = sub_cmd.add_parser('prepare', help='Prepare by creating the database and schemas')
    command_p.set_defaults(subcommand='prepare')

    command_p.add_argument('-c', '--clean', default=False, action="store_true", help='Clean first')
    command_p.add_argument('-r', '--rebuild', default=False, action="store_true",
                           help='Rebuild the schema, but dont delete built files')
    command_p.add_argument('-F', '--fast', default=False, action="store_true",
                           help='Load the schema faster by not checking for extant columns')
    command_p.add_argument('-f', '--force', default=False, action="store_true",
                           help='Force build. ( --clean is usually preferred ) ')
    #
    # Build Command
    #
    command_p = sub_cmd.add_parser('build', help='Build the data bundle and partitions')
    command_p.set_defaults(subcommand='build')
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
    command_p.add_argument(
        '-l',
        '--library',
        help='Name of the library, defined in the config file')
    command_p.add_argument(
        '-f',
        '--force',
        default=False,
        action="store_true",
        help='Force storing the file')

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

    #
    # Source Commands
    #

    command_p = sub_cmd.add_parser('commit', help='Commit the source')
    command_p.set_defaults(subcommand='commit', command_group='source')
    command_p.add_argument('-m', '--message', default=None, help='Git commit message')

    command_p = sub_cmd.add_parser('push', help='Commit and push to the git origin')
    command_p.set_defaults(subcommand='push', command_group='source')
    command_p.add_argument('-m', '--message', default=None, help='Git commit message')

    command_p = sub_cmd.add_parser('pull', help='Pull from the git origin')
    command_p.set_defaults(subcommand='pull', command_group='source')


def bundle_info(args, b, st, rc):
    raise NotImplementedError()

    import textwrap
    from ambry.orm.exc import DatabaseMissingError

    indent = "    "

    def hprt(k):
        prt(u"-----{:s}-----",format(k))

    def lprt(k,*v):
        prt(u"{:10s}: {}".format(k,*v))

    def wprt(k, *v):
        prt(u"{:20.20s}: {}".format(k, *v))

    def iprt(k,*v):
        prt(indent+u"{:10s}: {}".format(k,*v))

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
        from ambry.util.datestimes import compress_years

        lprt('Title', b.metadata.about.title)
        lprt('Summary', b.metadata.about.summary)
        lprt("VID", b.identity.vid)
        lprt("VName", b.identity.vname)
        lprt("DB",b.database.path)

        lprt('Geo cov', str(list(b.metadata.coverage.geo)))
        lprt('Grain cov', str(list(b.metadata.coverage.grain)))
        lprt('Time cov', compress_years(b.metadata.coverage.time))

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
                if args.stats:
                    wrapper = textwrap.TextWrapper()

                    lprt("Stats",'')
                    wprt('Col Name', "{:>7s} {:>7s} {:>10s} {:70s}" .format(
                            "Count",'Uniq','Mean', 'Sample Values'))
                    for col_name, s in p.stats.__dict__.items():

                        if s.uvalues:
                            vals = (u'\n' + u' ' * 49).join(wrapper.wrap(u','.join(s.uvalues.keys()[:5])))
                        elif 'values' in s.hist:

                            parts = u' ▁▂▃▄▅▆▇▉'

                            def sparks(nums):  # https://github.com/rory/ascii_sparks/blob/master/ascii_sparks.py
                                nums = list(nums)
                                fraction = max(nums) / float(len(parts) - 1)
                                if fraction:
                                    return ''.join(parts[int(round(x / fraction))] for x in nums)
                                else:
                                    return ''

                            vals = sparks(int(x[1]) for x in s.hist['values'])
                        else:
                            vals = ''

                        wprt(col_name, u"{:>7s} {:>7s} {:>10s} {:70s}". format(
                                str(s.count) if s.count else '',
                                str(s.nuniques) if s.nuniques else '',
                                '{:10.2e}'.format(s.mean) if s.mean else '',
                                vals
                            ))


def bundle_clean(args, b, st, rc):
    raise NotImplementedError()

    b.log("---- Cleaning ---")
    # Only clean the meta phases when it is explicityly specified.
    # b.clean(clean_meta=('meta' in phases))
    b.database.enable_delete = True
    b.clean()


def bundle_meta(args, b, st, rc):
    raise NotImplementedError()

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
    raise NotImplementedError()
    return b.do_prepare()


def bundle_build(args, b, st, rc):
    raise NotImplementedError()

    r = b.do_build()

    # Closing is generally important. In this case, if the bundle isn't closed, and the bundle is installed below,
    # the config table won't have metadata. No idea why.
    b.close()

    if args.install:
        bundle_install(args, b, st, rc)

    return r


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


def bundle_config(args, b, st, rc):

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


def bundle_config_scrape(args, b, st, rc):

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


def root_meta(args, l, rc):
    ident = l.resolve(args.term)

    if not ident:
        fatal("Failed to find record for: {}", args.term)
        return

    b = l.get(ident.vid)

    meta = b.metadata

    if not args.key:
        # Return all of the rows
        if args.yaml:
            print meta.yaml

        elif args.json:
            print meta.json

        elif args.key:
            for row in meta.rows:
                print '.'.join([e for e in row[0] if e]) + '=' + str(row[1] if row[1] else '')
        else:
            print meta.yaml

    else:

        v = None
        from ..util import AttrDict

        o = AttrDict()
        count = 0

        for row in meta.rows:
            k = '.'.join([e for e in row[0] if e])
            if k.startswith(args.key):
                v = row[1]
                o.unflatten_row(row[0], row[1])
                count += 1

        if count == 1:
            print v

        else:
            if args.yaml:
                print o.dump()

            elif args.json:
                print o.json()

            elif args.rows:
                for row in o.flatten():
                    print '.'.join([e for e in row[0] if e]) + '=' + str(row[1] if row[1] else '')

            else:
                print o.dump()

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






