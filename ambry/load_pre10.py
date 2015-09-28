#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import yaml

from fs.opener import fsopendir

from ambry.bundle import Bundle
from ambry.cli.bundle import check_built
from ambry.library import new_library
from ambry.orm.exc import NotFoundError
from ambry.run import get_runconfig


'''
Script loads to ambry and check bundles converted with pre10/to_v1.py script.

Usage:
    python load_pre10.py <file_or_dir>
'''


USAGE = '''
python load_pre10.py <file_or_dir>
'''


def _import(file_full_name):
    """ Imports bundle with given file name. """
    print('Starting import...')
    dir_name = os.path.dirname(file_full_name)
    file_name = os.path.basename(file_full_name)
    fs = fsopendir(dir_name)
    rc = get_runconfig(None)
    library = new_library(rc)
    config = yaml.load(fs.getcontents(file_name))
    bid = config['identity']['id']

    try:
        b = library.bundle(bid)
        # FIXME: Handle force attribute.
        b = None

        # if not args.force:
        #     print('Skipping existing  bundle: {}'.format(b.identity.fqname))

    except NotFoundError:
        b = None

    if not b:
        b = library.new_from_bundle_config(config)
        print('Loading bundle: {}'.format(b.identity.fqname))
    else:
        print('Loading existing bundle: {}'.format(b.identity.fqname))

    b.set_file_system(source_url=os.path.dirname(fs.getsyspath(file_name)))
    b.sync_in()
    return b


def _ingest(b):
    print('Starting ingest...')
    force = True
    clean_files = True
    b.ingest(force=force, clean_files=clean_files)
    b.set_last_access(Bundle.STATES.INGEST)
    # FIXME: Validate ingest.


def _schema(b):
    print('Starting schema...')
    clean = True
    force = True
    b.schema(force=force, clean=clean)
    # FIXME: Validate schema.


def _build(b):
    print('Starting build...')
    force = True
    clean = True
    if not force:
        check_built(b)
    else:
        b.state = Bundle.STATES.PREPARED

    if clean:
        b.dataset.delete_partitions()
    b = b.cast_to_subclass()
    b.build(force=force)
    b.set_last_access(Bundle.STATES.BUILT)

    # FIXME: validate build.


def main():
    from optparse import OptionParser
    parser = OptionParser()
    (options, args) = parser.parse_args()
    if len(args) != 1:
        print(USAGE)
        return

    if args[0].endswith('.yaml'):
        # single bundle

        b = _import(args[0])
        _ingest(b)
        _schema(b)
        _build(b)

    else:
        # directory
        raise NotImplementedError()

if __name__ == '__main__':
    main()
