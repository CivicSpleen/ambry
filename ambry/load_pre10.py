#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import yaml

import msgpack

from fs.opener import fsopendir

from six import iteritems

from ambry.bundle import Bundle
from ambry.cli.bundle import check_built
from ambry.library import new_library
from ambry.orm.exc import NotFoundError
from ambry.orm.file import File
from ambry.run import get_runconfig


'''
Script loads to ambry and check bundles converted with pre10/to_v1.py script.

Usage:
    python load_pre10.py <file_or_dir>
'''


USAGE = '''
python load_pre10.py <file_or_dir>
'''

# List of bundles loaded without errors sorted by name.
# FIXME: Sort by name.
SUCCESS_LIST = [
    'bls.gov-laus-decomp/bls.gov-laus-decomp.yaml',
]


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

    if 'build' in config and 'sources' in config['build']:
        # create sources file.
        rows = [
            ['name', 'title', 'dest_table_name', 'time', 'space', 'grain',
             'description', 'url', 'urltype', 'filetype',
             'start_line', 'end_line',
             'segment']
        ]
        for i, (name, source) in enumerate(iteritems(config['build']['sources'])):
            start_line = None
            end_line = None
            if 'row_spec' in source:
                start_line = int(source['row_spec'].get('data_start_line') or 0) or None
                end_line = int(source['row_spec'].get('data_end_line') or 0) or None
            row = [
                name, name, source.get('table') or name, source.get('time', config['about']['time']),
                source.get('space', config['about']['space']), source.get('grain', config['about']['grain']),
                source.get('description', ''), source['url'], source.get('urltype', ''),
                source.get('filetype'),
                start_line, end_line,
                source.get('segment')]
            assert len(row) == len(rows[0]), 'Length of the row does not match to the length of the header.'
            rows.append(row)

        file_const = File.BSFILE.SOURCES  # Or, another BSFILE for a different type of file.
        file_record = b.dataset.bsfile(file_const)
        file_record.mime_type = 'application/msgpack'
        file_record.update_contents(msgpack.packb(rows, encoding='utf-8'))
        b.commit()

    # Sync to objects
    b.sync_objects()
    b.sync(force='rtf', defaults=True)
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
