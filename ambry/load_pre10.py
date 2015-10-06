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
    'bls.gov-laus-decomp.yaml',
    'cdc.gov-mrfei.yaml',
    'cde.ca.gov-api-combined.yaml',
    'clarinova.com-dates-casnd.yaml',
    'sandiego.gov-businesses-casnd-orig.yaml',
    'clarinova.com-us_crime_incidents-state.yaml',
    'cde.ca.gov-api-combined.yaml',
    'cde.ca.gov-schools-combined.yaml',
    'civicknowledge.com-poverty.yaml',
    'civicknowledge.com-terms.yaml',
    'sandiego.gov-opendsd.yaml',
    'civicknowledge.com-housing.yaml',
    'exceptionalgrowth.org-nets-ca.yaml',
    'clarinova.com-streets-casnd.yaml',
    'clarinova.com-places-2013.yaml',
    'clarinova.com-parcels-casnd.yaml',
    'clarinova.com-street_lights-casnd.yaml',
    'sandiegodata.org-places-casnd.yaml',
    'example.com-level3.yaml',
    'clarinova.com-alcohol_licenses-casnd.yaml',
    'oshpd.ca.gov-psi.yaml',
    'example.com-altdb-orig.yaml',
    'example.com-random.yaml',
    'example.com-segmented-orig.yaml',
    'clarinova.com-2010census-sf1-casnd.yaml',
    'example.com-segmented.yaml',
    'abc.ca.gov-alcohol_licenses-orig.yaml',
    'oshpd.ca.gov-pqi.yaml',
    'clarinova.com-crosswalks-census-casnd.yaml',
    'clarinova.com-crime-incidents-casnd-geocoded.yaml',
    'cde.ca.gov-graduates.yaml',
    'example.com-simple-builder.yaml',
    'oshpd.ca.gov-facilities-index.yaml',
    'oshpd.ca.gov-msdrg-county.yaml',
    'bea.gov-international-transactions-orig.yaml',
    'sandiegodata.org-bad_addresses-casnd.yaml',
    'example.com-simple-orig.yaml',
    'sandag.org-popest.yaml',
    'civicknowledge.com-index-health_facilities-ca.yaml',
    'oshpd.ca.gov-patient_discharges-hospital.yaml',
    'clarinova.com-places-casnd.yaml',
    'cde.ca.gov-fitness-pivoted.yaml',
    'clarinova.com-collisions-casnd.yaml',
    'clarinova.com-crime-incidents-casnd-linked.yaml',
    'sangis.org-parcels-orig.yaml',
    'example.com-concurrency.yaml',
    'clarinova.com-geocode-casnd.yaml',
    'example.com-combined.yaml',
    'cde.ca.gov-api-orig.yaml',
    '211sandiego.org-calls-p1ye2014-dedupe.yaml',
]


def _import(file_full_name):
    """ Imports bundle with given file name. """
    print('Starting import {}...'.format(file_full_name))
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
                name, name, source.get('table') or name, source.get('time', config['about'].get('time')),
                source.get('space', config['about'].get('space')),
                source.get('grain', config['about'].get('grain')),
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
        success_list = []
        fail_list = []
        for dir_name, subdir_list, file_list in os.walk(args[0]):
            # convert to absolute path
            dir_name = os.path.abspath(dir_name)
            yaml_file = '{}.yaml'.format(dir_name.split('/')[-1])
            full_path = os.path.join(dir_name, yaml_file)
            if yaml_file in SUCCESS_LIST:
                continue
            if not os.path.exists(full_path):
                continue
            try:
                b = _import(full_path)
                _ingest(b)
                _schema(b)
                _build(b)
                success_list.append(yaml_file)
            except Exception as exc:

                fail_list.append((yaml_file, '{}: {}'.format(exc.__class__, exc)))
        open('success.txt', 'w').write('\n'.join(success_list))
        open('fails.txt', 'w').write('\n'.join(['{}, {}'.format(x, y) for (x, y) in fail_list]))
        print('Done:')

if __name__ == '__main__':
    main()
