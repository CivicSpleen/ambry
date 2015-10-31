# -*- coding: utf-8 -*-

""" Export datasets and partitions to CKAN. """

import ckanapi

from ambry.run import get_runconfig


MISSING_CREDENTIALS_MSG = '''Missing CKAN credentials.
HINT:
Add `ckan` section to the ~/.ambry_accounts.yaml. Example:

ckan:
    host: http://demo.ckan.org  # host with the ckan instance
    organization: org1  # default organization
    apikey: <apikey>  # your api key
'''

rc = get_runconfig()

CKAN_CONFIG = rc.accounts.get('ckan')

if CKAN_CONFIG and set(['host', 'organization', 'apikey']).issubset(CKAN_CONFIG.keys()):
    ckan = ckanapi.RemoteCKAN(
        CKAN_CONFIG.host,
        apikey=CKAN_CONFIG.apikey,
        user_agent='ambry/1.0 (+http://ambry.io)')
else:
    ckan = None


def export(dataset):
    """ Exports dataset to ckan instance.

    Args:
        dataset (ambry.orm.Dataset):

    """
    if not ckan:
        raise EnvironmentError(MISSING_CREDENTIALS_MSG)

    if dataset.config.metadata.about.access in ('internal', 'test', 'controlled', 'restricted', 'census'):
        # Never publish dataset with such access.
        raise Exception(
            '{} dataset can not be published because of {} access'
            .format(dataset.vid, dataset.config.metadata.about.access))

    # publish dataset
    ckan.action.package_create(**_convert_dataset(dataset))

    # publish partitions
    for partition in dataset.partitions:
        ckan.action.resource_create(**_convert_partition(partition))

    # publish schema.csv
    ckan.action.resource_create(**_convert_schema(dataset))


def is_exported(dataset):
    """ Returns True if dataset is already exported to CKAN. Otherwise returns False. """
    if not ckan:
        raise EnvironmentError(MISSING_CREDENTIALS_MSG)
    params = {'q': 'name:{}'.format(dataset.vid.lower())}
    resp = ckan.action.package_search(**params)
    return len(resp['results']) > 0


def _convert_dataset(dataset):
    """ Converts ambry dataset to dict ready to send to CKAN API.

    Args:
        dataset (orm.Dataset): dataset to convert.

    Returns:
        dict: dict to send to CKAN to create dataset.
            See http://docs.ckan.org/en/latest/api/#ckan.logic.action.create.package_create

    """
    # shortcut for metadata
    meta = dataset.config.metadata

    notes = ''

    for f in dataset.files:
        if f.path.endswith('documentation.md'):
            notes = f.contents
            break

    ret = {
        'name': dataset.vid.lower(),
        'title': meta.about.title,
        'author': meta.contacts.creator.name,
        'author_email': meta.contacts.creator.email,
        'maintainer': meta.contacts.maintainer.name,
        'maintainer_email': meta.contacts.maintainer.email,
        'license_id': '',
        'notes': notes,
        'url': meta.identity.source,
        'version': dataset.version,
        'state': 'active',
        'owner_org': CKAN_CONFIG['organization'],
    }
    return ret


def _convert_partition(partition):
    """ Converts partition to resource dict ready to save to CKAN. """
    # http://docs.ckan.org/en/latest/api/#ckan.logic.action.create.resource_create
    ret = {
        'package_id': partition.dataset.vid.lower(),
        'url': 'http://example.com',
        'revision_id': '',
        'description': '',
        'format': 'text/csv',
        'hash': '',
        'name': partition.name,
        'resource_type': '',
        'mimetype': '',
        'mimetype_inner': '',
        'webstore_url': '',
        'cache_url': '',
        'size': '',
        'created': '',
        'last_modified': '',
        'cache_last_updated': '',
        'webstore_last_updated': '',
        'upload': '',  # FIXME: Convert to CSV/KML or other.
    }

    return ret


def _convert_schema(dataset):
    """ Converts schema of the dataset to resource dict ready to save to CKAN. """
    # http://docs.ckan.org/en/latest/api/#ckan.logic.action.create.resource_create
    schema_csv = ''
    for f in dataset.files:
        if f.path.endswith('schema.csv'):
            schema_csv = f.contents

    ret = {
        'package_id': dataset.vid.lower(),
        'url': 'http://example.com',
        'revision_id': '',
        'description': 'Schema of the dataset tables.',
        'format': 'text/csv',
        'hash': '',
        'name': 'schema',
        'resource_type': '',
        'mimetype': '',
        'mimetype_inner': '',
        'webstore_url': '',
        'cache_url': '',
        'size': '',
        'created': '',
        'last_modified': '',
        'cache_last_updated': '',
        'webstore_last_updated': '',
        'upload': schema_csv,
    }

    return ret
