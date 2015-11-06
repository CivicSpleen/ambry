# -*- coding: utf-8 -*-

""" Export datasets and partitions to CKAN. """

import json

import six

import unicodecsv

import ckanapi

from ambry.run import get_runconfig
from ambry.util import get_logger


MISSING_CREDENTIALS_MSG = '''Missing CKAN credentials.
HINT:
Add `ckan` section to the ~/.ambry_accounts.yaml. Example:

ckan:
    host: http://demo.ckan.org  # host with the ckan instance
    organization: org1  # default organization
    apikey: <apikey>  # your api key
'''


class UnpublishedAccessError(Exception):
    pass

logger = get_logger(__name__)

rc = get_runconfig()

CKAN_CONFIG = rc.accounts.get('ckan')


if CKAN_CONFIG and set(['host', 'organization', 'apikey']).issubset(list(CKAN_CONFIG.keys())):
    ckan = ckanapi.RemoteCKAN(
        CKAN_CONFIG.host,
        apikey=CKAN_CONFIG.apikey,
        user_agent='ambry/1.0 (+http://ambry.io)')
else:
    ckan = None


def export(bundle, force=False, force_restricted=False):
    """ Exports bundle to ckan instance.

    Args:
        bundle (ambry.bundle.Bundle):
        force (bool, optional): if True, ignore existance error and continue to export.
        force_restricted (bool, optional): if True, then export restricted bundles as private (for debugging
            purposes).

    Raises:
        EnvironmentError: if ckan credentials are missing or invalid.
        UnpublishedAccessError: if dataset has unpublished access - one from ('internal', 'test',
            'controlled', 'restricted', 'census').

    """
    if not ckan:
        raise EnvironmentError(MISSING_CREDENTIALS_MSG)

    # publish dataset.
    try:
        ckan.action.package_create(**_convert_bundle(bundle))
    except ckanapi.ValidationError:
        if force:
            logger.warning(
                '{} dataset already exported, but new export forced. Continue to export dataset stuff.'
                .format(bundle.dataset))
        else:
            raise

    # set permissions.
    access = bundle.dataset.config.metadata.about.access

    if access == 'restricted' and force_restricted:
        access = 'private'

    assert access, 'CKAN publishing requires access level.'

    if access in ('internal', 'test', 'controlled', 'restricted', 'census'):
        # Never publish dataset with such access.
        raise UnpublishedAccessError(
            '{} dataset can not be published because of {} access.'
            .format(bundle.dataset.vid, bundle.dataset.config.metadata.about.access))
    elif access == 'public':
        # The default permission of the CKAN allows to edit and create dataset without logging in. But
        # admin of the certain CKAN instance can change default permissions.
        # http://docs.ckan.org/en/ckan-1.7/authorization.html#anonymous-edit-mode
        user_roles = [
            {'user': 'visitor', 'domain_object': bundle.dataset.vid.lower(), 'roles': ['editor']},
            {'user': 'logged_in', 'domain_object': bundle.dataset.vid.lower(), 'roles': ['editor']},
        ]

    elif access == 'registered':
        # Anonymous has no access, logged in users can read/edit.
        # http://docs.ckan.org/en/ckan-1.7/authorization.html#logged-in-edit-mode
        user_roles = [
            {'user': 'visitor', 'domain_object': bundle.dataset.vid.lower(), 'roles': []},
            {'user': 'logged_in', 'domain_object': bundle.dataset.vid.lower(), 'roles': ['editor']}
        ]
    elif access in ('private', 'licensed'):
        # Organization users can read/edit
        # http://docs.ckan.org/en/ckan-1.7/authorization.html#publisher-mode
        # disable access for anonymous and logged_in
        user_roles = [
            {'user': 'visitor', 'domain_object': bundle.dataset.vid.lower(), 'roles': []},
            {'user': 'logged_in', 'domain_object': bundle.dataset.vid.lower(), 'roles': []}
        ]
        organization_users = ckan.action.organization_show(id=CKAN_CONFIG.organization)['users']
        for user in organization_users:
            user_roles.append({
                'user': user['id'], 'domain_object': bundle.dataset.vid.lower(), 'roles': ['editor']}),

    for role in user_roles:
        # http://docs.ckan.org/en/ckan-2.4.1/api/#ckan.logic.action.update.user_role_update
        ckan.action.user_role_update(**role)

    # FIXME: Using bulk update gives http500 error. Find the way and use bulk update instead of many requests.
    # http://docs.ckan.org/en/ckan-2.4.1/api/#ckan.logic.action.update.user_role_bulk_update - the same
    # ckan.action.user_role_bulk_update(user_roles=user_roles)

    # publish partitions
    for partition in bundle.partitions:
        ckan.action.resource_create(**_convert_partition(partition))

    # publish schema.csv
    ckan.action.resource_create(**_convert_schema(bundle))

    # publish external documentation
    for name, external in six.iteritems(bundle.dataset.config.metadata.external_documentation):
        ckan.action.resource_create(**_convert_external(bundle, name, external))


def is_exported(bundle):
    """ Returns True if dataset is already exported to CKAN. Otherwise returns False. """
    if not ckan:
        raise EnvironmentError(MISSING_CREDENTIALS_MSG)
    params = {'q': 'name:{}'.format(bundle.dataset.vid.lower())}
    resp = ckan.action.package_search(**params)
    return len(resp['results']) > 0


def _convert_bundle(bundle):
    """ Converts ambry bundle to dict ready to send to CKAN API.

    Args:
        bundle (ambry.bundle.Bundle): bundle to convert.

    Returns:
        dict: dict to send to CKAN to create dataset.
            See http://docs.ckan.org/en/latest/api/#ckan.logic.action.create.package_create

    """
    # shortcut for metadata
    meta = bundle.dataset.config.metadata

    notes = ''

    for f in bundle.dataset.files:
        if f.path.endswith('documentation.md'):
            contents = f.unpacked_contents
            if isinstance(contents, six.binary_type):
                contents = contents.decode('utf-8')
            notes = json.dumps(contents)
            break

    ret = {
        'name': bundle.dataset.vid.lower(),
        'title': meta.about.title,
        'author': meta.contacts.creator.name,
        'author_email': meta.contacts.creator.email,
        'maintainer': meta.contacts.maintainer.name,
        'maintainer_email': meta.contacts.maintainer.email,
        'license_id': '',
        'notes': notes,
        'url': meta.identity.source,
        'version': bundle.dataset.version,
        'state': 'active',
        'owner_org': CKAN_CONFIG['organization'],
    }
    return ret


def _convert_partition(partition):
    """ Converts partition to resource dict ready to save to CKAN. """
    # http://docs.ckan.org/en/latest/api/#ckan.logic.action.create.resource_create

    # convert bundle to csv.
    csvfile = six.StringIO()
    writer = unicodecsv.writer(csvfile)
    headers = partition.datafile.headers
    writer.writerow(headers)
    for row in partition:
        writer.writerow([row[h] for h in headers])
    csvfile.seek(0)

    # prepare dict.
    ret = {
        'package_id': partition.dataset.vid.lower(),
        'url': 'http://example.com',
        'revision_id': '',
        'description': partition.description or '',
        'format': 'text/csv',
        'hash': '',
        'name': partition.name,
        'resource_type': '',
        'mimetype': 'text/csv',
        'mimetype_inner': '',
        'webstore_url': '',
        'cache_url': '',
        'upload': csvfile
    }

    return ret


def _convert_schema(bundle):
    """ Converts schema of the dataset to resource dict ready to save to CKAN. """
    # http://docs.ckan.org/en/latest/api/#ckan.logic.action.create.resource_create
    schema_csv = None
    for f in bundle.dataset.files:
        if f.path.endswith('schema.csv'):
            contents = f.unpacked_contents
            if isinstance(contents, six.binary_type):
                contents = contents.decode('utf-8')
            schema_csv = six.StringIO(contents)
            schema_csv.seek(0)
            break

    ret = {
        'package_id': bundle.dataset.vid.lower(),
        'url': 'http://example.com',
        'revision_id': '',
        'description': 'Schema of the dataset tables.',
        'format': 'text/csv',
        'hash': '',
        'name': 'schema',
        'upload': schema_csv,
    }

    return ret


def _convert_external(bundle, name, external):
    """ Converts external documentation to resource dict ready to save to CKAN. """
    # http://docs.ckan.org/en/latest/api/#ckan.logic.action.create.resource_create
    ret = {
        'package_id': bundle.dataset.vid.lower(),
        'url': external.url,
        'description': external.description,
        'name': name,
    }

    return ret
