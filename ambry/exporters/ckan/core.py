# -*- coding: utf-8 -*-

""" Export datasets and partitions to CKAN. """

import ckanapi

ckan = ckanapi.RemoteCKAN(
    'http://demo.ckan.org',
    apikey='FIXME:',  # FIXME: Read API key from config.
    user_agent='ambry/1.0 (+http://ambry.io)')


def export(dataset):
    """ FIXME: """

    # publish dataset
    ckan.action.package_create(**_convert_dataset(dataset))

    # publish partitions
    for partition in dataset.partitions:
        ckan.action.resource_create(**_convert_partition(partition))

    # publish schema.csv
    if dataset.tables:
        ckan.action.resource_create(**_convert_schema(dataset))


def is_exported(dataset):
    """ Returns True if dataset is already exported to CKAN. Otherwise returns False. """
    pass


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

    ret = {
        'name': dataset.vid,
        'title': meta.about.title,
        'author': meta.contacts.creator.name,
        'author_email': meta.contacts.creator.email,
        'maintainer': meta.contacts.maintainer.name,
        'maintainer_email': meta.contacts.maintainer.email,
        'license_id': '',
        'notes': meta.about.summary,
        'url': meta.identity.source,
        'version': dataset.version,
        'state': 'active',
    }
    return ret


def _convert_partition(partition):
    """ Converts partition to resource dict ready to save to CKAN. """
    # http://docs.ckan.org/en/latest/api/#ckan.logic.action.create.resource_create
    ret = {
        'package_id': partition.dataset.vid,
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
    schema_csv = _get_schema_file(dataset)
    ret = {
        'package_id': dataset.vid,
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
        'upload': schema_csv.read(),
    }

    return ret


def _get_schema_file(dataset):
    """ Converts tables of the dataset to csv. Returns file-like. """
    # FIXME: Find better way to get schema.
    import unicodecsv
    from StringIO import StringIO
    csv_content = StringIO()
    writer = unicodecsv.writer(csv_content)
    writer.writerow(['table', 'datatype', 'size', 'column', 'description'])
    for table in dataset.tables:
        for column in table.columns:
            writer.writerow([table.name, column.datatype, column.size, column.name, column.description])
        writer.writerow(['', '', '', '', ''])
    csv_content.seek(0)
    return csv_content
