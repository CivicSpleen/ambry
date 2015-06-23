# -*- coding: utf-8 -*-
import logging

# TODO: Move that functions to the appropriate place.
from ambry.orm.config import Config
from ambry.metadata.proptree import Term, ListTerm, DictGroup
from ambry.metadata.schema import Top

logger = logging.getLogger(__name__)


def db_save(session, properties):
    """ Saves bundle settings to the database.

    Args:
        session (Session): database where to save top.
        properties (StructuredPropertyTree): bundle properties.
    """

    # Save root config for that dataset.
    dataset_vid = properties.names.vid
    root_config = Config(d_vid=dataset_vid)
    session.add(root_config)
    session.commit()

    # Save all child configs.
    for member in properties._members.iteritems():
        name, group_or_term = member
        if isinstance(group_or_term, Term):
            _save_term(session, member, properties.names.vid, properties, root_config)
        elif isinstance(group_or_term, DictGroup):
            _save_group(session, member, properties.names.vid, properties, root_config)


def db_retrieve(session, dataset_vid):
    """ Collects bundle properties from db.

    Args:
        session (Session): sqlalchemy session used to retrieve settings.
        dataset_vid (str): vid of the dataset which properties to retrieve.

    Returns:
        StructuredPropertyTree
    """
    prop_tree = Top()

    configs = session.query(Config)\
        .filter_by(d_vid=dataset_vid)\
        .all()

    configs_map = {x.id: x for x in configs}

    for config in configs:
        if not config.parent_id:
            # Skip root config
            continue

        if config.group:
            # Skip all groups
            continue

        # value found, populate.
        # FIXME: Populate settings without looking for path.
        path = _get_path(configs_map, prop_tree, config)
        _set_by_path(prop_tree, path, config.value)

    return prop_tree


def _save_group(session, group, dataset_vid, parent_group, parent_instance):
    """ Saves given group of the settings to the db.

    Args:
        session (Session): sqlalchemy session used to save instance.
        group (tuple): first element is name of the group, second is DictGroup instance.
        dataset_vid (str): dataset vid.
        parent_group (DictGroup): group's parent where to get value
        parent_instance (ambry.orm.Config): config used as parent while group saving.

    """

    logger.debug(u'Saving group: name={}, parent={}'.format(group[0], parent_group))
    group_instance = Config(d_vid=dataset_vid, parent_id=parent_instance.id, key=group[0])
    session.add(group_instance)
    session.commit()

    for member in group[1]._members.iteritems():
        name, group_or_term = member
        if isinstance(group_or_term, Term):
            _save_term(session, member, dataset_vid, getattr(parent_group, group[0]), group_instance)
        elif isinstance(group_or_term, DictGroup):
            _save_group(session, member, dataset_vid, getattr(parent_group, group[0]), group_instance)


def _save_term(session, term, dataset_vid, parent_group, parent_instance):
    """ Saves given term with setting to the db.

    Args:
        session (Session): sqlalchemy session used to save instance.
        term (tuple): first element is name of the setting, second is Term instance.
        dataset_vid (str): dataset vid.
        parent_group (DictGroup): term's parent where to get value.
        parent_instance (ambry.orm.Config): config used as parent while term saving.

    """
    value = getattr(parent_group, term[0])
    if isinstance(value, ListTerm):
        value = [x for x in value]
    logger.debug('Saving %s=`%s` term with %s parent'.format(term[0], value, parent_group))
    value_instance = Config(d_vid=dataset_vid, parent_id=parent_instance.id, key=term[0], value=value)
    session.add(value_instance)
    session.commit()


def _get_path(configs_map, prop_tree, config_instance):
    """ Returns path of the config in the tree. """
    if config_instance.parent_id is None:
        # root node found
        return ''
    parent = configs_map[config_instance.parent_id]
    return '{}.{}'.format(_get_path(configs_map, prop_tree, parent), config_instance.key)


def _set_by_path(prop_tree, path, value):
    """ Sets value by given path. """
    logger.debug('Setting {} to {}'.format(path, value))

    group = prop_tree
    parts = path.split('.')
    path, key = parts[0:-1], parts[-1]

    for name in path:
        if not name:
            continue
        group = getattr(group, name)
    setattr(group, key, value)
