"""Metadata objects for a bundle.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""
from collections import Mapping, OrderedDict, MutableMapping
import copy

from six import iteritems, iterkeys, itervalues, StringIO, text_type, binary_type, string_types, u
from six.moves.html_parser import HTMLParser

from sqlalchemy.orm import object_session

from ambry.orm.config import Config
from ambry.orm.exc import MetadataError

from ambry.util import get_logger

logger = get_logger(__name__)


class AttrDict(OrderedDict):
    def __init__(self, *argz, **kwz):

        super(AttrDict, self).__init__(*argz, **kwz)

    def __setitem__(self, k, v):
        super(AttrDict, self).__setitem__(k, AttrDict(v) if isinstance(v, Mapping) else v)

    def __getitem__(self, key):
        return super(AttrDict, self).__getitem__(key)

    def __getattr__(self, k):

        if not (k.startswith('__') or k.startswith('_OrderedDict__')):
            return self[k]
        else:
            return super(AttrDict, self).__getattr__(k)

    def __setattr__(self, k, v):
        if k.startswith('_OrderedDict__'):
            return super(AttrDict, self).__setattr__(k, v)

        self[k] = v

    def __iter__(self):
        for key in iterkeys(super(OrderedDict, self)):
            yield key

    @staticmethod
    def flatten_dict(data, path=tuple()):
        dst = list()
        for k, v in iteritems(data):
            k = path + (k,)
            if isinstance(v, Mapping):
                for v in v.flatten(k):
                    dst.append(v)
            else:
                dst.append((k, v))
        return dst

    def flatten(self, path=tuple()):
        return self.flatten_dict(self, path=path)

    def to_dict(self):
        root = {}
        val = self.flatten()
        for k, v in val:
            dst = root
            for slug in k[:-1]:
                if dst.get(slug) is None:
                    dst[slug] = dict()
                dst = dst[slug]
            if v is not None or not isinstance(dst.get(k[-1]), Mapping):
                dst[k[-1]] = v

        return root

    def dump(self, stream=None, map_view=None):
        import yaml
        from ..orm import MutationList, MutationDict
        from yaml.representer import RepresenterError
        from collections import defaultdict
        from ambry.util import MapView

        yaml.representer.SafeRepresenter.add_representer(
            MapView, yaml.representer.SafeRepresenter.represent_dict)

        yaml.representer.SafeRepresenter.add_representer(
            AttrDict, yaml.representer.SafeRepresenter.represent_dict)

        yaml.representer.SafeRepresenter.add_representer(
            OrderedDict, yaml.representer.SafeRepresenter.represent_dict)

        yaml.representer.SafeRepresenter.add_representer(
            defaultdict, yaml.representer.SafeRepresenter.represent_dict)

        yaml.representer.SafeRepresenter.add_representer(
            MutationDict, yaml.representer.SafeRepresenter.represent_dict)

        yaml.representer.SafeRepresenter.add_representer(
            set, yaml.representer.SafeRepresenter.represent_list)

        yaml.representer.SafeRepresenter.add_representer(
            MutationList, yaml.representer.SafeRepresenter.represent_list)

        yaml.representer.SafeRepresenter.add_representer(
            _ScalarTermS, yaml.representer.SafeRepresenter.represent_str)

        yaml.representer.SafeRepresenter.add_representer(
            _ScalarTermU, yaml.representer.SafeRepresenter.represent_str)

        if stream is None:
            stream = StringIO()

        d = self

        if map_view is not None:
            map_view.inner = d
            d = map_view

        try:
            yaml.safe_dump(d, stream, default_flow_style=False, indent=4, encoding='utf-8')
        except RepresenterError:
            import pprint

            pprint.pprint(self.to_dict())
            raise

        if isinstance(stream, StringIO):
            return stream.getvalue()


class StructuredPropertyTree(object):
    """A structure of dictionaries and lists that can have a defined "schema" that
    restricts what keys objects can have

    The structure consists of a collection of top level groups, each of this has a collection
    of inner terms. Some terms are Scalars, while other terms can hold lists or dicts.

    The StructuredPropertyTree is subclassed in meta.py and given Group properties. The Groups
    are subclassed to have Term properties. The terms can be acessed as keys, and the values are read
    from and written to the database as Config records with the type value set to 'metadata'

    So in a call like:
        ds = library.dataset(...)
        title = ds.metadata.about.title

    The value for title comes from a Config record with these values:
        type = 'metadata'
        group = 'about'
        key = 'title'

    The value is stored in the Config records as JSON.

    In a call like:
        ds = library.dataset(...)
        title = ds.metadata.external_documentation.dataset.url

    The config record has a two part key:
        type = 'metadata'
        group = 'external_documentation'
        key = 'dataset.url'

    In the current metadata schema, key have a maximum of two parts, but it is possible
    to have keys of any depth.

    Other things to note:

    - If a declared proptery value does not exist in the database, fetching it returns None
    - If the property is not declared ( as with top level keys in TypedDictGroup ) a KeyError is thrown.
    - If a value is set to None, the COnfig record for the associated key is deleted.

    """

    _members = None
    _term_values = None
    _errors = None
    _top = None

    _synonyms = None

    def __init__(self, d=None, path=None, synonyms=None):
        """ Object heirarchy for holding metadata.

        :param d:  Initial dict of values.
        :param path: File path for writing data to yaml
        :param synonyms: Mapping for values that will be considered the same
        :param context: Context for Jinja2 template conversion in SalarTerms
        :return:
        """

        self._top = self

        self._errors = {}

        self._cached_configs = {}  # key is parent config, values are children of that parent.

        self._term_values = AttrDict()

        self.register_members()
        self._config = None  # appropriate orm.config.Config instance
        self._valid_configs = []  # Configs exist in both - db and StructuredPropertyTree
        self.set(d)

    def _add_valid(self, config):
        """ """
        self._valid_configs.append(config)

    def set(self, d):
        if d is not None:
            self._valid_configs = [self._config]
            for k, v in iteritems(d):
                if k in self._members:
                    m = self._members[k]
                    o = copy.copy(m)
                    o.init_instance(self)
                    o.set(v)
                else:
                    raise MetadataError('Undeclared group: {} '.format(k))

            # delete instances that were not updated as inactive.
            session = object_session(self._config)
            valid_ids = [x.id for x in self._valid_configs]
            missed_configs = session\
                .query(Config)\
                .filter(~Config.id.in_(valid_ids),
                        Config.d_vid == self._config.dataset.vid,
                        Config.type == self._type)\
                .all()

            for conf in missed_configs:
                logger.debug('Deleting {} config from database because it was removed from file.'.format(conf))
                session.delete(conf)
                session.commit()

    def _get_path(self):
        """ Returns tuple with full path. """
        return tuple()

    def build_from_db(self, dataset):
        logger.debug(
            'Building property tree from db. dataset: {}, type: {}'.format(dataset.vid, self._type))
        session = object_session(dataset)

        # optimization to use only one db hit.
        instance_to_path_map = {}  # key is Config instance, value if tuple with path.

        configs = session.query(Config)\
            .filter_by(d_vid=dataset.vid, type=self._type)\
            .all()

        # optimization: cache all configs to reduce db hits
        for config in configs:

            path = []
            current = config
            while current.parent:
                # iterating until the root.
                if current.parent and current.parent.key:
                    path.insert(0, current.parent.key)
                current = current.parent

            if config.key:
                path.append(config.key)
            self._cached_configs[tuple(path)] = config
            instance_to_path_map[config] = tuple(path)

        # populate all keys of the tree with appropriate values from db.
        for config in configs:
            if not config.parent_id:
                # Skip root config
                self._config = config
                continue

            if config.group:
                # Skip all groups
                continue

            # value found, populate.
            _set_value(instance_to_path_map, self._cached_configs, self, config)

        # tree is bound after build from db.
        self.link_config(session, dataset)

    def is_bound(self):
        """ Returns True if poperty tree is bound to the db. Otherwise returns False. """
        return self._config is not None

    def link_config(self, session, dataset):
        logger.debug(
            'Binding top level config to the db. dataset: {}, type: {}'.format(dataset.vid, self._type))
        self._config, created = _get_config_instance(
            self, session,parent_id=None, d_vid=dataset.vid,type=self._type, dataset=dataset)

        if created:
            logger.debug(
                'New top level config created and bound. config: {}'.format(self._config))
        else:
            logger.debug(
                'Existing top level config bound. config: {}'.format(self._config))

    def register_members(self):
        """Collect the names of the class member and convert them to object
        members.

        Unlike Terms, the Group class members are converted into object
        members, so the configuration data

        """

        self._members = {
            name: attr for name, attr in iteritems(type(self).__dict__) if isinstance(attr, Group)}

        for name, m in iteritems(self._members):
            m.init_descriptor(name, self)

    def __getattr__(self, k):
        if k.startswith('_'):
            object.__getattribute__(self, k)
        else:

            return self._term_values[k]

    def __getitem__(self, item):
        return self.__getattr__(item)

    @property
    def errors(self):
        return self._errors

    @property
    def dict(self):
        return self._term_values.to_dict()

    @property
    def flat(self):
        return self._term_values.flatten()

    @property
    def kv(self):
        """Return a fattened set of key vlaue pairs, where heirarchical keys are
        in dotten format"""

        for key, value in self.flat:
            yield '.'.join(key), value

    @property
    def json(self):
        return self._term_values.json()

    @property
    def yaml(self):
        return self._term_values.dump()

    def add_error(self, group, term, sub_term, value):
        """For records that are not defined as terms, either add it to the
        errors list."""

        self._errors[(group, term, sub_term)] = value

    def dump(self, stream=None, map_view=None, keys=None):

        if map_view is None and keys is not None:
            from ..util import MapView
            map_view = MapView(keys=keys)

        return self._term_values.dump(stream, map_view=map_view)

    def _jinja_sub(self, st):
        """Create a Jina template engine, then perform substitutions on a string"""

        if isinstance(st, string_types):
            from jinja2 import Template

            try:
                for i in range(5):  # Only do 5 recursive substitutions.
                    st = Template(st).render(**(self._top.dict))
                    if '{{' not in st:
                        break
                return st
            except Exception as e:
                return st
                #raise ValueError(
                #    "Failed to render jinja template for metadata value '{}': {}".format(st, e))

        return st

    def scalar_term(self, st):
        """Return a _ScalarTermS or _ScalarTermU from a string, to perform text and HTML substitutions"""
        if isinstance(st, binary_type):
            return _ScalarTermS(st, self._jinja_sub)
        elif isinstance(st, text_type):
            return _ScalarTermU(st, self._jinja_sub)
        elif st is None:
            return _ScalarTermU(u(''), self._jinja_sub)
        else:
            return st


class Group(object):
    """A group of terms. Groups are descriptors, so when they are acessed, as class variables, the
    return an object that is linked to the class object that contains them.

    Groups are linked to the group property in config records. Accessing a metadata group loads
    the whole group from the database.

    So,
        ds = library.dataset(...)
        ident = ds.metadata.identity

    Will load all of the Config items that have a type='metadata' and group='identity'. The group
    itself doesn't exist in the database.

    Group subclass objects create Config records when they are set, and return None when one is
    referenced that does not exist. The group will delete Config records when the correcsponding key
    is deleted or set to None; the database does not store Config records with None values

    """

    _key = None
    _fqkey = None
    _members = None

    # Set in init_instance
    _parent = None
    _top = None

    def __init__(self):
        """ """

        self._members = {
            name: attr for name,  attr in iteritems(type(self).__dict__)
            if isinstance(attr, Term) and not name.startswith('_')}

        self._parent = None
        self._top = None
        self._key = None
        self._config = None  # appropriate orm.config.Config instance of the group

    def _get_path(self):
        """ Returns tuple with full path (including the key). """
        return self._parent._get_path() + (self._key,)

    def update_config(self):
        """ Updates or creates config of that group. Requires tree bound to db. """
        dataset = self._top._config.dataset
        session = object_session(self._top._config)
        logger.debug(
            'Updating group config. dataset: {}, type: {}, key: {}'.format(dataset.vid, self._top._type, self._key))

        self._config, created = _get_config_instance(
            self, session,
            parent_id=self._parent._config.id, d_vid=dataset.vid,
            group=self._key, key=self._key, type=self._top._type, dataset = dataset)
        if created:
            self._top._cached_configs[self._get_path()] = self._config
        self._top._add_valid(self._config)

        if created:
            logger.debug(
                'New group config created and linked. config: {}'.format(self._config))
        else:
            logger.debug(
                'Existing group config linked. config: {}'.format(self._config))

    def init_descriptor(self, key, top):
        self._key = key
        self._fqkey = key

        for name, m in iteritems(self._members):
            m.init_descriptor(name, top)

    def init_instance(self, parent):
        self._parent = parent
        self._top = parent._top

    def get_term_instance(self, key):
        try:
            m = self._members[key]
        except KeyError:
            # TODO: Is it really attribute error?
            raise AttributeError('{} group does not have {} member'.format(self, key))
        o = copy.copy(m)
        o.init_instance(self)
        return o

    def get_group_instance(self, parent):
        """Create an instance object"""
        o = copy.copy(self)
        o.init_instance(parent)
        return o

    def __set__(self, instance, v):
        '''Called when a whole group is set'''

        assert isinstance(v, dict), 'Dictionary is required to set the whole group.'
        o = self.get_group_instance(instance)
        o.set(v)

    def __get__(self, instance, owner):
        """Descriptor that returns the group instance. """
        return self.get_group_instance(instance)

    @property
    def _term_values(self):
        raise NotImplementedError()

    def set(self, d):
        raise NotImplementedError(type(self))  # Can't change the groups at the top level.

    def __setattr__(self, attr, value):
        # Allows access to set _key, _member, etc
        if attr not in dir(self):
            raise AttributeError('{} group does not have {} term'.format(self, attr))

        return object.__setattr__(self, attr, value)


class DictGroup(Group, MutableMapping):
    """A group that holds key/value pairs.

    The identity group is a dict group, with single values under the group:

        identity:
            dataset: dataset
            id: diEGPXmDC8
            revision: 1

    These terms are mapped to two-part configs:

        identity.dataset
        identity.id
        identity.revision

    """

    def init_descriptor(self, key, top):
        super(DictGroup, self).init_descriptor(key, top)

        top._term_values[key] = AttrDict()

        for name, m in iteritems(self._members):
            top._term_values[key][name] = m.null_entry()
            m.init_descriptor(name, top)

    @property
    def _term_values(self):
        # This only works after being instantiated in __get__, which sets
        # _parent
        if self._key not in self._parent._term_values:
            self._parent._term_values[self._key] = {}

        return self._parent._term_values[self._key]

    def ensure_index(self, index):
        if index not in self._term_values:
            o = self.get_term_instance(index)
            self._term_values[index] = o.null_entry()

    def __setattr__(self, k, v):
        if k.startswith('_'):
            object.__setattr__(self, k, v)
        else:
            return self.__setitem__(k, v)

    def __getattr__(self, k):

        if k.startswith('_'):
            object.__getattribute__(self, k)
        else:
            return self.__getitem__(k)

    def __getitem__(self, key):

        o = self.get_term_instance(key)
        return o.get()

    def __setitem__(self, key, value):

        if key not in self._members:
            raise AttributeError('DictGroup does not have such term in {}: {}. Use one from: {}'
                                 .format(self._key, key, [k for k in iterkeys(self._members)]))

        o = self.get_term_instance(key)

        o.set(value)

    def __delitem__(self, key):

        return self._term_values.__delitem__(key)

    def __len__(self):
        return self._term_values.__len__()

    def __iter__(self):
        """Iterated over dictionary values, not Term instances."""
        return self._term_values.__iter__()

    def set(self, d):
        assert isinstance(d, dict)
        for k, v in iteritems(d):
            try:
                self.__setitem__(k, v)
            except AttributeError:
                self._top.add_error(self._key, k, None, v)


class TypedDictGroup(DictGroup):

    """A DictGroup where the term structure is constrained, but they keys are
    not.

    There must be one term, named 'proto', to set the type of the terms. Only works with DictTerms

    The external_documentation metadata is a TypedDictTerm:

        external_documentation:
            dataset:
                description: IQI Dataset Page
                source: null
                title: IQI Dataset Page
                url: http://www.oshpd.ca.gov/HID/Products/PatDischargeData/AHRQ/iqi-imi_overview.html
            download:
                description: Web page that links to the source files.
                source: null
                title: Download Page
                url: http://example.com


    Each inner block is a DictTerm, but the keys ( 'dataset' and 'docnload' ) are not defined by the schema.

    These entries will be mapped to three-part config records:

        external_documentation.dataset.description
        external_documentation.dataset.source
        ...
        external_documentation.download.description


    """

    def init_descriptor(self, key, top):
        super(DictGroup, self).init_descriptor(key, top)

        if '_proto' not in dir(self):
            raise AttributeError("TypeDictGroup must have a _proto Term")

        proto = type(self).__dict__['_proto']  # Avoids __get___?

        proto.init_descriptor('_proto', self._top)

    def get_term_instance(self, key):

        if '_proto' not in dir(self):
            raise AttributeError("TypeDictGroup must have a _proto Term")

        o = copy.copy(self._proto)
        o.init_instance(self)
        o._key = key
        o._fqkey = o._fqkey.replace('_proto', str(key))
        return o

    # This getter wil return a DictTerm, usually, so it is the DictTerm
    # that actually sets the value.
    def __getitem__(self, key):

        if key not in self._term_values:
            self._term_values[key] = {
                name: None for name,
                _ in iteritems(self._proto._members)}

        o = self.get_term_instance(key)
        return o.get()

    def __setitem__(self, key, value):

        o = self.get_term_instance(key)
        o.set(value)

class VarDictGroup(DictGroup):

    """A Dict group that doesnt' use terms to enforce a structure. It has top level  keys and values that
    are defined by the user.

    Dependencies are a var dict group:

        dependencies:
            requests: requests
            beautifulsoup: bs4

    These entries are stores as two-part configs:

        dependencies.requests
        dependencies.beautifulsoup


    """

    def __getattr__(self, name):

        if name.startswith('_'):
            raise AttributeError

        if name not in self._term_values:
            self._term_values[name] = AttrDict(_key=self._key)

        return self.__getitem__(name)

    def __getitem__(self, key):
        return self._term_values[key]

    def __setitem__(self, key, value):
        self._term_values[key] = value

        if self._top.is_bound():
            self.update_config(key, value)

    def update_config(self, key, value):
        """ Creates or updates db config of the VarDictGroup. Requires bound to db tree. """
        dataset = self._top._config.dataset
        session = object_session(self._top._config)
        logger.debug(
            'Updating VarDictGroup config. dataset: {}, type: {}, key: {}, value: {}'.format(
                dataset, self._top._type, key, value))

        if not self._parent._config:
            self._parent.update_config()

        # create or update group config
        self._config, created = get_or_create(
            session, Config,
            d_vid=dataset.vid, type=self._top._type,
            parent=self._parent._config, group=self._key,
            key=self._key,dataset=dataset)
        self._top._add_valid(self._config)

        # create or update value config
        config, created = get_or_create(
            session, Config, parent=self._config, d_vid=dataset.vid,
            type=self._top._type, key=key,dataset=dataset)

        if config.value != value:
            # sync db value with term value.
            config.value = value
            session.merge(config)
            session.commit()
            logger.debug(
                'Config bound to the VarDictGroup key updated. config: {}'.format(config))
        self._top._add_valid(config)


class Term(object):
    """A single term in a group."""

    _key = None
    _fqkey = None
    _parent = None  # set after being cloned in some subclass __get__
    _top = None
    _store_none = None
    _default = None
    _members = None
    _link_on_null = None
    _constraint = None

    def __init__(self, store_none=True, link_on_null=None, default=None, constraint=None):

        self._members = {
            name: attr for name, attr in iteritems(type(self).__dict__)
            if isinstance(attr, Term)}

        self._store_none = store_none
        self._default = default
        self._link_on_null = link_on_null
        self._constraint = constraint or []
        self._config = None  # Config instance of the term.

    def init_descriptor(self, key, top):
        assert(key is not None)
        self._key = key

    def init_instance(self, parent):
        self._parent = parent
        self._top = parent._top
        self._fqkey = self._parent._fqkey + '.' + self._key

    def _get_path(self):
        """ Returns tuple with full path (including the key). """
        return self._parent._get_path() + (self._key,)

    def update_config(self):
        """ Creates or updates db config of the term. Requires bound to db tree. """
        dataset = self._top._config.dataset
        session = object_session(self._top._config)

        #logger.debug('Updating term config. dataset: {}, type: {}, key: {}, value: {}'.format(
        #        dataset, self._top._type, self._key, self.get()))

        if not self._parent._config:
            self._parent.update_config()

        self._config, created = _get_config_instance(
            self, session,
            parent=self._parent._config, d_vid=dataset.vid,
            type=self._top._type, key=self._key, dataset=dataset)
        if created:
            self._top._cached_configs[self._get_path()] = self._config

        # We update ScalarTerm and ListTerm values only. Composite terms (DictTerm for example)
        # should not contain value.
        if isinstance(self, (ScalarTerm, ListTerm)):
            if self._config.value != self.get():
                self._config.value = self.get()
                session.merge(self._config)
                session.commit()
        self._top._add_valid(self._config)

        # Tese lines fail when the term includes unicode
        #if created:
        #    logger.debug('New config created and bound to the term. config: {}'.format(self._config))
        #else:
        #    logger.debug('Existing config bound to the term. config: {}'.format(self._config))

    def null_entry(self):
        raise NotImplementedError("Not implemented by {}".format(type(self)))

    def reformat(self, v):
        raise NotImplementedError("Not implemented by {}".format(type(self)))

    def __set__(self, instance, v):
        instance._term_values[self._key] = v

    def __get__(self, instance, owner):

        if self._key is None:
            raise Exception(self._key)
        else:
            try:
                return instance._term_values.get(self._key).get()
            except TypeError:
                return None

    def get(self):
        """Return the value type for this Term."""
        raise NotImplementedError("Not implemented by {}".format(type(self)))

    def set(self, v):
        raise NotImplementedError("Not implemented in {} ".format(type(self)))

    def is_empty(self):
        raise NotImplementedError('Not implemented in {} '.format(type(self)))

# For ScalarTerm.text()
# from http://stackoverflow.com/a/925630/1144479


class MLStripper(HTMLParser):

    def __init__(self):
        self.reset()
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return ''.join(self.fed)


class _ScalarTermS(binary_type):
    """A scalar term for extension for  strings, with support for Jinja substitutions"""

    def __new__(cls, string, jinja_sub):
        ob = super(_ScalarTermS, cls).__new__(cls, string)
        return ob

    def __init__(self, string, jinja_sub):
        super(_ScalarTermS, self).__init__()
        self.jinja_sub = jinja_sub

    @property
    def html(self):
        """Interpret the scalar as Markdown and return HTML"""
        import markdown

        return markdown.markdown(self.jinja_sub(self))

    @property
    def text(self):
        """Interpret the scalar as Markdown, strip the HTML and return text"""

        s = MLStripper()
        s.feed(self.html)
        return s.get_data()


class _ScalarTermU(text_type):
    """A scalar term for extension for unicode, with support for Jinja substitutions"""
    def __new__(cls, string, jinja_sub):
        ob = super(_ScalarTermU, cls).__new__(cls, string)
        return ob

    def __init__(self, string, jinja_sub):
        super(_ScalarTermU, self).__init__()
        self.jinja_sub = jinja_sub

    @property
    def html(self):
        """Interpret the scalar as Markdown and return HTML"""
        import markdown
        return markdown.markdown(self.jinja_sub(self))

    @property
    def text(self):
        """Interpret the scalar as Markdown, strip the HTML and return text"""

        s = MLStripper()
        s.feed(self.html)
        return s.get_data()


class ScalarTerm(Term):

    """A Term that can only be a string or number."""

    def set(self, v):
        # logger.debug(u'set term: {} = {}'.format(self._fqkey, v))
        if self._constraint and v not in self._constraint:
            raise ValueError('{} is not valid value. Use one from {}.'.format(v, self._constraint))
        self._parent._term_values[self._key] = v
        if self._top.is_bound():
            self.update_config()

    def get(self):
        st = self._parent._term_values.get(self._key, None)
        return self._top.scalar_term(st)

    def null_entry(self):
        return None

    def reformat(self, v):
        return v

    def is_empty(self):
        return self.get() is None


class DictTerm(Term, MutableMapping):

    """A term that contains a dict of sub-parts

    A value may be specified as a scalar, in which case it will be converted to a dict
    """

    default = None
    _store_none_map = None

    def init_descriptor(self, key, top):
        super(DictTerm, self).init_descriptor(key, top)

        for name, m in iteritems(self._members):
            m.init_descriptor(name, top)

        assert(self._key is not None)

        self._store_none_map = {name: m._store_none for name, m in iteritems(self._members)}

    def get_term_instance(self, key):
        m = self._members[key]
        o = copy.copy(m)
        o.init_instance(self)
        return o

    def _new_instance(self, parent):
        o = copy.copy(self)
        o.init_instance(parent)
        o._key = self._key

        return o

    def ensure_index(self, index):
        return

    def __set__(self, instance, d):

        o = self._new_instance(instance)
        o.set(d)

    def __get__(self, instance, owner):
        """"""
        # Instantiate a copy of this group, assign a specific Metadata instance
        # and return it.

        v = self._new_instance(instance)
        return v

    @property
    def _term_values(self):
        # This only works after being instantiated in __get__, which sets
        # _parent

        self._parent.ensure_index(self._key)

        tv = self._parent._term_values[self._key]

        if not tv:
            self._parent._term_values[self._key] = {}
            tv = self._parent._term_values[self._key]

        return tv

    def __setattr__(self, k, v):
        if k.startswith('_'):
            object.__setattr__(self, k, v)
        else:

            self.__setitem__(k, v)

    def __getattr__(self, k):

        if k.startswith('_'):
            return object.__getattribute__(self, k)
        else:
            return self.__getitem__(k)

    def __getitem__(self, key):

        if key not in dir(self):
            raise AttributeError('DictTerm does not have such term: {} '.format(key))

        o = self.get_term_instance(key)

        v = o.get()

        return v

    def __setitem__(self, k, v):
        if k not in iterkeys(self._members):
            self._top.add_error(self._parent._key, self._key, k, v)
            return

        o = self.get_term_instance(k)
        o.set(v)

    def __delitem__(self, key):
        try:
            return self._term_values.__delitem__(key)
        except KeyError:
            # From the external interface, DictTerms always appear to have keys, even when they really dont.
            pass

    def __len__(self):
        return self._term_values.__len__()

    def __iter__(self):

        if not self._term_values:
            return iter([])
        else:
            return self._term_values.__iter__()

    def set(self, d):
        for k, v in iteritems(d):
            self.__setitem__(k, v)

    @property
    def dict(self):

        if self._key not in self._parent._term_values:
            return None

        return self._term_values.to_dict()

    def get(self):
        return self

    def null_entry(self):

        d = AttrDict()

        for k, v in iteritems(self._members):
            if self._store_none_map[k] is True:
                d[k] = None

        return d

    def is_empty(self):
        return all([v is None for v in itervalues(self._term_values)])


class ListTerm(Term):

    """A Term that is always a list.

    The value may be specified as a scalar, in which case it will be
    converted to a list

    """

    def __set__(self, instance, v):

        if not isinstance(v, (list, tuple)):
            v = [v]

        instance._term_values[self._key] = list(v)
        if self._top.is_bound():
            self.update_config()

    def set(self, v):
        logger.debug('set list term: {} = {}'.format(self._fqkey, v))
        self.__set__(self._parent, v)

    def get(self):
        return self._term_values

    def null_entry(self):
        return []

    def is_empty(self):
        return all([v is None for v in self])

    @property
    def _term_values(self):
        # This only works after being instantiated in __get__, which sets
        # _parent

        if self._key not in self._parent._term_values:
            self._parent._term_values[self._key] = []

        tv = self._parent._term_values[self._key]

        assert tv is not None

        return tv

    def __getitem__(self, key):
        return self._term_values[key]

    def __setitem__(self, key, value):

        self._term_values[key] = value

    def __iter__(self):
        return iter(self._term_values)


def _set_value(instance_to_path_map, path_to_instance_map, prop_tree, config_instance):
    """ Finds appropriate term in the prop_tree and sets its value from config_instance.

    Args:
        configs_map (dict): key is id of the config, value is Config instance (AKA cache of the configs)
        prop_tree (PropertyDictTree): poperty tree to populate.
        config_instance (Config):

    """
    path = instance_to_path_map[config_instance]

    # find group
    group = prop_tree
    for elem in path[:-1]:
        group = getattr(group, elem)

    assert group._key == config_instance.parent.key
    setattr(group, config_instance.key, config_instance.value)

    #
    # bind config to the term
    #
    # FIXME: Make all the terms to store config instance the same way.
    term = getattr(group, config_instance.key)
    try:
        if hasattr(term, '_term'):
            # ScalarTermS and ScalarTermU case
            term._term._config = config_instance
            return
    except KeyError:
        # python3 case. TODO: Find the way to make it simple.
        pass

    try:
        if hasattr(term, '_config'):
            term._config = config_instance
            return
    except KeyError:
        # python3 case. TODO: Find the way to make it simple.
        pass
    else:
        pass  # the setting should have been handled by setattr(group, key, config.value)


def get_or_create(session, model, **kwargs):
    """ Get or create sqlalchemy instance.

    Args:
        session (Sqlalchemy session):
        model (sqlalchemy model):
        kwargs (dict): kwargs to lookup or create instance.

    Returns:
        Tuple: first element is found or created instance, second is boolean - True if instance created,
            False if instance found.
    """
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance, False
    else:
        instance = model(**kwargs)
        if 'dataset' in kwargs:
            instance.update_sequence_id(session, kwargs['dataset'])
        session.add(instance)
        session.commit()
        return instance, True


def _get_config_instance(group_or_term, session, **kwargs):
    """ Finds appropriate config instance and returns it.

    Args:
        group_or_term (Group or Term):
        session (Sqlalchemy session):
        kwargs (dict): kwargs to pass to get_or_create.

    Returns:
        tuple of (Config, bool):
    """
    path = group_or_term._get_path()
    cached = group_or_term._top._cached_configs.get(path)
    if cached:
        config = cached
        created = False
    else:
        # does not exist or not yet cached
        config, created = get_or_create(session, Config, **kwargs)
    return config, created
