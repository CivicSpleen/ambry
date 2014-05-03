"""Metadata objects for a bundle

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import collections
from ..util import AttrDict, MapView
import copy

class Metadata(object):
    """A top level group of groups"""

    _members = None
    _term_values = None
    _errors = None
    _top = None
    _loaded = None
    _path = None

    def __init__(self, d=None, path=None):

        self._top = self
        self._path = path

        self._term_values = AttrDict()
        self._errors = {}
        self._loaded = set()

        self.register_members()

        self.set(d)

    def set(self, d):
        if d is not None:
            for k, v in d.items():
                if k in self._members:

                    m = self._members[k]
                    o = copy.deepcopy(m)
                    o.init_instance(self)

                    o.set(v)
                    self.mark_loaded(k)
                else:
                    # Top level groups that don't match a member group are preserved,
                    # not errors like unknown terms in a group.
                    self._term_values[k] = v

    def register_members(self):
        """Collect the names of the class member and convert them to object members.

        Unlike Terms, the Group class members are converted into object members, so the configuration data
        """
        import copy

        self._members = {name: attr for name, attr in type(self).__dict__.items() if isinstance(attr, Group)}

        for name, m in self._members.items():
            m.init_descriptor(name, self)

    def ensure_loaded(self, group):
        # Called from Group__get__ to ensure that the group is loaded

        if not self.is_loaded(group) and self._path is not None:
            self.load_group(group)
            self.mark_loaded(group)

        pass

    # For access to non term entries
    def __getattr__(self, k):
        if k.startswith('_'):
            object.__getattribute__(self, k)
        else:
            self.ensure_loaded(k)
            return self._term_values[k]

    @property
    def rows(self):
        """Return the configuration information flattened into database records """

        for k,v in  self._term_values.flatten():

            if len(k) == 1 and isinstance(v, list):
                for i,item in enumerate(v):
                    yield k+(i,None), item
            else:
                if len(k) == 2:
                    k = k + (None, )

                yield k,v

    def load_rows(self, rows):
        import json

        for  (group, term, sub_term), value in rows:

            try:
                v = json.loads(value)
            except ValueError:
                pass
            except TypeError:
                pass

            try:
                m = self._members[group]
            except KeyError:

                if group not in self._term_values:
                    self._term_values[group] = AttrDict()

                path = [term]
                if sub_term is not  None:
                    path += [sub_term]

                self._term_values[group].unflatten_row( path, value)
                continue

            m = self._members[group]
            o = copy.deepcopy(m)
            o.init_instance(self)

            o.set_row((term, sub_term), value)

            self.mark_loaded(group)

    @property
    def errors(self):
        return self._errors


    @property
    def dict(self):
        return self._term_values.to_dict()

    def add_error(self, group, term, sub_term, value):
        self._errors[(group, term, sub_term)]  =  value

    def mark_loaded(self, group):
        if self._path is not None:
            self._loaded.add(group)

    def is_loaded(self, group):
        return group in self._loaded

    def dump(self, stream = None, map_view = None):
        return self._term_values.dump(stream, map_view = map_view)


    def groups_by_file(self):
        """Returns a map of the files defined in groups, and the groups that define those files"""
        d = {}

        for name, m in self._members.items():
            file_ = m._file

            if not file_:
                file_ = 'bundle.yaml'

            if file_ not in d:
                d[file_] = []

            d[file_].append(m)

        return d

    def load_from_dir(self, path, group = None):
        '''Load groups from specified files. '''
        import os
        import yaml


        n_loaded = 0
        for file_, groups in self.groups_by_file().items():

            if group is not None and group not in [ g._key for g in groups]:
                continue

            fn = os.path.join(path, file_)

            if not os.path.exists(fn):
                n_loaded += 1 # so we don't try to load the non terms for files that are missing
                continue

            try:
                self._term_values.update_yaml(fn)
                n_loaded += 1

                for g in groups: # Each file causes multiple groups to load.
                    self.mark_loaded(g._key)


            except IOError:
                raise

        # Didn't find the group we intended to load, so try the non term file.
        if n_loaded == 0 and group is not None:
            if (hasattr(self, '_non_term_file')):
                fn = os.path.join(path, self._non_term_file)
                self._term_values.update_yaml(fn)

                self.mark_loaded(group)

                for file_, groups in self.groups_by_file().items():
                    if file_ == self._non_term_file:
                        for g in groups:  # Each file causes multiple groups to load.
                            self.mark_loaded(g._key)


    def load_group(self, group):
        self.load_from_dir(self._path, group)


    def write_to_dir(self, path=None):
        import os

        if path is None:
            path = self._path

        if path is None:
            raise ValueError("Must specify a path")

        non_term_keys = [key for key in self._term_values.keys() if key not in self._members]

        for file_, groups in self.groups_by_file().items():

            # If we are lazy-loading, only write the files that have at least on groups loaded
            # Actually, all of the groups should be loaded.
            if self._path and not any([ self.is_loaded(g._key) for g in groups]):
                continue

            fn = os.path.join(path, file_)
            dir_ = os.path.dirname(fn)

            if not os.path.isdir(dir_):
                os.makedirs(dir_)

            with open(fn, 'w+') as f:
                keys = [ g._key for g in groups]

                # Include the non-term keys when writing the non-term file
                if (hasattr(self, '_non_term_file')  and file_ == self._non_term_file and non_term_keys):
                    keys += non_term_keys

                self.dump(stream=f, map_view = MapView(keys = keys))


class Group(object):
    """A  group of terms"""

    _key = None
    _members = None

    _file = None
    _to_rows = None

    # Set in init_instance
    _parent = None
    _top = None


    def __init__(self, file=None, to_rows=True):

        self._members = {name: attr for name, attr in type(self).__dict__.items() if isinstance(attr, Term) and not name.startswith('_')}

        self._file = file
        self._to_rows = to_rows

    def init_descriptor(self,key, top):
        self._key = key

        for name, m in self._members.items():

            m.init_descriptor(name, top)

    def init_instance(self, parent):
        self._parent = parent
        self._top = parent._top

    def get_term_instance(self, key):
        m = self._members[key]
        o = copy.deepcopy(m)
        o.init_instance(self)
        return o

    def __set__(self, instance, v):
        assert isinstance(v, dict)

        instance._term_values[self._key].update(v)


    def __get__(self, instance, owner):
        # Instantiate a copy of this group, assign a specific Metadata instance
        # ans return it.
        import copy

        instance.ensure_loaded(self._key)

        o = copy.deepcopy(self)
        o.init_instance(instance)
        return o

    @property
    def _term_values(self):
        raise NotImplementedError()

    def set(self,d):
        raise NotImplementedError(type(self))


    def set_row(self, key, value):
        raise NotImplementedError(type(self))


    def __setattr__(self, attr, value):

        if attr not in dir(self):
            raise AttributeError("No such term: {} ".format(attr))

        return object.__setattr__(self, attr, value)



class DictGroup(Group, collections.MutableMapping):
    """A group that holds key/value pairs"""


    def init_descriptor(self, key, top):
        super(DictGroup, self).init_descriptor(key, top)

        top._term_values[key] = {}

        for name, m in self._members.items():
            top._term_values[key][name] = m.null_entry()
            m.init_descriptor(name, top)

    @property
    def _term_values(self):
        # This only works after being instantiated in __get__, which sets
        # _parent
        if not self._key in self._parent._term_values:
            self._parent._term_values[self._key] = {}

        return self._parent._term_values[self._key]


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
        return self._term_values.__getitem__(key)

    def __setitem__(self, key, value):
        if not key in self._members:
            raise AttributeError("No such term in {}: {}. Has: {}"
                                 .format(self._key, key, [key for key in self._members.keys()]))

        o = self.get_term_instance(key)
        o.set(value)


    def __delitem__(self, key):
        return self._term_values.__delitem__(key)

    def __len__(self):
        return self._term_values.__len__()

    def __iter__(self):
        return self._term_values.__iter__()


    def set(self, d):
        for k,v in d.items():
            try:
                self[k] = v
            except AttributeError as e:
                self._top.add_error(self._key, k, None, v)

    def set_row(self, key, value):
        key, subkey = key
        if subkey:
            self.set({key: { subkey: value}})
        else:
            self.set({key:  value})


class TypedDictGroup(DictGroup):
    """A DictGroup where the term structure is constrained, but they keys are not.

    There must be one term, named 'proto', to set the type of the terms.

    Only works with DictTerms
    """

    def init_descriptor(self, key, top):
        super(DictGroup, self).init_descriptor(key, top)

        if '_proto' not in dir(self):
            raise AttributeError("TypeDictGroup must have a _proto Term")

        proto = type(self).__dict__['_proto']

        proto.init_descriptor('_proto', self._top)

    # This getter wil return a DictTerm, usually, so it is the DictTerm
    # that actually sets the value.
    def __getitem__(self, name):

        if '_proto' not in dir(self):
            raise AttributeError("TypeDictGroup must have a _proto Term")

        if not name in self._term_values:
            self._term_values[name] =  { name:None for name, _ in self._proto._members.items() }

        o = copy.deepcopy(self._proto)
        o._key = name
        o.init_instance(self)

        return o

    def __setitem__(self, key, value):

        if '_proto' not in dir(self):
            raise AttributeError("TypeDictGroup must have a _proto Term")

        o = copy.deepcopy(self._proto)
        o._key = key
        o.init_instance(self)

        o.set(value)

        return o


class VarDictGroup(DictGroup):
    """A Dict group that doesnt' use terms to enforce a structure.
    """
    from ..util import AttrDict


    def __getattr__(self, name):
        import copy

        if name.startswith('_'):
            raise AttributeError

        if not name in self._term_values:
            self._term_values[name] =  AttrDict()


        return self.__getitem__(name)


    def __getitem__(self, key):

        return self._term_values[key]

    def __setitem__(self, key, value):
        self._term_values[key] = value


class ListGroup(Group, collections.MutableSequence):
    """A group that holds a list of DictTerms"""

    def __init__(self, file=None, to_rows=True):
        super(ListGroup, self).__init__(file=file, to_rows = to_rows)


    @property
    def _term_values(self):
        # This only works after being instantiated in __get__, which sets
        # _parent
        if not self._key in self._parent._term_values:
            self._parent._term_values[self._key] = []

        return self._parent._term_values[self._key]

    def insert(self, index, value):
        self._term_values.insert(index,value)

    def __set__(self, instance, v):
        assert isinstance(v, list)

        o = copy.deepcopy(self)
        o._key = self._key
        o.init_instance(instance)

        instance._term_values[self._key] = []

        o.set(v)

    def __getitem__(self, index):
        return self._term_values.__getitem__(index)

    def __setitem__(self, index, value):
        return self._term_values.__setitem__(index,value)

    def __delitem__(self, index):
        return self._term_values.__delitem__(index)

    def __len__(self):
        return self._term_values.__len__()

    def __iter__(self):
        return self._term_values.__iter__()

    def set(self,d):
        for item in d:
            self.append(item)

    def set_row(self, key, value):
        index, _ = key

        self.append(value)


class Term(object):
    """A single term in a group"""

    _key = None
    _parent = None # set after being cloned in some subclass __get__
    _top = None
    _synonym = None
    _show_none = None
    _default = None
    _members = None

    def __init__(self, synonym=None, show_none=True, default = None):

        self._members = {name: attr for name, attr in type(self).__dict__.items() if isinstance(attr, Term)}

        if synonym and not isinstance(synonym, (list, tuple)):
            synonym = (synonym,)

        self._synonym = tuple(synonym) if synonym else tuple()
        self._show_none = show_none
        self._default = default


    def init_descriptor(self, key, top):
        assert(key is not None)
        self._key = key

    def init_instance(self, parent):
        self._parent = parent
        self._top = parent._top

    def null_entry(self):
        raise NotImplementedError("Not implemented by {}".format(type(self)))

    def __set__(self, instance, v):
        instance._term_values[self._key] = v

    def __get__(self, instance, owner):
        if self._key is None:
            raise Exception(self._key)
        else:
            return instance._term_values.get(self._key)

    def get(self, instance, owner):
        '''A get that turns only dicts, lists and scalars, for use in creating dicts'''
        return instance._term_values.get(self._key)

    def set(self,v):
        raise NotImplementedError("Not implemented in {} ".format(type(self)))

class ScalarTerm(Term):
    """A Term that can only be a string"""

    def set(self, v):
        self._parent._term_values[self._key] = v

    def null_entry(self):
        return None


class DictTerm(Term, collections.MutableMapping):
    """A term that contains a dict of sub-parts

    A value may be specified as a scalar, in which case it will be converted to a dict
    """

    default = None

    def init_descriptor(self, key, top):
        super(DictTerm, self).init_descriptor(key, top)

        for name, m in self._members.items():
            m.init_descriptor(name, top)

        assert(self._key is not None)

    @property
    def _term_values(self):
        # This only works after being instantiated in __get__, which sets
        # _parent
        if not self._key in self._parent._term_values:
            self._parent._term_values[self._key] =  { name:None for name, _ in self._members.items() }

        return self._parent._term_values[self._key]


    def __setattr__(self, k, v):
        if k.startswith('_'):
            object.__setattr__(self, k, v)
        else:
            self.__setitem__(k, v)

    def __getattr__(self, k):
        if k.startswith('_'):
            return object.__getattr__(self, k)
        else:
            return self.__getitem__(k)

    def __getitem__(self, key):

        if key not in dir(self):
            raise AttributeError("No such term: {} ".format(key))

        return self._term_values.__getitem__(key)

    def __setitem__(self, key, value):
        if key not in dir(self):
            raise AttributeError("No such term: {} ".format(key))

        return self._term_values.__setitem__(key, value)

    def __delitem__(self, key):
        return self._term_values.__delitem__(key)

    def __len__(self):
        return self._term_values.__len__()

    def __iter__(self):
        return self._term_values.__iter__()

    def __set__(self, instance, d):

        if not self._key in instance._term_values:
            instance._term_values[self._key] = {name: None for name, _ in self._members.items()}

        for k, v in d.items():
            if k in instance._term_values[self._key]:
                instance._term_values[self._key][k] = d[k]
            else:
                self._top.add_error(self._parent._key, self._key, k, v)

    def set(self, d):

        self.__set__(self._parent, d)


    def __get__(self, instance, owner):
        """ """
        # Instantiate a copy of this group, assign a specific Metadata instance
        # and return it.
        import copy

        o = copy.deepcopy(self)
        o.init_instance(instance)
        o._key = self._key

        return o


    def null_entry(self):
        d = AttrDict()

        for k, v in self._members.items():
            d[k] = None

        return d


class ListTerm(Term):
    """A Term that is always a list.

    The value may be specified as a scalar, in which case it will be converted to a list"""

    def __set__(self, instance, v):

        if not isinstance(v, (list, tuple)):
            v = [v]

        instance._term_values[self._key] = list(v)

    def set(self, v):
        self.__set__(self._parent, v)


    def null_entry(self):
        return []