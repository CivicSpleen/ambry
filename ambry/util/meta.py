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

    def mark_loaded(self, group):
        if self._path is not None:
            self._loaded.add(group)

    def is_loaded(self, group):
        return group in self._loaded

    def load_all(self):
        """Load all of the files for all of the groups.

        Wont re-load groups that have already been loaded or altered. """
        for group in self._members.keys():
            self.ensure_loaded(group)


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

        for  row in rows:

            (group, term, sub_term), value = row

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


    def dump(self, stream = None, map_view = None, keys = None):

        if map_view is None and keys is not None:
            from ..util import MapView
            map_view = MapView(keys = keys )

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


    def write_to_dir(self, path=None, write_all=False):
        import os

        if path is None:
            path = self._path

        if path is None:
            raise ValueError("Must specify a path")

        non_term_keys = [key for key in self._term_values.keys() if key not in self._members]

        for file_, groups in self.groups_by_file().items():

            # If we are lazy-loading, only write the files that have at least on groups loaded
            # Actually, all of the groups should be loaded.
            if not write_all and self._path and not any([ self.is_loaded(g._key) for g in groups]):
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

    def get_group_instance(self, parent):

        o = copy.deepcopy(self)
        o.init_instance(parent)
        return o

    def __set__(self, instance, v):
        assert isinstance(v, dict)

        instance._term_values[self._key].update(v)


    def __get__(self, instance, owner):
        # Instantiate a copy of this group, assign a specific Metadata instance
        # ans return it.
        import copy

        instance.ensure_loaded(self._key)

        return self.get_group_instance(instance)


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
        '''Iterated over dictionary values, not Term instances'''
        return self._term_values.__iter__()

    def set(self, d):
        assert isinstance(d, dict)
        for k,v in d.items():
            try:
                self.__setitem__(k,v)
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

        proto = type(self).__dict__['_proto'] # Avoids __get___?

        proto.init_descriptor('_proto', self._top)

    def get_term_instance(self, key):

        if '_proto' not in dir(self):
            raise AttributeError("TypeDictGroup must have a _proto Term")

        o = copy.deepcopy(self._proto)
        o.init_instance(self)
        o._key = key
        return o

    # This getter wil return a DictTerm, usually, so it is the DictTerm
    # that actually sets the value.
    def __getitem__(self, key):

        if not key in self._term_values:
            self._term_values[key] =  { name:None for name, _ in self._proto._members.items() }

        o = self.get_term_instance(key)

        return o.get()

    def __setitem__(self, key, value):

        o = self.get_term_instance(key)
        o.set(value)



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


    def init_descriptor(self, key, top):
        super(ListGroup, self).init_descriptor(key, top)

        if '_proto' not in dir(self):
            raise AttributeError("TypeDictGroup must have a _proto Term")

        proto = type(self).__dict__['_proto']  # Avoids __get___?

        proto.init_descriptor('_proto', self._top)


    @property
    def _term_values(self):
        # This only works after being instantiated in __get__, which sets
        # _parent

        if not self._key in self._parent._term_values:
            self._parent._term_values[self._key] = []


        return self._parent._term_values[self._key]

    def get_term_instance(self, key):

        if '_proto' not in dir(self):
            raise AttributeError("TypeDictGroup must have a _proto Term")

        o = copy.deepcopy(self._proto)
        o.init_instance(self)
        o._key = key
        return o

    def __set__(self, instance, v):
        assert isinstance(v, list)

        instance._term_values[self._key] = []

        o = self.get_group_instance(instance)

        o.set(v)


    def insert(self, index, value):
        self.__setitem__(index, value)

    def ensure_index(self,index):

        if index >= len(self._term_values):
            o = self.get_term_instance(index)

            to_add = (len(self._term_values) - index + 1)

            self._parent._term_values[self._key] += ([o.null_entry()] * to_add)

        assert len(self._parent._term_values[self._key]) > index

    def __getitem__(self, index):

        self.ensure_index(index)

        o = self.get_term_instance(index)
        return o.get()

    def __setitem__(self, index, value):

        self.ensure_index(index)
        o = self.get_term_instance(index)



        o.set(value)


    def __delitem__(self, index):
        return self._term_values.__delitem__(index)

    def __len__(self):
        return self._term_values.__len__()

    def __iter__(self):
        return self._term_values.__iter__()

    def set(self,d):

        for index, value in enumerate(d):
            self.__setitem__(index, value)

    def set_row(self, key, value):
        index, _ = key

        self.__setitem__(index,value)

    def reformat(self,v):
        raise NotImplementedError()


class Term(object):
    """A single term in a group"""

    _key = None
    _parent = None # set after being cloned in some subclass __get__
    _top = None
    _synonym = None
    _store_none = None
    _default = None
    _members = None

    def __init__(self, synonym=None, store_none=True, default = None):

        self._members = {name: attr for name, attr in type(self).__dict__.items() if isinstance(attr, Term)}

        if synonym and not isinstance(synonym, (list, tuple)):
            synonym = (synonym,)

        self._synonym = tuple(synonym) if synonym else tuple()
        self._store_none = store_none
        self._default = default


    def init_descriptor(self, key, top):
        assert(key is not None)
        self._key = key

    def init_instance(self, parent):
        self._parent = parent
        self._top = parent._top

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
            return instance._term_values.get(self._key).get()

    def get(self):
        '''Return the value type for this Term'''
        raise NotImplementedError("Not implemented by {}".format(type(self)))

    def set(self,v):
        raise NotImplementedError("Not implemented in {} ".format(type(self)))

class ScalarTerm(Term):
    """A Term that can only be a string"""

    def set(self, v):
        self._parent._term_values[self._key] = v

    def get(self):
        return self._parent._term_values[self._key]

    def null_entry(self):
        return None

    def reformat(self, v):
        return v

class DictTerm(Term, collections.MutableMapping):
    """A term that contains a dict of sub-parts

    A value may be specified as a scalar, in which case it will be converted to a dict
    """

    default = None
    _store_none_map = None

    def init_descriptor(self, key, top):
        super(DictTerm, self).init_descriptor(key, top)

        for name, m in self._members.items():
            m.init_descriptor(name, top)

        assert(self._key is not None)


        self._store_none_map =  {name: m._store_none for name, m in self._members.items()}

    def get_term_instance(self, key):
        m = self._members[key]
        o = copy.deepcopy(m)
        o.init_instance(self)
        return o


    def _new_instance(self, parent):
        o = copy.deepcopy(self)
        o.init_instance(parent)
        o._key = self._key

        return o

    @property
    def _term_values(self):
        # This only works after being instantiated in __get__, which sets
        # _parent

        self._parent.ensure_index(self._key)

        tv =  self._parent._term_values[self._key]

        assert tv is not None

        return tv



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

        o = self.get_term_instance(key)

        if key == 'name':
            pass

        return o.get()

    def __setitem__(self, key, value):

        self.set_row(key,value)


    def __delitem__(self, key):
        return self._term_values.__delitem__(key)

    def __len__(self):
        return self._term_values.__len__()

    def __iter__(self):
        if not self._term_values:
            return iter([])
        else:
            return self._term_values.__iter__()


    def __set__(self, instance, d):

        o = self._new_instance(instance)
        o.set(d)

    def __get__(self, instance, owner):
        """ """
        # Instantiate a copy of this group, assign a specific Metadata instance
        # and return it.
        import copy

        return self._new_instance(instance)


    def set(self, d):

        for k, v in d.items():
            self.set_row(k,v)


    def set_row(self, k, v):

        if k not in self._members.keys():
            self._top.add_error(self._parent._key, self._key, k, v)
            return

        self._term_values[k] = v

        #if k in self._term_values and self._term_values[k] is None and self._store_none_map[k] is False:
        #    del self._term_values[k]

    def get(self):
        return self

    def null_entry(self):
        d = AttrDict()

        for k, v in self._members.items():
            if self._store_none_map[k] is True:
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

    def get(self):
        return self


    def null_entry(self):
        return []

