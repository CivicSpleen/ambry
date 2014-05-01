"""Metadata objects for a bundle

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


import collections
from ..util import AttrDict, MapView

class Metadata(object):
    """A top level group of groups"""

    _members = None
    _non_term_values = None
    _term_values = None
    _errors = None

    def __init__(self, d=None):
        self._term_values = AttrDict()
        self._errors = {}

        self.register_members()

        self.set(d)

    def set(self, d):
        if d is not None:
            for k, v in d.items():
                if k in self._members:
                    getattr(self, k).set(v)
                else:
                    # Top level groups that don't match a member group are preserved,
                    # not errors like unknown terms in a group.
                    self._non_term_members[k] = v

    def register_members(self):
        """Collect the names of the class member and convert them to object members.

        Unlike Terms, the Group class members are converted into object members, so the configuration data
        """
        import copy

        self._members = {name: attr for name, attr in type(self).__dict__.items() if isinstance(attr, Group)}

        for name, m in self._members.items():
            m.init_descriptor(name)

    def visit(self, f):
        for _, m in self._members.items():
            m.visit(f)

    @property
    def dict(self):
        d = {}
        for name, m in self._members.items():
            d[name] = m.value

        for k, v in self._non_term_members.items():
            d[k] = v

        return d

    @property
    def rows(self):
        """Return the configuration information flattened into database records """

        rows = []
        for name, m in self._members.items():
            rows.extend(m.rows)

        return rows

    def load_rows(self, rows):
        import json

        for group, key, value in rows:
            key_parts = key.split('.')

            try:
                v = json.loads(value)
            except ValueError:
                pass
            except TypeError:
                pass

            m = self._members[group]

            m.set_row(key, value)


    @property
    def errors(self):
        return self._errors


    def dump(self, stream = None, map_view = None):
        return self._term_values.dump(stream, map_view = map_view)

    def missing_term(self, path, value):
        """Called when a term value isn't assigned, try to find a synonym to assign it to

        Currently only assign top-level; terms; it can't do DictTerms
        """
        from functools import partial
        key = '.'.join([ str(p) for p in path])

        failed = [True]

        def match_syn(failed, term):
            if term._synonym and key in term._synonym :
                term.__set__(term._parent, value)
                failed[0] = False

        self.visit(partial(match_syn, failed))

        if failed[0]:
            self._errors[path] =  value


    def groups_by_file(self):
        d = {}

        for name, m in self._members.items():
            file_ = m._file

            if not file_:
                file_ = 'bundle.yaml'

            if file_ not in d:
                d[file_] = []

            d[file_].append(m)


        return d

    def dict_by_file(self):
        '''Return as a dictionary, organized by file'''
        from collections import defaultdict

        d = {}

        for name, m in self._members.items():
            file_ = m._file

            if not file_:
                file_ = 'bundle.yaml'

            if file_ not in d:
                d[file_] = {}

            d[file_][name] = m.value

        file_ = 'bundle.yaml'

        for k, v in self._non_term_members.items():

            if file_ not in d:
                d[file_] = {}

            d[file_][k] = v

        return d


    def load_from_dir(self, path):
        '''Load groups from sepcified files. '''
        import os
        import yaml

        for file, groups in self.groups_by_file():
            try:
                with open(os.path.join(path, file)) as f:
                    d = yaml.load(f)

                    self.set(d)

            except IOError:
                raise

    def write_to_dir(self, path):
        for file_, d in self.dict_by_file().items():

            print '======== {} ========='.format(file_)

            print d


class Group(object):
    """A  group of terms"""

    _key = None
    _members = None

    _file = None
    _to_rows = None

    # Set in init_instance
    _parent = None


    def __init__(self, file=None, to_rows=True):
        self._file = file
        self._to_rows = to_rows

        self._members = {name: attr for name, attr in type(self).__dict__.items() if isinstance(attr, Term) and not name.startswith('_')}


    def init_descriptor(self,key):
        self._key = key

        for name, m in self._members.items():
            m.init_descriptor(name)

    def init_instance(self, parent):
        self._parent = parent

    def __set__(self, instance, v):
        raise NotImplementedError()

    def __get__(self, instance, owner):
        # Instantiate a copy of this group, assign a specific Metadata instance
        # ans return it.
        import copy

        o = copy.deepcopy(self)
        o.init_instance(instance)
        return o

    @property
    def _term_values(self):
        raise NotImplementedError()


    def set_row(self,k, v):
            print k,v

    @property
    def rows(self):
        if self._members:
            for name, m in self._members.items():
                v = m.get(self, type(self))
                if v is None:
                    continue
                if isinstance(v, dict):
                    for sk,sv in v.items():
                        yield self._key, name+'.'+sk, sv
                elif isinstance(v,list):
                    for i,sv in enumerate(v):
                        yield self._key, name + '.' + str(i), sv
                else:
                    yield self._key, name, v

    def __setattr__(self, attr, value):

        if attr not in dir(self):
            raise AttributeError("No such term: {} ".format(attr))

        return object.__setattr__(self, attr, value)



    @property
    def path(self):

        return self._parent.path + (self._key,)

    def visit(self, f):
        for _, m in self._members.items():
            m.visit(f)


class DictGroup(Group, collections.MutableMapping):
    """A group that holds key/value pairs"""

    def __init__(self, file=None, to_rows=True):
        super(DictGroup, self).__init__(file=file, to_rows=to_rows)

    @property
    def _term_values(self):
        # This only works after being instantiated in __get__, which sets
        # _parent
        if not self._key in self._parent._term_values:
            self._parent._term_values[self._key] = {}

        return self._parent._term_values[self._key]



    def __getitem__(self, key):
        return self._term_values.__getitem__(key)

    def __setitem__(self, key, value):
        raise NotImplementedError()
        return self._term_values.__setitem__(key, value)

    def __delitem__(self, key):
        return self._term_values.__delitem__(key)

    def __len__(self):
        return self._term_values.__len__()

    def __iter__(self):
        return self._term_values.__iter__()


class TypedDictGroup(DictGroup):
    """A DictGroup where the term structure is constrained, but they keys are not.

    There must be one term, named 'proto', to set the type of the terms.

    Only works with DictTerms
    """

    def __getattr__(self, name):
        import copy

        try:
            return object.__getattr__(self, name)
        except AttributeError:
            if name.startswith('__'):
                raise

            if '_proto' not in dir(self):
                raise AttributeError("TypeDictGroup must have a _proto Term")

            if not name in self._term_values:
                self._term_values[name] =  { name:None for name, _ in self._proto._members.items() }

            o = copy.deepcopy(self._proto)
            o.init_descriptor(name)
            o.init_instance(self)

            return o


class VarDictGroup(DictGroup):
    """A Dict group that doesnt' use terms to enforce a structure.
    """
    from ..util import AttrDict


    def __getattr__(self, name):
        import copy

        try:
            return object.__getattr__(self, name)
        except AttributeError:
            if name.startswith('__'):
                raise

            if not name in self._term_values:

                self._term_values[name] =  AttrDict()


            return self.__getitem__(name)


class ListGroup(Group, collections.MutableSequence):
    """A group that holds a list of DictTerms"""



    def __init__(self, file=None, to_rows=True):
        super(ListGroup, self).__init__(file=file, to_rows = to_rows)


    def register_members(self):

        try:
            self._proto = self._members.values()[0]
        except KeyError:
            raise


    @property
    def _term_values(self):
        # This only works after being instantiated in __get__, which sets
        # _parent
        if not self._key in self._parent._term_values:
            self._parent._term_values[self._key] = []

        return self._parent._term_values[self._key]


    def __repr__(self):
        raise NotImplementedError()

    def insert(self, index, value):
        self._term_values.insert(index,value)


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


    @property
    def value(self):
        """Return the most suitable representation for this group, either a dict or a list"""

        return [ { k:v for k,v in t.get(self, type(self)).items() if v is not None} for t in self.data ]

    @property
    def rows(self):
        if not self._to_rows:
            return

        for i,v in enumerate(self.value):
            if isinstance(v,dict):
                for sk,sv in v.items():
                    yield self._key, str(i)+'.'+sk, sv
            else:
                yield self._key, str(i), v


class Term(object):
    """A single term in a group"""

    _key = None
    _parent = None # set after being cloned in some subclass __get__
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



    def init_descriptor(self, key):
        self._key = key

    def init_instance(self, parent):
        self._parent = parent



    def __set__(self, instance, v):
        instance._term_values[self._key] = v

    def __get__(self, instance, owner):
        print "GET", self._key, type(self), type(instance)
        return instance._term_values.get(self._key)

    def get(self, instance, owner):
        '''A get that turns only dicts, lists and scalars, for use in creating dicts'''
        return instance._term_values.get(self._key)



    @property
    def path(self):
        return self._parent.path+(self._key,)

    def visit(self, f):
        f(self)


class ScalarTerm(Term):
    """A Term that can only be a string"""


class DictTerm(Term, collections.MutableMapping):
    """A term that contains a dict of sub-parts

    A value may be specified as a scalar, in which case it will be converted to a dict
    """

    default = None


    def init_descriptor(self, key):
        super(DictTerm, self).init_descriptor(key)

        for name, m in self._members.items():
            m.init_descriptor(name)

    @property
    def _term_values(self):
        # This only works after being instantiated in __get__, which sets
        # _parent
        if not self._key in self._parent._term_values:
            self._parent._term_values[self._key] =  { name:None for name, _ in self._members.items() }

        return self._parent._term_values[self._key]

    def __setattr__(self, attr, value):

        if attr not in dir(self):
            raise AttributeError("No such term: {} ".format(attr))



        return object.__setattr__(self, attr, value)


    def __getitem__(self, key):
        return self._term_values.__getitem__(key)

    def __setitem__(self, key, value):
        print '!!!', key, value
        return self._term_values.__setitem__(key, value)

    def __delitem__(self, key):
        return self._term_values.__delitem__(key)

    def __len__(self):
        return self._term_values.__len__()

    def __iter__(self):
        return self._term_values.__iter__()

    def __set__(self, instance, d):
        for k, v in d.items():
            if k in instance._term_values[self._key]:
                instance._term_values[self._key][k] = d[k]
            else:
                self._parent.missing_term((self._parent._key, self._key, k), v)


    def __get__(self, instance, owner):
        """ """
        # Instantiate a copy of this group, assign a specific Metadata instance
        # and return it.
        import copy

        o = copy.deepcopy(self)
        o.init_instance(instance)

        return o


    def get(self, instance, owner):
        '''A get that turns only dicts, lists and sclars, for us in creating dicts'''
        return instance._term_values.get(self._key,{})


    @property
    def dict(self):
        d = {}

        for k, v in self._members.items():

            dv  = v.get(self, type(self))

            if dv is not None  or ( dv is None and v._show_none is True):
                d[k] = dv


        return d

    @property
    def value(self):
        return self.dict


    def visit(self, f):
        f(self)
        for _, m in self._members.items():
            m.visit(f)


class ListTerm(Term):
    """A Term that is always a list.

    The value may be specified as a scalar, in which case it will be converted to a list"""

    def __set__(self, instance, v):
        if not isinstance(v, (list, tuple)):
            v = [v]

        instance._term_values[self._key] = list(v)