"""Metadata objects for a bundle

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


from ..util import AttrDict

class Metadata(object):
    """A top level group of groups"""

    _members = None
    _non_term_members = None
    _errors = {}

    def __init__(self, d=None):
        self._non_term_members = {}
        self.visit_members()

        if d is not None:
            for k, v in d.items():
                if k in self._members:
                    self._members[k].set(v)
                else:
                    self._non_term_members[k] = v

    def visit_members(self):

        self._members = {name: attr for name, attr in type(self).__dict__.items() if isinstance(attr, Group)}

        for name,m in self._members.items():
            m.init(name, self, self)

    @property
    def dict(self):
        d = {}
        for name, m in self._members.items():
            d[name] = m.dict

        for k, v in self._non_term_members.items():
            d[k] = v

        return d

    @property
    def errors(self):
        return self._errors



class Group(object):
    """A  group of terms"""

    _key = None
    _parent = None
    _top = None
    _members = None
    _term_values = None

    def __init__(self):
        self._key = None
        self._term_values = {}

    def init(self,key, parent, top):
        self._key = key
        self._parent = parent
        self._top = top
        self.visit_members()

    def visit_members(self):
        raise NotImplementedError('visit_members not implemented in {}'.format(type(self)))

    def set(self):
        raise NotImplementedError('set not implemented in {}'.format(type(self)))

    @property
    def dict(self):
        if not self._members:
            return {}

        d = {}
        for name, m in self._members.items():

            d[name] = m.get(self, type(self))

        return d


    @property
    def x_dict(self):
        return self._term_values

class DictGroup(Group):
    """A group that holds key/value pairs"""

    def __init__(self):
        super(DictGroup, self).__init__()

    def visit_members(self):

        # since the Terms are descriptors, we have to access them from the class, not the instance
        # or we'll be checking the type returned from __get__
        self._members = {name: attr for name, attr in type(self).__dict__.items() if isinstance(attr, Term)}

        for name, m in self._members.items():
            m.init(name, self, self._top)

    @property
    def key(self):
        return str(self.__class__).lower()

    def set(self, value):
        """Copy a dict into the members of the group"""
        for k,v in value.items():
            if k in self._members:
                self._members[k].__set__(self,v)
            else:
                self._top._errors[(self._key,k)] = v


class Term(object):
    """A single term in a group"""

    _key = None
    _parent = None
    _top = None

    def __init__(self):
        pass

    def init(self, key, parent, top):
        assert(top is not None)
        self._key = key
        self._parent = parent
        self._top = top

    def __set__(self, instance, v):
        instance._term_values[self._key] = v

    def __get__(self, instance, owner):
        return instance._term_values.get(self._key)

    def get(self, instance, owner):
        '''A get that turns only dicts, lists and scalars, for us in creating dicts'''
        return instance._term_values.get(self._key)

class ScalarTerm(Term):
    """A Term that can only be a string"""



class DictTerm(Term):
    """A term that contains a dict of sub-parts

    A value may be specified as a scalar, in which case it will be converted to a dict
    """

    default = None

    def __init__(self, default=None):
        self.default = default

    def init(self, key, parent, top):
        super(DictTerm, self).init(key, parent, top)
        self.visit_members()

    def visit_members(self):

        # since the Terms are descriptors, we have to access them from the class, not the instance
        # or we'll be checking the type returned from __get__
        self._members = {name: attr for name, attr in type(self).__dict__.items() if isinstance(attr, Term)}

        for name, m in self._members.items():
            m.init(name, self, self._top)

        # Copy the member term names into the parent _term_values
        # The sub-terms aren't actually used -- they are just declared for consistency.

        if not self._key in self._parent._term_values:
            h = {}
            for name, m in self._members.items():
                h[name] = None

            self._parent._term_values[self._key] = h

    def set(self, value):
        """Set the sub-elements of the term from a dict

        If the value is a scalar, set the default sub-element"""

        if not isinstance(value, dict):

            if not self.default:
                raise ValueError("Can't set a scalar if there is no default defined")

            self._members[self.default].__set__(self, value)

        for k,v in value.items():
            if k in self._members:
                raise NotImplementedError()
                self._members[k].__set__(self,v)
            else:
                self._top._errors[(self._parent._key, self._key, k)] = v

    def __set__(self, instance, d):

        for k, v in d.items():
            if k in instance._term_values[self._key]:
                instance._term_values[self._key][k] = d[k]
            else:
                self._top._errors[(self._parent._key, self._key, k)] = v


    def __get__(self, instance, owner):

        class inner_dict(object):

            instance = None
            key = None

            def __init__(self,instance, key):
                object.__setattr__(self, 'instance',instance)
                object.__setattr__(self, 'key', key)

            def __getattr__(self, k):
                return self.instance._term_values.get(self.key,{}).get(k, None)

            def __setattr__(self, k, v):
                if not self._key in self.instance._term_values:
                    self.instance._term_values[self.key]  = {}

                self.instance._term_values[self.key][k] = v


        return inner_dict(instance, self._key)

    def get(self, instance, owner):
        '''A get that turns only dicts, lists and sclars, for us in creating dicts'''
        return instance._term_values.get(self._key,{})


    @property
    def dict(self):
        d = {}
        for k, v in self._members.items():
            d[k] = v.__get__(self, type(self))

        return d


class ListTerm(Term):
    """A Term that is always a list.

    The value may be specified as a scalar, in which case it will be converted to a list"""

    def __set__(self, instance, v):
        if not isinstance(v, (list, tuple)):
            v = [v]

        instance._term_values[self._key] = list(v)