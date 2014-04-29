"""Metadata objects for a bundle

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


from ..util import AttrDict
from UserList import UserList
from UserDict import IterableUserDict

class Metadata(object):
    """A top level group of groups"""

    _members = None
    _non_term_members = None
    _errors = {}

    def __init__(self, d=None):
        self._non_term_members = {}
        self.register_members()

        if d is not None:
            for k, v in d.items():
                if k in self._members:
                    self._members[k].set(v)
                else:
                    # Top level groups that don't match a member group are preserved,
                    # not errors like unknown terms in a group.
                    self._non_term_members[k] = v

    def register_members(self):

        self._members = {name: attr for name, attr in type(self).__dict__.items() if isinstance(attr, Group)}

        for name,m in self._members.items():
            m.init(name, self, self)

    def visit(self, f):
        for _, m in self._members.items():
            m.visit(f)

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


    @property
    def path(self):
        return tuple()

class Group(object):
    """A  group of terms"""

    _key = None
    _parent = None
    _top = None
    _members = None
    _term_values = None

    def __init__(self):
        self._key = None
        try:
            self._term_values = {}
        except AttributeError: # its a property in one subclass
            pass

    def init(self,key, parent, top):
        self._key = key
        self._parent = parent
        self._top = top
        self.register_members()

    def _terms(self):
        return {name: attr for name, attr in type(self).__dict__.items() if isinstance(attr, Term)}

    def register_members(self):
        raise NotImplementedError('register_members not implemented in {}'.format(type(self)))

    def set(self):
        raise NotImplementedError('set not implemented in {}'.format(type(self)))

    def member(self, name):
        try:
            return type(self).__dict__[name]
        except KeyError:
            return None

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

    @property
    def path(self):

        return self._parent.path + (self._key,)

    def visit(self, f):
        for _, m in self._members.items():
            m.visit(f)


class DictGroup(Group):
    """A group that holds key/value pairs"""

    def __init__(self):
        super(DictGroup, self).__init__()

    def register_members(self):

        # since the Terms are descriptors, we have to access them from the class, not the instance
        # or we'll be checking the type returned from __get__
        self._members = self._terms()

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
                self._top.missing_term((self._key,k),v)

class VarDictGroup(Group, IterableUserDict):
    """A group that holds key/value pairs, but all of the same type.

    Probably only works with DictTerms
    """

    _proto = None

    def __init__(self):
        super(VarDictGroup, self).__init__()
        self.data = {}


    def register_members(self):
        # In UserList, the terms are prototypes for the term type used in the elements.
        self._members = {}

        try:
            self._proto = self._terms().values()[0]
        except KeyError:
            raise

    def __setitem__(self, key, value):
        if not key in self.data:
            t = type(self._proto)()
            t.init(key, self, self._top)
            self.data[key] = t

        self.data[key].__set__(self, value)

    def __getattr__(self, key):
        if not key in self.data:
            t = type(self._proto)()
            t.init(key, self, self._top)
            self.data[key] = t


        return self.data[key]

    @property
    def dict(self):
        return self._term_values


class ListGroup(Group, UserList):
    """A group that holds a list of DictTerms"""

    data = None

    def __init__(self):
        super(ListGroup, self).__init__()
        self.data = []

    def register_members(self):

        # In UserList, the terms are prototypes for the term type used in the elements.
        terms = self._terms()

        try:
            self._proto = terms.values()[0]
        except KeyError:
            raise


    @property
    def key(self):
        return str(self.__class__).lower()

    def set(self, value):
        """Set them members of the group from a list"""

        for i,v in enumerate(value):
            t = type(self._proto)()
            t.init(i, self, self._top)

            if len(self.data) <= i:
                self.data.extend([]*(1+i-len(self.data)))

            t.__set__(self, v)

            self.data.append(t)


    def visit(self, f):
        pass

class Term(object):
    """A single term in a group"""

    _key = None
    _parent = None
    _top = None
    _synonym = []
    _show_none = None
    _default = None


    def __init__(self, synonym=None, show_none=True, default = None):
        if synonym and not isinstance(synonym, (list, tuple)):
            synonym = (synonym,)

        self._synonym = tuple(synonym) if synonym else tuple()
        self._show_none = show_none
        self._default = default


    def init(self, key, parent, top):

        self._key = key
        self._parent = parent
        self._top = top

    def __set__(self, instance, v):
        instance._term_values[self._key] = v

    def __get__(self, instance, owner):
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



class DictTerm(Term):
    """A term that contains a dict of sub-parts

    A value may be specified as a scalar, in which case it will be converted to a dict
    """

    default = None

    def init(self, key, parent, top):
        super(DictTerm, self).init(key, parent, top)
        self.register_members()

    def register_members(self):

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

    @property
    def _term_values(self):
        """Redirects the _term_values dictionary to the pare, so the member terms access to it will
        save data to the Group."""

        if not self._key in self._parent._term_values:
            self.instance._term_values[self._key] = {}

        return self._parent._term_values[self._key]


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
                self._top.missing_term((self._parent._key, self._key, k), v)


    def __set__(self, instance, d):
        for k, v in d.items():
            if k in instance._term_values[self._key]:
                instance._term_values[self._key][k] = d[k]
            else:
                self._top.missing_term((self._parent._key, self._key, k), v)



    def __get__(self, instance, owner):
        """This version of __get__ does what would have happened if there were no __get__;
        it returns the member of the parent directly. """
        return self._parent._members[self._key]


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