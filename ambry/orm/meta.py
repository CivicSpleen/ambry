"""Metadata objects for a bundle

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from proptree import *


class About(DictGroup):
    title = ScalarTerm()
    subject = ScalarTerm()
    summary = ScalarTerm()
    space = ScalarTerm()
    time = ScalarTerm()
    grain = ScalarTerm()
    access = ScalarTerm(store_none=False)
    # Internal, Private, Controlled, Restrcted, Registered, Licensed, Public
    license = ScalarTerm(store_none=False)
    rights = ScalarTerm(store_none=False)
    tags = ListTerm(store_none=False)
    groups = ListTerm(store_none=False)
    source = ScalarTerm(store_none=False) # A text statement about the source of the data
    footnote = ScalarTerm(store_none=False) # A footnote entry
    processed = ScalarTerm(store_none=False) # A statement about how the data were processed.

class Documentation(DictGroup):
    readme = ScalarTerm()
    main = ScalarTerm()
    source = ScalarTerm(store_none=False)  # Expanded from about.source
    footnote = ScalarTerm(store_none=False) # Expanded from about.footnote
    processed = ScalarTerm(store_none=False)  # expanded from about.processed
    title = ScalarTerm(store_none=False)  # expanded from about.title
    summary = ScalarTerm(store_none=False)  # expanded from about.summary

class Coverage(DictGroup):
    geo = ListTerm()
    grain = ListTerm()
    time = ListTerm()


class Identity(DictGroup):
    """ """
    dataset = ScalarTerm()
    id = ScalarTerm()
    revision = ScalarTerm()
    source = ScalarTerm()
    subset = ScalarTerm()
    variation = ScalarTerm()
    btime = ScalarTerm()
    bspace = ScalarTerm()
    type = ScalarTerm()
    version = ScalarTerm()


class Names(DictGroup):
    """Names that are generated from the identity"""
    fqname = ScalarTerm()
    name = ScalarTerm()
    vid = ScalarTerm()
    vname = ScalarTerm()


class Dependencies(VarDictGroup):
    """Bundle dependencies"""


class Build(VarDictGroup):
    """Build parameters"""


class ExtDocTerm(DictTerm):
    url = ScalarTerm()
    title = ScalarTerm()
    description = ScalarTerm()
    source = ScalarTerm()

class ExtDoc(TypedDictGroup):
    """External Documentation"""
    _proto = ExtDocTerm()  # Reusing


class ContactTerm(DictTerm):
    role = ScalarTerm(store_none=False)
    name = ScalarTerm(store_none=False)
    org = ScalarTerm(store_none=False)
    email = ScalarTerm(store_none=False)
    url = ScalarTerm(store_none=False)

    def __nonzero__(self):
        return bool(self.name or self.email or self.url)

    def __bool__(self):
        return self.__nonzero__()

class Contact(DictGroup):
    """ """
    _proto = ContactTerm()


class VersonTerm(TypedDictGroup):
    """Version Description"""
    version = ScalarTerm()
    date = ScalarTerm()
    description = ScalarTerm(store_none=False)

class Versions(TypedDictGroup):
    """Names that are generated from the identity"""
    _proto = VersonTerm()

class Top(StructuredPropertyTree):
    _non_term_file = 'meta/build.yaml'

    about = About()
    identity = Identity()
    dependencies = Dependencies()
    external_documentation = ExtDoc()
    build = Build()
    contacts = Contact()
    versions = Versions()
    names = Names()
    documentation = Documentation()
    coverage = Coverage()
