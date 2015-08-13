"""Metadata objects for a bundle

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from .proptree import DictGroup, VarDictGroup, TypedDictGroup,\
    ScalarTerm, ListTerm, DictTerm, StructuredPropertyTree


class About(DictGroup):
    title = ScalarTerm()
    subject = ScalarTerm()
    summary = ScalarTerm()
    space = ScalarTerm()
    time = ScalarTerm()
    grain = ListTerm()
    remote = ScalarTerm(store_none=False)  # If empty, take the remote name from access
    access = ScalarTerm(
        store_none=False,
        constraint=['internal', 'private', 'controlled', 'restricted', 'registered', 'licensed', 'public', 'test'])
    license = ScalarTerm(store_none=False)
    rights = ScalarTerm(store_none=False)
    tags = ListTerm(store_none=False)
    groups = ListTerm(store_none=False)
    source = ScalarTerm(store_none=False)  # A text statement about the source of the data
    footnote = ScalarTerm(store_none=False)  # A footnote entry
    processed = ScalarTerm(store_none=False)  # A statement about how the data were processed.


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


class Dependencies(DictGroup):
    """Bundle dependencies"""
    counties = ScalarTerm()
    facility_index = ScalarTerm()
    facility_info = ScalarTerm()


class Requirements(VarDictGroup):
    """Python package requirements"""


class Build(VarDictGroup):
    """Build parameters"""


class Pipeline(VarDictGroup):
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

    def __bool__(self):
        return bool(self.name or self.email or self.url)

# py2 compatibility, defining it such way fools 2to3 tool.
ContactTerm.__nonzero__ = ContactTerm.__bool__


class Contacts(DictGroup):
    creator = ContactTerm()
    maintainer = ContactTerm()
    source = ContactTerm()
    analyst = ContactTerm()


class VersonTerm(DictTerm):
    """Version Description"""
    version = ScalarTerm()
    date = ScalarTerm()
    description = ScalarTerm(store_none=False)


class Versions(TypedDictGroup):
    """Names that are generated from the identity"""
    _proto = VersonTerm()


class Top(StructuredPropertyTree):
    _non_term_file = 'meta/build.yaml'
    _type = 'metadata'  # used in the Config.type while storing in the db.

    about = About()
    identity = Identity()
    dependencies = Dependencies()
    requirements = Requirements()
    external_documentation = ExtDoc()
    build = Build()
    pipelines = Pipeline()
    contacts = Contacts()
    names = Names()
    versions = Versions()
