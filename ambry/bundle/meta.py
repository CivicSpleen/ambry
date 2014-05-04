"""Metadata objects for a bundle

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..util.meta import *

class About(DictGroup):

    title = ScalarTerm()
    license = ScalarTerm()
    rights = ScalarTerm()
    subject = ScalarTerm()
    summary = ScalarTerm()

    rights = ScalarTerm()
    tags = ListTerm()
    groups = ListTerm()
    url = ScalarTerm()

class ContactTerm(DictTerm):

    name = ScalarTerm()
    email = ScalarTerm()
    url = ScalarTerm()

class Contact(DictGroup):
    """ """

    creator = ContactTerm()
    maintainer = ContactTerm()
    source = ContactTerm()
    publisher = ContactTerm()

class Identity(DictGroup):
    """ """
    dataset = ScalarTerm()
    id = ScalarTerm()
    revision = ScalarTerm()
    source = ScalarTerm()
    subset = ScalarTerm()
    variation = ScalarTerm()
    version = ScalarTerm()

class Names(DictGroup):
    """Names that are generated from the identity"""

    fqname = ScalarTerm()
    name = ScalarTerm()
    vid = ScalarTerm()
    vname = ScalarTerm()

class PartitionTerm(DictTerm):

    name = ScalarTerm(store_none=False)
    time = ScalarTerm(store_none=False)
    space = ScalarTerm(store_none=False)
    grain = ScalarTerm(store_none=False)
    table = ScalarTerm(store_none=False)
    format = ScalarTerm(store_none=False)
    segment = ScalarTerm(store_none=False)

class Partitions(ListGroup):
    """Names that are generated from the identity"""

    _proto = PartitionTerm()

class SourceTerm(DictTerm):
    url = ScalarTerm()
    description = ScalarTerm(store_none=False)

class Sources(TypedDictGroup):
    """Names that are generated from the identity"""
    _proto = SourceTerm()


class Build(VarDictGroup):
    """Build parameters"""

class Extract(VarDictGroup):
    """Extract parameters"""


class ExtDocTerm(DictTerm):
    url = ScalarTerm()
    title = ScalarTerm()
    description = ScalarTerm()

class ExtDoc(ListGroup):
    """External Documentation"""
    _proto = ExtDocTerm() # Reusing

class VersonTerm(DictTerm):
    """Version Description"""
    semver = ScalarTerm()
    description = ScalarTerm(store_none=False)

class Versions(ListGroup):
    """Names that are generated from the identity"""
    _proto = VersonTerm()

class Top(Metadata):

    _non_term_file = 'meta/build.yaml'

    about = About(file='bundle.yaml')
    contact = Contact(file='bundle.yaml')
    sources = Sources(file='meta/build.yaml')
    identity = Identity(file='bundle.yaml')
    names = Names(file='bundle.yaml')
    partitions = Partitions(file='meta/partitions.yaml')
    build = Build(file='meta/build.yaml')
    extract = Extract(file='meta/build.yaml')
    external_documentation = ExtDoc(file='bundle.yaml')
    versions = Versions(file='bundle.yaml')

