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

    name = ScalarTerm()
    time = ScalarTerm()
    space = ScalarTerm()
    grain = ScalarTerm()
    table = ScalarTerm()
    format = ScalarTerm()
    segment = ScalarTerm()

class Partitions(ListGroup):
    """Names that are generated from the identity"""

    _proto = PartitionTerm()

class SourceTerm(DictTerm):

    url = ScalarTerm()
    description = ScalarTerm(show_none=False)


class Sources(TypedDictGroup):
    """Names that are generated from the identity"""

    _proto = SourceTerm()

class Build(VarDictGroup):
    """Build parameters"""

class Extract(VarDictGroup):
    """Extract parameters"""

class Top(Metadata):

    _non_term_file = 'meta/build.yaml'

    about = About(file='bundle.yaml')
    contact = Contact(file='bundle.yaml')
    sources = Sources(file='bundle.yaml')
    identity = Identity(file='bundle.yaml')
    names = Names(file='bundle.yaml')

    partitions = Partitions(file='meta/partitions.yaml', to_rows = False)

    build = Build(file='meta/build.yaml')
    extract = Extract(file='meta/build.yaml')

