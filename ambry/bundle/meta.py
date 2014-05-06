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

class ContactTerm(DictTerm):

    name = ScalarTerm()
    email = ScalarTerm()
    url = ScalarTerm()

class Contact(DictGroup):
    """ """
    creator = ContactTerm()
    maintainer = ContactTerm()

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


class SourceTerm(DictTerm):
    url = ScalarTerm()
    description = ScalarTerm(store_none=False)

class Sources(TypedDictGroup):
    """Names that are generated from the identity"""
    _proto = SourceTerm()


class Dependencies(VarDictGroup):
    """Names that are generated from the identity"""

class Build(VarDictGroup):
    """Build parameters"""

class Extract(VarDictGroup):
    """Extract parameters"""


class ExtDocTerm(DictTerm):
    url = ScalarTerm()
    title = ScalarTerm()
    description = ScalarTerm()
    source = ScalarTerm()

class ExtDoc(ListGroup):
    """External Documentation"""
    _proto = ExtDocTerm() # Reusing

class VersonTerm(DictTerm):
    """Version Description"""
    version = ScalarTerm()
    description = ScalarTerm(store_none=False)

class Versions(TypedDictGroup):
    """Names that are generated from the identity"""
    _proto = VersonTerm()


class Top(Metadata):

    _non_term_file = 'meta/build.yaml'

    _x_synonyms = {
        'about.maintainer': 'contact_bundle.maintainer.name',
        'about.maintainer_email': 'contact_bundle.maintainer.email',
        'about.author': 'contact_bundle.creator.name',
        'about.author_email': 'contact_bundle.creator.email',
        'about.homepage': 'contact_source.creator.url',
        'about.url': 'contact_source.creator.url',
        'about.website': 'contact_source.creator.url',
        'about.description': 'about.summary',
        'about.organization': 'contact_source.creator.name'


    }

    about = About(file='bundle.yaml')
    contact_source = Contact(file='bundle.yaml')
    contact_bundle = Contact(file='bundle.yaml')
    sources = Sources(file='meta/build.yaml')
    dependencies = Dependencies(file='meta/build.yaml')
    identity = Identity(file='bundle.yaml')
    names = Names(file='bundle.yaml')
    build = Build(file='meta/build.yaml')
    extract = Extract(file='meta/build.yaml')
    external_documentation = ExtDoc(file='bundle.yaml')
    versions = Versions(file='bundle.yaml')

