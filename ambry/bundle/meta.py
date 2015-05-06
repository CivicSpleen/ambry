"""Metadata objects for a bundle

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..util.meta import *


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


class Documentation(DictGroup):
    readme = ScalarTerm()
    main = ScalarTerm()


class Coverage(DictGroup):
    geo = ListTerm()
    grain = ListTerm()
    time = ListTerm()


class ContactTerm(DictTerm):
    name = ScalarTerm(store_none=False)
    email = ScalarTerm(store_none=False)
    url = ScalarTerm(store_none=False)

    def __nonzero__(self):
        return bool(self.name or self.email or self.url)

    def __bool__(self):
        return self.__nonzero__()


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


class RowSpecDictTerm(DictTerm):
    data_start_line = ScalarTerm()
    data_end_line = ScalarTerm()
    header_lines = ListTerm()
    header_comment_lines = ListTerm()


class SourceTerm(DictTerm):
    """A term that describes a source file for constructing a partition"""

    url = ScalarTerm()
    title = ScalarTerm(store_none=False)  # Title for use in table.
    description = ScalarTerm(store_none=False)
    dd_url = ScalarTerm(store_none=False)  # Data Dictitionary URL
    file = ScalarTerm(store_none=False)  # A name or regex to extract from a multi-file ZIP
    filetype = ScalarTerm(store_none=False)
    # For the LoaderBundle, use this file type ( file extensino ) rather than from the url

    segment = ScalarTerm(store_none=False)  # Specify a sub-component of the file, like a sheet in an excel workbook.
    comment = ScalarTerm(store_none=False)  # Just a comment
    is_loadable = ScalarTerm(store_none=False)  # If false, ignore in auto-loading
    time = ScalarTerm(store_none=False)  # Specify a time component, usually a year.
    space = ScalarTerm(store_none=False)  # Specify a space component
    grain = ScalarTerm(store_none=False)  # Specify a grain component
    table = ScalarTerm(store_none=False)  # For auto imports, name of table to load into.
    conversion = ScalarTerm(store_none=False)  # An alternate URL or regular expression for a file in the source store
    foreign_key = ScalarTerm(store_none=False)  # ID of the foreign key for the table.
    row_spec = RowSpecDictTerm(store_none=False)
    # Spec for non data rows. 'start' for first line of data, 'header' for sclar/list of header lines

    row_data = DictTerm(store_none=False)  # A dict of values that are added to every row of the table.

    def __nonzero__(self):
        return bool(self.url or self.file or self.description or self.dd_url)

    def __bool__(self):
        return self.__nonzero__()


class Sources(TypedDictGroup):
    """References to source URLS"""
    _proto = SourceTerm()


class Dependencies(VarDictGroup):
    """Bundle dependencies"""


class Build(VarDictGroup):
    """Build parameters"""


class Extract(VarDictGroup):
    """Extract parameters"""


class Views(VarDictGroup):
    """Extract parameters"""


class Process(VarDictGroup):
    """Process data. Build times, etc."""


class ExtDocTerm(DictTerm):
    url = ScalarTerm()
    title = ScalarTerm()
    description = ScalarTerm()
    source = ScalarTerm()


class ExtDoc(TypedDictGroup):
    """External Documentation"""
    _proto = ExtDocTerm()  # Reusing


class VersonTerm(DictTerm):
    """Version Description"""
    version = ScalarTerm()
    date = ScalarTerm()
    description = ScalarTerm(store_none=False)


class Versions(TypedDictGroup):
    """Names that are generated from the identity"""
    _proto = VersonTerm()


class Top(Metadata):

    _non_term_file = 'meta/build.yaml'

    about = About(file='bundle.yaml')
    identity = Identity(file='bundle.yaml')
    names = Names(file='bundle.yaml')
    contact_source = Contact(file='bundle.yaml')
    contact_bundle = Contact(file='bundle.yaml')
    versions = Versions(file='bundle.yaml')
    process = Process(file='bundle.yaml')
    external_documentation = ExtDoc(file='bundle.yaml')

    sources = Sources(file='meta/build.yaml')

    dependencies = Dependencies(file='meta/build.yaml')
    build = Build(file='meta/build.yaml')
    views = Views(file='meta/build.yaml')

    documentation = Documentation(file='meta/doc.yaml')

    coverage = Coverage(file='meta/coverage.yaml')
