"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

__docformat__ = 'restructuredtext en'

from collections import OrderedDict

from os.path import splitext

from six import iteritems
from six.moves.urllib.parse import urlparse

from sqlalchemy import Column as SAColumn
from sqlalchemy import Text, String, ForeignKey, INTEGER, UniqueConstraint
from sqlalchemy.orm import relationship

from .source_table import SourceTable
from .table import Table

from . import MutationList, JSONEncodedObj
from . import Base,  DictableMixin

class DataSourceBase(object):
    """Base class for data soruces, so we can have a persistent and transient versions"""

    def get_filetype(self):
        if self.filetype:
            return self.filetype

        if self.file:
            root, ext = splitext(self.file)
            return ext[1:]

        parsed = urlparse(self.url)

        root, ext = splitext(parsed.path)

        if ext == '.zip':
            parsed_path = parsed.path.replace('.zip', '')
            root, ext = splitext(parsed_path)

            return ext[1:]

        elif ext:
            return ext[1:]

        return None

    def get_urltype(self):
        from os.path import splitext

        if self.urltype:
            return self.urltype

        if self.url and self.url.startswith('gs://'):
            return 'gs'  # Google spreadsheet

        if self.url:
            root, ext = splitext(self.url)
            return ext[1:]

        return None

    @property
    def dict(self):
        """A dict that holds key/values for all of the properties in the
        object.

        :return:

        """
        SKIP_KEYS = ('_source_table', '_dest_table', 'd_vid', 't_vid', 'st_id', 'dataset', 'hash')
        return OrderedDict(
            (p.key, getattr(self, p.key)) for p in self.__mapper__.attrs if p.key not in SKIP_KEYS)

    @property
    def row(self):

        # Use an Ordered Dict to make it friendly to creating CSV files.
        SKIP_KEYS = ('sequence_id', 'vid', '_source_table',
                     '_dest_table', 'd_vid', 't_vid', 'st_vid', 'dataset')

        d = OrderedDict(
            [(p.key, getattr(self, p.key)) for p in self.__mapper__.attrs if p.key not in SKIP_KEYS])
        return d

    def update(self, **kwargs):

        for k, v in iteritems(kwargs):
            if hasattr(self, k):
                setattr(self, k, v)

    @property
    def source_table(self):

        if not self._source_table:
            name = self.source_table_name if self.source_table_name else self.name
            st = self.dataset.source_table(name)
            if not st:
                st = self.dataset.new_source_table(name)

            assert bool(st)

            self._source_table = st

        return self._source_table

    @property
    def dest_table(self):
        from .exc import NotFoundError

        if not self._dest_table:
            name = self.dest_table_name if self.dest_table_name else self.name

            try:
                self._dest_table = self.dataset.table(name)
            except NotFoundError:
                self._dest_table = self.dataset.new_table(name)

        return self._dest_table

    @property
    def column_map(self):
        """For each column, map from the source header ( column name ) to the destination header """
        return self.source_table.column_map

    @property
    def column_index_map(self):
        """For each column, map from the source header ( column name ) to the column position ( index )  """
        return self.source_table.column_index_map

    @property
    def widths(self):
        return self.source_table.widths

    @property
    def headers(self):
        return self.source_table.headers

    @property
    def is_downloadable(self):
        """Return true if the URL is probably downloadable, and is not a reference or a template"""
        return self.urltype not in ('ref', 'template')


class DataSource(DataSourceBase, Base, DictableMixin):
    """A source of data, such as a remote file or bundle"""

    __tablename__ = 'datasources'

    vid = SAColumn('st_vid', String(17), primary_key=True)
    sequence_id = SAColumn('ds_sequence_id', INTEGER)

    name = SAColumn('ds_name', Text)
    d_vid = SAColumn('ds_d_vid', String(13), ForeignKey('datasets.d_vid'), nullable=False)

    title = SAColumn('ds_title', Text)

    st_vid = SAColumn('ds_st_vid', String(22), ForeignKey('sourcetables.st_vid'), nullable=True)
    source_table_name = SAColumn('ds_st_name', Text)
    _source_table = relationship(SourceTable, backref='sources')

    t_vid = SAColumn('ds_t_vid', String(15), ForeignKey('tables.t_vid'), nullable=True)
    dest_table_name = SAColumn('ds_dt_name', Text)
    _dest_table = relationship(Table, backref='sources')

    stage = SAColumn('ds_stage', Text)
    time = SAColumn('ds_time', Text)
    space = SAColumn('ds_space', Text)
    grain = SAColumn('ds_grain', Text)
    segment = SAColumn('ds_segment', Text)
    start_line = SAColumn('ds_start_line', INTEGER)
    end_line = SAColumn('ds_end_line', INTEGER)
    comment_lines = SAColumn('ds_comment_lines', MutationList.as_mutable(JSONEncodedObj))
    header_lines = SAColumn('ds_header_lines', MutationList.as_mutable(JSONEncodedObj))
    description = SAColumn('ds_description', Text)
    file = SAColumn('ds_file', Text)
    urltype = SAColumn('ds_urltype', Text)  # null or zip
    filetype = SAColumn('ds_filetype', Text)  # tsv, csv, fixed
    encoding = SAColumn('ds_encoding', Text)
    url = SAColumn('ds_url', Text)
    ref = SAColumn('ds_ref', Text)
    hash = SAColumn('ds_hash', Text)

    generator = SAColumn('ds_generator', Text)  # class name for a Pipe to generator rows

    account_acessor = None

    __table_args__ = (
        UniqueConstraint('ds_d_vid', 'ds_name', name='_uc_ds_d_vid'),
    )


class TransientDataSource(DataSourceBase):
    """A Transient version of Data Source, which can be created temporaritly, without being stored in the
    database"""

    def __init__(self,  **kwargs):

        import inspect
        from sqlalchemy.orm.attributes import InstrumentedAttribute

        # Make sure we have all of the attributes of the DataSource class
        for name, o in inspect.getmembers(DataSource):
            if isinstance(o, InstrumentedAttribute):
                setattr(self, name, None)

        for k, v in iteritems(kwargs):
            setattr(self, k, v)
