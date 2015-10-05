"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

__docformat__ = 'restructuredtext en'

from collections import OrderedDict

from os.path import splitext

from six import iteritems
# noinspection PyUnresolvedReferences
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

    _bundle = None  # Set externally
    _datafile = None

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
    def resolved_dest_table_name(self):

        return self.dest_table_name if self.dest_table_name else (
            self.source_table_name if self.source_table_name else self.name)

    @property
    def dest_table(self):
        from .exc import NotFoundError

        if not self._dest_table:
            name = self.resolved_dest_table_name

            try:
                self._dest_table = self.dataset.table(name)
            except NotFoundError:
                self._dest_table = self.dataset.new_table(name)

        return self._dest_table

    @property
    def datafile(self):
        """Return an MPR datafile from the /ingest directory of the build filesystem"""
        from ambry_sources import MPRowsFile
        from os.path import join

        if self._datafile is None:
            self._datafile = MPRowsFile(self._bundle.build_ingest_fs, self.name)

        return self._datafile

    @property
    def spec(self):
        """Return a SourceSpec to describe this source"""
        from ambry_sources.sources import SourceSpec

        return SourceSpec(**self.dict)

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

    def update_table(self):
        """Update the source table from the datafile"""
        from ambry_sources.intuit import TypeIntuiter

        if self.datafile.exists:
            with self.datafile.reader as r:

                st = self.source_table
                for col in r.columns:

                    c = st.column(col['name'])

                    if c:
                        c.datatype = TypeIntuiter.promote_type(c.datatype, col['resolved_type'])

                        #self._bundle.log('Update column: ({}) {}.{}'.format(c.position, st.name, c.source_header))
                    else:

                        c = st.add_column(col['pos'], source_header=col['name'], dest_header=col['name'],
                                          datatype=col['resolved_type'])

                        #self._bundle.log('Created column: ({}) {}.{}'.format(c.position, st.name, c.source_header))

    def update_spec(self):
        """Update the source specification with information from the row intuiter, but only if the spec values
        are not already set. """

        if self.datafile.exists:
            with self.datafile.reader as r:

                self.header_lines = r.info['header_rows']
                self.comment_lines =  r.info['comment_rows']
                self.start_line = r.info['data_start_row']
                self.end_line = r.info['data_end_row']


    @property
    def abbrev_url(self):
        from ..util import parse_url_to_dict, unparse_url_dict

        d = parse_url_to_dict(self.url)

        d['path'] = '/.../' + d['path'].split('/').pop()

        return unparse_url_dict(d)


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
    urltype = SAColumn('ds_urltype', Text)  # null, zip, ref, template
    filetype = SAColumn('ds_filetype', Text)  # tsv, csv, fixed, partition
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
    """A Transient version of Data Source, which can be created temporarily, without being stored in the
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
