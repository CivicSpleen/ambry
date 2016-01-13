"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

__docformat__ = 'restructuredtext en'

from collections import OrderedDict

from six import iteritems

from sqlalchemy import Column as SAColumn, Text, String, ForeignKey, INTEGER, UniqueConstraint
from sqlalchemy.orm import relationship

from .source_table import SourceTable
from .table import Table

from . import MutationList, JSONEncodedObj
from . import Base,  DictableMixin
from ..util import Constant


class DataSourceBase(object):
    """Base class for data soruces, so we can have a persistent and transient versions"""

    _bundle = None  # Set externally
    _datafile = None

    # reftypes for sources that should not be downloaded or injested
    NON_DOWNLOAD_REFTYPES = ('ref', 'template', 'partition', 'sql')

    # reftypes for sources that should not be built or have schemas create for
    NON_PROCESS_REFTYPES = ('ref', 'template')

    # reftypes for sources that should not be built or have schemas create for
    NON_INGEST_REFTYPES = ('ref', 'template', 'partition',)

    STATES = Constant()
    STATES.NEW = 'new'
    STATES.INGESTING = 'ingest'
    STATES.INGESTED = 'ingest_done'
    STATES.NOTINGESTABLE = 'not_ingestable'
    STATES.BUILDING = 'build'
    STATES.BUILT = 'build_done'

    @property
    def urltype(self):
        return self.reftype

    @urltype.setter
    def urltype(self, v):
        self.reftype = v

    @property
    def url(self):
        return self.ref

    @url.setter
    def url(self, v):
        self.ref = v

    @property
    def generator(self):
        return self.ref

    @generator.setter
    def generator(self, v):
        self.reftype = 'generator'
        self.ref = v

    @property
    def dict(self):
        """A dict that holds key/values for all of the properties in the
        object.

        :return:

        """
        SKIP_KEYS = ('_source_table', '_dest_table', 'd_vid', 't_vid', 'st_id', 'dataset',
                     'hash', 'process_records')
        return OrderedDict(
            (p.key, getattr(self, p.key)) for p in self.__mapper__.attrs if p.key not in SKIP_KEYS)

    @property
    def row(self):

        # WARNING There is another .row() in TransientSource

        # Use an Ordered Dict to make it friendly to creating CSV files.
        SKIP_KEYS = ('sequence_id', 'vid', '_source_table',
                     '_dest_table', 'd_vid', 't_vid', 'st_vid', 'dataset', 'process_records')

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
    def partition(self):
        """For partition urltypes, return the partition specified by the ref """
        if self.urltype != 'partition':
            return None

        p = self._bundle.partition(self.url)

        if not p:
            p = self._bundle.library.partition(self.url)

        return p

    @property
    def datafile(self):
        """Return an MPR datafile from the /ingest directory of the build filesystem"""
        from ambry_sources import MPRowsFile

        if self._datafile is None:
            if self.urltype == 'partition':
                self._datafile = self.partition.datafile
            else:
                self._datafile = MPRowsFile(self._bundle.build_ingest_fs, self.name)

        return self._datafile

    @property
    def spec(self):
        """Return a SourceSpec to describe this source"""
        from ambry_sources.sources import SourceSpec

        url = self.ref

        # Will get the URL twice; once as ref and onces as URL, but the ref is ignored
        return SourceSpec(url, **self.dict)

    @property
    def account(self):
        """Return an account record, based on the host in the url"""
        from ambry.util import parse_url_to_dict

        d = parse_url_to_dict(self.url)

        return self._bundle.library.account(d['netloc'])

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
        return self.urltype not in self.NON_DOWNLOAD_REFTYPES

    @property
    def is_processable(self):
        """Return true if the URL is probably downloadable, and is not a reference or a template"""

        return self.urltype not in self.NON_PROCESS_REFTYPES

    @property
    def is_ingestible(self):
        """Return true if the URL is probably downloadable, and is not a reference or a template"""
        return self.urltype not in self.NON_INGEST_REFTYPES

    @property
    def is_reference(self):
        """Return true if the URL is probably downloadable, and is not a reference or a template"""

        return self.urltype in self.NON_PROCESS_REFTYPES

    @property
    def is_partition(self):
        """Return true if the reference is to a partition"""

        return self.reftype == 'partition'

    @property
    def is_relation(self):
        """Returns True if the reference is to a relation. """
        return self.reftype == 'sql'

    @property
    def is_finalized(self):

        if not self.datafile.exists:
            return False

        return self.datafile.is_finalized

    @property
    def is_ingested(self):
        return self.state == self.STATES.INGESTED

    @property
    def is_built(self):
        return self.state == self.STATES.BUILT

    def update_table(self):
        """Update the source table from the datafile"""
        from ambry_sources.intuit import TypeIntuiter

        st = self.source_table

        if self.reftype == 'partition':
            for c in self.partition.table.columns:
                st.add_column(c.sequence_id, source_header=c.name, dest_header=c.name, datatype=c.datatype)
        elif self.reftype == 'sql':
            if self._bundle.library.database.engine.name == 'sqlite':
                SQL_TO_PYTHON_MAP = {
                    'INTEGER': int,
                    'NUMERIC': float,
                    'REAL': float,
                    'CHARACTER': str,
                    'VARCHAR': str,
                    'TEXT': str
                }
                query = 'PRAGMA table_info(\'{}\');'.format(self.spec.url)
                result = self._bundle.library.database.connection.execute(query)

                for row in result:
                    position = row[0] + 1
                    name = row[1]
                    datatype = row[2]
                    if datatype.lower() == 'integer':
                        datatype = int
                    # FIXME: convert other sql types to python types.
                    st.add_column(position, name, datatype, dest_header=name)
            else:
                raise NotImplementedError(
                    '{} engine schema retrieve is not implemented.'
                    .format(self._bundle.library.database.engine.name))

        elif self.datafile.exists:
            with self.datafile.reader as r:

                for col in r.columns:

                    c = st.column(col['name'])

                    if c:
                        c.datatype = TypeIntuiter.promote_type(c.datatype, col['resolved_type'])
                    else:

                        c = st.add_column(col['pos'],
                                          source_header=col['name'],
                                          dest_header=col['name'],
                                          datatype=col['resolved_type'],
                                          description=col['description'],
                                          has_codes=col['has_codes'])

    def update_spec(self):
        """Update the source specification with information from the row intuiter, but only if the spec values
        are not already set. """

        if self.datafile.exists:
            with self.datafile.reader as r:

                self.header_lines = r.info['header_rows']
                self.comment_lines = r.info['comment_rows']
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

    vid = SAColumn('ds_vid', String(17), primary_key=True)
    sequence_id = SAColumn('ds_sequence_id', INTEGER)

    name = SAColumn('ds_name', Text)
    d_vid = SAColumn(
        'ds_d_vid', String(13), ForeignKey('datasets.d_vid'), nullable=False,
        doc='Dataset vid')

    title = SAColumn('ds_title', Text)

    st_vid = SAColumn('ds_st_vid', String(22), ForeignKey('sourcetables.st_vid'), nullable=True)
    source_table_name = SAColumn('ds_st_name', Text)
    _source_table = relationship(SourceTable, backref='sources')

    t_vid = SAColumn('ds_t_vid', String(15), ForeignKey('tables.t_vid'), nullable=True, doc='Table vid')
    dest_table_name = SAColumn('ds_dt_name', Text)
    _dest_table = relationship(Table, backref='sources')

    stage = SAColumn('ds_stage', INTEGER, default=0)  # Order in which to process sources.
    pipeline = SAColumn('ds_pipeline', Text)

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

    filetype = SAColumn('ds_filetype', Text)  # tsv, csv, fixed, partition
    encoding = SAColumn('ds_encoding', Text)

    hash = SAColumn('ds_hash', Text)

    reftype = SAColumn('ds_reftype', Text)  # null, zip, ref, template
    ref = SAColumn('ds_ref', Text)

    state = SAColumn('ds_state', Text)

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

        self.properties = []

        # Make sure we have all of the attributes of the DataSource class
        for name, o in inspect.getmembers(DataSource):
            if isinstance(o, InstrumentedAttribute):
                self.properties.append(name)
                setattr(self, name, None)

        for k, v in iteritems(kwargs):
            setattr(self, k, v)

    @property
    def row(self):

        # WARNING! There is another .row() in DataSourceBase which gets uses for non persistent records

        # Use an Ordered Dict to make it friendly to creating CSV files.
        SKIP_KEYS = ('sequence_id', 'vid', '_source_table',
                     '_dest_table', 'd_vid', 't_vid', 'st_vid', 'dataset', 'process_records')

        d = OrderedDict([(k, getattr(self, k)) for k in self.properties if k not in SKIP_KEYS])

        assert 'process_records' not in d

        return d

    @property
    def dict(self):
        """A dict that holds key/values for all of the properties in the
        object.

        :return:

        """
        SKIP_KEYS = ('_source_table', '_dest_table', 'd_vid', 't_vid', 'st_id', 'dataset', 'hash', 'process_records')
        return OrderedDict([(k, getattr(self, k)) for k in self.properties if k not in SKIP_KEYS])
