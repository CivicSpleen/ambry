"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing partitions.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'

from collections import OrderedDict
import six
from six import string_types
from geoid.util import isimplify
from geoid.civick import GVid
from geoid import parse_to_gvid
from dateutil import parser
from sqlalchemy import event
from sqlalchemy import Column as SAColumn, Integer, UniqueConstraint
from sqlalchemy import String, ForeignKey
from sqlalchemy.orm import relationship, object_session, backref
from ambry.identity import ObjectNumber, PartialPartitionName, PartitionIdentity
from ambry.orm.columnstat import ColumnStat
from ambry.orm.dataset import Dataset
from ambry.util import Constant
import logging
from ambry.util import get_logger

logger = get_logger(__name__)
# logger.setLevel(logging.DEBUG)

from . import Base, MutationDict, MutationList, JSONEncodedObj, BigIntegerType


class PartitionDisplay(object):
    """Helper object to select what to display for titles and descriptions"""

    def __init__(self, p):

        self._p = p

        desc_used = False
        self.title = self._p.title
        self.description = ''

        if not self.title:
            self.title = self._p.table.description
            desc_used = True

        if not self.title:
            self.title = self._p.vname

        if not desc_used:
            self.description = self._p.description.strip('.') + '.' if self._p.description else ''

        self.notes = self._p.notes

    @property
    def geo_description(self):
        """Return a description of the geographic extents, using the largest scale
        space and grain coverages"""

        sc = self._p.space_coverage
        gc = self._p.grain_coverage

        if sc and gc:
            if parse_to_gvid(gc[0]).level == 'state' and parse_to_gvid(sc[0]).level == 'state':
                return parse_to_gvid(sc[0]).geo_name
            else:
                return ("{} in {}".format(
                    parse_to_gvid(gc[0]).level_plural.title(),
                    parse_to_gvid(sc[0]).geo_name))
        elif sc:
            return parse_to_gvid(sc[0]).geo_name.title()
        elif sc:
            return parse_to_gvid(gc[0]).level_plural.title()
        else:
            return ''

    @property
    def time_description(self):
        """String description of the year or year range"""

        tc = [t for t in self._p.time_coverage if t]

        if not tc:
            return ''

        mn = min(tc)
        mx = max(tc)

        if not mn and not mx:
            return ''
        elif mn == mx:
            return mn
        else:
            return "{} to {}".format(mn, mx)

    @property
    def sub_description(self):
        """Time and space dscription"""
        gd = self.geo_description
        td = self.time_description

        if gd and td:
            return '{}, {}. {} Rows.'.format(gd, td, self._p.count)
        elif gd:
            return '{}. {} Rows.'.format(gd, self._p.count)
        elif td:
            return '{}. {} Rows.'.format(td, self._p.count)
        else:
            return '{} Rows.'.format(self._p.count)


class Partition(Base):
    __tablename__ = 'partitions'

    STATES = Constant()
    STATES.SYNCED = 'synced'
    STATES.CLEANING = 'cleaning'
    STATES.CLEANED = 'cleaned'
    STATES.PREPARING = 'preparing'
    STATES.PREPARED = 'prepared'
    STATES.BUILDING = 'building'
    STATES.BUILT = 'built'
    STATES.COALESCING = 'coalescing'
    STATES.COALESCED = 'coalesced'
    STATES.ERROR = 'error'
    STATES.FINALIZING = 'finalizing'
    STATES.FINALIZED = 'finalized'
    STATES.INSTALLING = 'installing'
    STATES.INSTALLED = 'installed'

    TYPE = Constant
    TYPE.SEGMENT = 's'
    TYPE.UNION = 'u'

    sequence_id = SAColumn('p_sequence_id', Integer)
    vid = SAColumn('p_vid', String(16), primary_key=True, nullable=False)
    id = SAColumn('p_id', String(13), nullable=False)
    d_vid = SAColumn('p_d_vid', String(13), ForeignKey('datasets.d_vid'), nullable=False, index=True)
    t_vid = SAColumn('p_t_vid', String(15), ForeignKey('tables.t_vid'), nullable=False, index=True)
    name = SAColumn('p_name', String(200), nullable=False, index=True)
    vname = SAColumn('p_vname', String(200), unique=True, nullable=False, index=True)
    fqname = SAColumn('p_fqname', String(200), unique=True, nullable=False, index=True)

    title = SAColumn('p_title', String())
    description = SAColumn('p_description', String())
    notes = SAColumn('p_notes', String())

    cache_key = SAColumn('p_cache_key', String(200), unique=True, nullable=False, index=True)
    parent_vid = SAColumn('p_p_vid', String(16), ForeignKey('partitions.p_vid'), nullable=True, index=True)
    ref = SAColumn('p_ref', String(16), index=True,
                   doc='VID reference to an eariler version to use instead of this one.')
    type = SAColumn('p_type', String(20), default=TYPE.UNION,
                    doc='u - normal partition, s - segment')
    table_name = SAColumn('p_table_name', String(50))
    time = SAColumn('p_time', String(20))  # FIXME: add helptext
    space = SAColumn('p_space', String(50))
    grain = SAColumn('p_grain', String(50))
    variant = SAColumn('p_variant', String(50))
    format = SAColumn('p_format', String(50))
    segment = SAColumn('p_segment', Integer,
                       doc='Part of a larger partition. segment_id is usually also a source ds_id')
    epsg = SAColumn('p_epsg', Integer, doc='EPSG SRID for the reference system of a geographic dataset. ')

    # The partition could hold data that is considered a dimension -- if multiple datasets
    # were joined, that dimension would be a dimension column, but it only has a single
    # value in each partition.
    # That could be part of the name, or it could be declared in a table, with a single value for all of the
    # rows in a partition.

    min_id = SAColumn('p_min_id', BigIntegerType)
    max_id = SAColumn('p_max_id', BigIntegerType)
    count = SAColumn('p_count', Integer)
    state = SAColumn('p_state', String(50))
    data = SAColumn('p_data', MutationDict.as_mutable(JSONEncodedObj))

    space_coverage = SAColumn('p_scov', MutationList.as_mutable(JSONEncodedObj))
    time_coverage = SAColumn('p_tcov', MutationList.as_mutable(JSONEncodedObj))
    grain_coverage = SAColumn('p_gcov', MutationList.as_mutable(JSONEncodedObj))

    installed = SAColumn('p_installed', String(100))
    _location = SAColumn('p_location', String(100))  # Location of the data file

    __table_args__ = (
        # ForeignKeyConstraint( [d_vid, d_location], ['datasets.d_vid','datasets.d_location']),
        UniqueConstraint('p_sequence_id', 'p_d_vid', name='_uc_partitions_1'),
    )

    # For the primary table for the partition. There is one per partition, but a table
    # can be primary in multiple partitions.
    table = relationship('Table', backref='partitions', foreign_keys='Partition.t_vid')

    stats = relationship(ColumnStat, backref='partition', cascade='all, delete, delete-orphan')

    children = relationship('Partition', backref=backref('parent', remote_side=[vid]), cascade='all')

    _bundle = None  # Set when returned from a bundle.
    _datafile = None  # TODO: Unused variable.
    _datafile_writer = None  # TODO: Unused variable.
    _stats_dict = None

    @property
    def identity(self):
        """Return this partition information as a PartitionId."""

        if self.dataset is None:
            # The relationship will be null until the object is committed
            s = object_session(self)

            ds = s.query(Dataset).filter(Dataset.id_ == self.d_id).one()
        else:
            ds = self.dataset

        d = {
            'id': self.id,
            'vid': self.vid,
            'name': self.name,
            'vname': self.vname,
            'ref': self.ref,
            'space': self.space,
            'time': self.time,
            'table': self.table_name,
            'grain': self.grain,
            'variant': self.variant,
            'segment': self.segment,
            'format': self.format if self.format else 'db'
        }

        return PartitionIdentity.from_dict(dict(list(ds.dict.items()) + list(d.items())))

    @property
    def display(self):
        """Return an acessor object to get display titles and descriptions"""
        return PartitionDisplay(self)

    @property
    def bundle(self):
        return self._bundle  # Set externally, such as Bundle.wrap_partition

    @property
    def is_segment(self):
        return self.type == self.TYPE.SEGMENT

    @property
    def headers(self):
        return [c.name for c in self.table.columns]

    def __repr__(self):
        return '<partition: {} {}>'.format(self.vid, self.vname)

    def set_stats(self, stats):

        self.stats[:] = []  # Delete existing stats

        for c in self.table.columns:

            if c.name not in stats:
                continue

            d = stats[c.name].dict

            del d['name']
            del d['flags']
            cs = ColumnStat(p_vid=self.vid, d_vid=self.d_vid, c_vid=c.vid, **d)
            self.stats.append(cs)

    def parse_gvid_or_place(self, gvid_or_place):
        try:
            return parse_to_gvid(gvid_or_place)
        except KeyError:

            places = list(self._bundle._library.search.search_identifiers(gvid_or_place))

            if not places:
                err_msg = "Failed to find space identifier '{}' in full " \
                          "text identifier search  for partition '{}'" \
                    .format(gvid_or_place, str(self.identity))
                self._bundle.error(err_msg)
                return None

            return parse_to_gvid(places[0].vid)

    def set_coverage(self, stats):
        """"Extract time space and grain coverage from the stats and store them in the partition"""
        from ambry.util.datestimes import expand_to_years

        scov = set()
        tcov = set()
        grains = set()

        def summarize_maybe(gvid):
            try:
                return parse_to_gvid(gvid).summarize()
            except:
                return None

        def simplifiy_maybe(values, column):

            parsed = []

            for gvid in values:
                # The gvid should not be a st
                if gvid is None or gvid == 'None':
                    continue
                try:
                    parsed.append(parse_to_gvid(gvid))
                except ValueError as e:
                    if self._bundle:
                        self._bundle.error("Failed to parse gvid '{}' in {}.{}: {}"
                                           .format(str(gvid), column.table.name, column.name, e))

            try:
                return isimplify(parsed)
            except:
                return None

        def int_maybe(year):
            try:
                return int(year)
            except:
                return None

        for c in self.table.columns:

            if c.name not in stats:
                continue

            try:
                if stats[c.name].is_gvid or stats[c.name].is_geoid:
                    scov |= set(x for x in simplifiy_maybe(stats[c.name].uniques, c))
                    grains |= set(summarize_maybe(gvid) for gvid in stats[c.name].uniques)

                elif stats[c.name].is_year:
                    tcov |= set(int_maybe(x) for x in stats[c.name].uniques)

                elif stats[c.name].is_date:
                    # The fuzzy=True argument allows ignoring the '-' char in dates produced by .isoformat()
                    try:
                        tcov |= set(parser.parse(x, fuzzy=True).year if isinstance(x, string_types) else x.year for x in
                                    stats[c.name].uniques)
                    except ValueError:
                        pass

            except Exception as e:
                self._bundle.error("Failed to set coverage for column '{}', partition '{}': {}"
                                   .format(c.name, self.identity.vname, e))
                raise

        # Space Coverage

        if 'source_data' in self.data:

            for source_name, source in list(self.data['source_data'].items()):
                scov.add(self.parse_gvid_or_place(source['space']))

        if self.identity.space:  # And from the partition name
            try:
                scov.add(self.parse_gvid_or_place(self.identity.space))
            except ValueError:
                # Couldn't parse the space as a GVid
                pass

        # For geo_coverage, only includes the higher level summary levels, counties, states,
        # places and urban areas.
        self.space_coverage = sorted([str(x) for x in scov if bool(x) and x.sl
                                      in (10, 40, 50, 60, 160, 400)])

        #
        # Time Coverage

        # From the source
        # If there was a time value in the source that this partition was created from, then
        # add it to the years.
        if 'source_data' in self.data:
            for source_name, source in list(self.data['source_data'].items()):
                if 'time' in source:
                    for year in expand_to_years(source['time']):
                        if year:
                            tcov.add(year)

        # From the partition name
        if self.identity.name.time:
            for year in expand_to_years(self.identity.name.time):
                if year:
                    tcov.add(year)

        self.time_coverage = [t for t in tcov if t]

        #
        # Grains

        if 'source_data' in self.data:
            for source_name, source in list(self.data['source_data'].items()):
                if 'grain' in source:
                    grains.add(source['grain'])

        self.grain_coverage = sorted(str(g) for g in grains if g)

    @property
    def dict(self):
        """A dict that holds key/values for all of the properties in the
        object.

        :return:

        """

        d = {p.key: getattr(self, p.key) for p in self.__mapper__.attrs
             if p.key not in ('table', 'dataset', '_codes', 'stats', 'data', 'process_records')}

        if self.data:
            # Copy data fields into top level dict, but don't overwrite existind values.
            for k, v in six.iteritems(self.data):
                if k not in d and k not in ('table', 'stats', '_codes', 'data'):
                    d[k] = v

        return d

    @property
    def detail_dict(self):
        """A more detailed dict that includes the descriptions, sub descriptions, table
        and columns."""

        d = self.dict

        def aug_col(c):
            d = c.dict
            d['stats'] = [s.dict for s in c.stats]
            return d

        d['table'] = self.table.dict
        d['table']['columns'] = [aug_col(c) for c in self.table.columns]

        return d

    @property
    def stats_dict(self):

        class Bunch(object):
            """Dict and object access to properties"""

            def __init__(self, o):
                self.__dict__.update(o)

            def __str__(self):
                return str(self.__dict__)

            def __repr__(self):
                return repr(self.__dict__)

            def keys(self):
                return list(self.__dict__.keys())

            def items(self):
                return list(self.__dict__.items())

            def iteritems(self):
                return iter(self.__dict__.items())

            def __getitem__(self, k):
                if k in self.__dict__:
                    return self.__dict__[k]
                else:
                    from . import ColumnStat
                    return ColumnStat(hist=[])

        if not self._stats_dict:
            cols = {s.column.name: Bunch(s.dict) for s in self.stats}

            self._stats_dict = Bunch(cols)

        return self._stats_dict

    def build_sample(self):

        name = self.table.name

        count = int(
            self.database.connection.execute('SELECT count(*) FROM "{}"'.format(name)).fetchone()[0])

        skip = count / 20

        if count > 100:
            sql = 'SELECT * FROM "{}" WHERE id % {} = 0 LIMIT 20'.format(name, skip)
        else:
            sql = 'SELECT * FROM "{}" LIMIT 20'.format(name)

        sample = []

        for j, row in enumerate(self.database.connection.execute(sql)):
            sample.append(list(row.values()))

        self.record.data['sample'] = sample

        s = self.bundle.database.session
        s.merge(self.record)
        s.commit()

    @property
    def row(self):
        # Use an Ordered Dict to make it friendly to creating CSV files.
        SKIP_KEYS = [
            'sequence_id', 'vid', 'id', 'd_vid', 't_vid', 'min_key', 'max_key',
            'installed', 'ref', 'count', 'state', 'data', 'space_coverage',
            'time_coverage', 'grain_coverage', 'name', 'vname', 'fqname', 'cache_key'
        ]

        d = OrderedDict([('table', self.table.name)] +
                        [(p.key, getattr(self, p.key)) for p in self.__mapper__.attrs
                         if p.key not in SKIP_KEYS])
        return d

    def update(self, **kwargs):

        if 'table' in kwargs:
            del kwargs['table']  # In source_schema.csv, this is the name of the table, not the object

        for k, v in list(kwargs.items()):
            if hasattr(self, k):
                setattr(self, k, v)

    def finalize(self, ps=None):

        self.state = self.STATES.FINALIZING

        # Write the stats for this partition back into the partition

        with self.datafile.writer as w:
            for i, c in enumerate(self.table.columns, 1):
                wc = w.column(i)
                assert wc.pos == c.sequence_id, (c.name, wc.pos, c.sequence_id)
                wc.name = c.name
                wc.description = c.description
                wc.type = c.python_type.__name__
                self.count = w.n_rows
            w.finalize()

        if self.type == self.TYPE.UNION:
            ps.update('Running stats ', state='running')
            stats = self.datafile.run_stats()

            self.set_stats(stats)
            self.set_coverage(stats)

        self._location = 'build'

        self.title = PartitionDisplay(self).title
        self.description = PartitionDisplay(self).description

        self.state = self.STATES.FINALIZED

    # =============
    # These methods are a bit non-cohesive, since they require the _bundle value to be set, which is
    # set externally, when the object is retured from a bundle.

    def clean(self):
        """Remove all built files and return the partition to a newly-created state"""
        if self.datafile:
            self.datafile.remove()

    @property
    def location(self):

        base_location = self._location

        if not base_location:
            return None

        if self._bundle.build_fs.exists(base_location):
            if self._bundle.build_fs.hashsyspath(base_location):
                return self._bundle.build_fs.getsyspath(base_location)

        return base_location

    @location.setter
    def location(self, v):
        self._location = v

    @property
    def datafile(self):
        from ambry.exc import NotFoundError

        if self.is_local:
            # Use the local version, if it exists
            logger.debug('datafile: Using local datafile {}'.format(self.vname))
            return self.local_datafile
        else:
            # If it doesn't try to get the remote.
            try:
                logger.debug('datafile: Using remote datafile {}'.format(self.vname))
                return self.remote_datafile
            except NotFoundError:
                # If the remote doesnt exist, return the local, so the caller can call  exists() on it,
                # get its path, etc.
                return self.local_datafile

    @property
    def local_datafile(self):
        """Return the datafile for this partition, from the build directory, the remote, or the warehouse"""
        from ambry_sources import MPRowsFile
        from fs.errors import ResourceNotFoundError
        from ambry.orm.exc import NotFoundError

        try:
            return MPRowsFile(self._bundle.build_fs, self.cache_key)

        except ResourceNotFoundError:
            raise NotFoundError(
                'Could not locate data file for partition {} (local)'.format(self.identity.fqname))

    @property
    def remote(self):
        """
        Return the remote for this partition

        :return:

        """
        from ambry.exc import NotFoundError

        ds = self.dataset

        if 'remote_name' not in ds.data:
            raise NotFoundError('Could not determine remote for partition: {}'.format(self.identity.fqname))

        return self._bundle.library.remote(ds.data['remote_name'])

    @property
    def remote_datafile(self):

        from fs.errors import ResourceNotFoundError
        from ambry.exc import AccessError, NotFoundError
        from boto.exception import S3ResponseError

        try:

            from ambry_sources import MPRowsFile

            remote = self.remote

            datafile = MPRowsFile(remote.fs, self.cache_key)

            if not datafile.exists:
                raise NotFoundError(
                    'Could not locate data file for partition {} from remote {} : file does not exist'
                        .format(self.identity.fqname, remote))

        except ResourceNotFoundError as e:
            raise NotFoundError('Could not locate data file for partition {} (remote): {}'
                                .format(self.identity.fqname, e))
        except S3ResponseError as e:
            # HACK. It looks like we get the response error with an access problem when
            # we have access to S3, but the file doesn't exist.
            raise NotFoundError("Can't access MPR file for {} in remote {}".format(self.cache_key, remote.fs))

        return datafile

    @property
    def is_local(self):
        """Return true is the partition file is local"""
        from ambry.orm.exc import NotFoundError
        try:
            if self.local_datafile.exists:
                return True
        except NotFoundError:
            pass

        return False

    def localize(self, ps=None):
        """Copy a non-local partition file to the local build directory"""
        from filelock import FileLock
        from ambry.util import ensure_dir_exists
        from ambry_sources import MPRowsFile
        from fs.errors import ResourceNotFoundError

        if self.is_local:
            return

        local = self._bundle.build_fs

        b = self._bundle.library.bundle(self.identity.as_dataset().vid)

        remote = self._bundle.library.remote(b)

        lock_path = local.getsyspath(self.cache_key + '.lock')

        ensure_dir_exists(lock_path)

        lock = FileLock(lock_path)

        if ps:
            ps.add_update(message='Localizing {}'.format(self.identity.name),
                          partition=self,
                          item_type='bytes',
                          state='downloading')

        if ps:
            def progress(bts):
                if ps.rec.item_total is None:
                    ps.rec.item_count = 0

                if not ps.rec.data:
                    ps.rec.data = {}  # Should not need to do this.
                    return self

                item_count = ps.rec.item_count + bts
                ps.rec.data['updates'] = ps.rec.data.get('updates', 0) + 1

                if ps.rec.data['updates'] % 32 == 1:
                    ps.update(message='Localizing {}'.format(self.identity.name),
                              item_count=item_count)
        else:
            from ambry.bundle.process import call_interval
            @call_interval(5)
            def progress(bts):
                self._bundle.log("Localizing {}. {} bytes downloaded".format(self.vname, bts))

        def exception_cb(e):
            raise e

        with lock:
            # FIXME! This won't work with remote ( http) API, only FS ( s3:, file:)

            if self.is_local:
                return self

            try:
                with remote.fs.open(self.cache_key + MPRowsFile.EXTENSION, 'rb') as f:
                    event = local.setcontents_async(self.cache_key + MPRowsFile.EXTENSION,
                                                    f,
                                                    progress_callback=progress,
                                                    error_callback=exception_cb)
                    event.wait()
                    if ps:
                        ps.update_done()
            except ResourceNotFoundError as e:
                from ambry.orm.exc import NotFoundError
                raise NotFoundError("Failed to get MPRfile '{}' from {}: {} "
                                    .format(self.cache_key, remote.fs, e))

        return self

    @property
    def reader(self):
        from ambry.orm.exc import NotFoundError
        from fs.errors import ResourceNotFoundError
        """The reader for the datafile"""

        try:
            return self.datafile.reader
        except ResourceNotFoundError:
            raise NotFoundError("Failed to find partition file, '{}' "
                                .format(self.datafile.path))

    def select(self, predicate=None, headers=None):
        """
        Select rows from the reader using a predicate to select rows and and itemgetter to return a
        subset of elements
        :param predicate: If defined, a callable that is called for each row, and if it returns true, the
        row is included in the output.
        :param headers: If defined, a list or tuple of header names to return from each row
        :return: iterable of results

        WARNING: This routine works from the reader iterator, which returns RowProxy objects. RowProxy objects
        are reused, so if you construct a list directly from the output from this method, the list will have
        multiple copies of a single RowProxy, which will have as an inner row the last result row. If you will
        be directly constructing a list, use a getter that extracts the inner row, or which converts the RowProxy
        to a dict:

            list(s.datafile.select(lambda r: r.stusab == 'CA', lambda r: r.dict ))

        """

        # FIXME; in Python 3, use yield from
        with self.reader as r:
            for row in r.select(predicate, headers):
                yield row

    def __iter__(self):
        """ Iterator over the partition, returning RowProxy objects.
        :return: a generator
        """

        with self.reader as r:
            for row in r:
                yield row

    @property
    def analysis(self):
        """Return an AnalysisPartition proxy, which wraps this partition to provide acess to
        dataframes, shapely shapes and other analysis services"""
        if isinstance(self, PartitionProxy):
            return AnalysisPartition(self._obj)
        else:
            return AnalysisPartition(self)

    @property
    def measuredim(self):
        """Return a MeasureDimension proxy, which wraps the partition to provide access to
        columns in terms of measures and dimensions"""

        if isinstance(self, PartitionProxy):
            return MeasureDimensionPartition(self._obj)
        else:
            return MeasureDimensionPartition(self)

    # ============================

    def update_id(self, sequence_id=None):
        """Alter the sequence id, and all of the names and ids derived from it. This
        often needs to be done after an IntegrityError in a multiprocessing run"""

        if sequence_id:
            self.sequence_id = sequence_id

        self._set_ids(force=True)

        if self.dataset:
            self._update_names()

    def _set_ids(self, force=False):

        if not self.sequence_id:
            from .exc import DatabaseError

            raise DatabaseError('Sequence ID must be set before insertion')

        if not self.vid or force:
            assert bool(self.d_vid)
            assert bool(self.sequence_id)
            don = ObjectNumber.parse(self.d_vid)
            assert don.revision
            on = don.as_partition(self.sequence_id)
            self.vid = str(on.rev(don.revision))
            self.id = str(on.rev(None))

        if not self.data:
            self.data = {}

    def _update_names(self):
        """Update the derived names"""

        d = dict(
            table=self.table_name,
            time=self.time,
            space=self.space,
            grain=self.grain,
            variant=self.variant,
            segment=self.segment
        )

        assert self.dataset

        name = PartialPartitionName(**d).promote(self.dataset.identity.name)

        self.name = str(name.name)
        self.vname = str(name.vname)
        self.cache_key = name.cache_key
        self.fqname = str(self.identity.fqname)

    @staticmethod
    def before_insert(mapper, conn, target):
        """event.listen method for Sqlalchemy to set the sequence for this
        object and create an ObjectNumber value for the id_"""

        target._set_ids()

        if target.name and target.vname and target.cache_key and target.fqname and not target.dataset:
            return

        Partition.before_update(mapper, conn, target)

    @staticmethod
    def before_update(mapper, conn, target):
        target._update_names()

    @staticmethod
    def before_delete(mapper, conn, target):
        pass


event.listen(Partition, 'before_insert', Partition.before_insert)
event.listen(Partition, 'before_update', Partition.before_update)
event.listen(Partition, 'before_delete', Partition.before_delete)


class PartitionProxy(object):
    __slots__ = ["_obj", "__weakref__"]

    def __init__(self, obj):
        object.__setattr__(self, "_obj", obj)

    #
    # proxying (special cases)
    #
    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_obj"), name)

    def __delattr__(self, name):
        delattr(object.__getattribute__(self, "_obj"), name)

    def __setattr__(self, name, value):
        setattr(object.__getattribute__(self, "_obj"), name, value)

    def __nonzero__(self):
        return bool(object.__getattribute__(self, "_obj"))

    def __str__(self):
        return "<{}: {}>".format(type(self), str(object.__getattribute__(self, "_obj")))

    def __repr__(self):
        return "<{}: {}>".format(type(self), repr(object.__getattribute__(self, "_obj")))

    def __iter__(self):
        return iter(object.__getattribute__(self, "_obj"))


class AnalysisPartition(PartitionProxy):
    """A subclass of Partition with methods designed for analysis with Pandas. It is produced from
    the partitions analysis property"""

    def dataframe(self, predicate=None, filtered_columns=None, columns=None, df_class=None):
        """Return the partition as a Pandas dataframe


        :param predicate: If defined, a callable that is called for each row, and if it returns true, the
        row is included in the output.
        :param filtered_columns: If defined, the value is a dict of column names and
        associated values. Only rows where all of the named columms have the given values will be returned.
        Setting the argument will overwrite any value set for the predicate
        :param columns: A list or tuple of column names to return

        :return: Pandas dataframe

        """

        from operator import itemgetter
        from ambry.pands import AmbryDataFrame

        df_class = df_class or AmbryDataFrame

        if columns:
            ig = itemgetter(*columns)
        else:
            ig = None
            columns = self.table.header

        if filtered_columns:

            def maybe_quote(v):
                from six import string_types
                if isinstance(v, string_types):
                    return '"{}"'.format(v)
                else:
                    return v

            code = ' and '.join("row.{} == {}".format(k, maybe_quote(v))
                                for k, v in filtered_columns.items())

            predicate = eval('lambda row: {}'.format(code))

        if predicate:
            def yielder():
                for row in self.reader:
                    if predicate(row):
                        if ig:
                            yield ig(row)
                        else:
                            yield row.dict

            df = df_class(yielder(), columns=columns, partition=self.measuredim)

            return df

        else:

            def yielder():
                for row in self.reader:
                    yield row.values()

            # Put column names in header order
            columns = [c for c in self.table.header if c in columns]

            return df_class(yielder(), columns=columns, partition=self.measuredim)

    def geoframe(self, simplify=None, predicate=None, crs=None, epsg=None):
        """
        Return geopandas dataframe

        :param simplify: Integer or None. Simplify the geometry to a tolerance, in the units of the geometry.
        :param predicate: A single-argument function to select which records to include in the output.
        :param crs: Coordinate reference system information
        :param epsg: Specifiy the CRS as an EPGS number.
        :return: A Geopandas GeoDataFrame
        """
        import geopandas
        from shapely.wkt import loads
        from fiona.crs import from_epsg

        if crs is None and epsg is None and self.epsg is not None:
            epsg = self.epsg

        if crs is None:
            try:
                crs = from_epsg(epsg)
            except TypeError:
                raise TypeError('Must set either crs or epsg for output.')

        df = self.dataframe(predicate=predicate)
        geometry = df['geometry']

        if simplify:
            s = geometry.apply(lambda x: loads(x).simplify(simplify))
        else:
            s = geometry.apply(lambda x: loads(x))

        df['geometry'] = geopandas.GeoSeries(s)

        return geopandas.GeoDataFrame(df, crs=crs, geometry='geometry')

    def shapes(self, simplify=None, predicate=None):
        """
        Return geodata as a list of Shapely shapes

        :param simplify: Integer or None. Simplify the geometry to a tolerance, in the units of the geometry.
        :param predicate: A single-argument function to select which records to include in the output.

        :return: A list of Shapely objects
        """

        from shapely.wkt import loads

        if not predicate:
            predicate = lambda row: True

        if simplify:
            return [loads(row.geometry).simplify(simplify) for row in self if predicate(row)]
        else:
            return [loads(row.geometry) for row in self if predicate(row)]

    def patches(self, basemap, simplify=None, predicate=None, args_f=None, **kwargs):
        """
        Return geodata as a list of Matplotlib patches

        :param basemap: A mpl_toolkits.basemap.Basemap
        :param simplify: Integer or None. Simplify the geometry to a tolerance, in the units of the geometry.
        :param predicate: A single-argument function to select which records to include in the output.
        :param args_f: A function that takes a row and returns a dict of additional args for the Patch constructor

        :param kwargs: Additional args to be passed to the descartes Path constructor
        :return: A list of patch objects
        """
        from descartes import PolygonPatch
        from shapely.wkt import loads
        from shapely.ops import transform

        if not predicate:
            predicate = lambda row: True

        def map_xform(x, y, z=None):
            return basemap(x, y)

        def make_patch(shape, row):

            args = dict(kwargs.items())

            if args_f:
                args.update(args_f(row))

            return PolygonPatch(transform(map_xform, shape), **args)

        def yield_patches(row):

            if simplify:
                shape = loads(row.geometry).simplify(simplify)
            else:
                shape = loads(row.geometry)

            if shape.geom_type == 'MultiPolygon':
                for subshape in shape.geoms:
                    yield make_patch(subshape, row)
            else:
                yield make_patch(shape, row)

        return [patch for row in self if predicate(row)
                for patch in yield_patches(row)]


class MeasureDimensionPartition(PartitionProxy):
    """A partition proxy for accessing measure and dimensions. When returning a column, it returns
    a PartitionColumn, which proxies the table column while adding partition specific functions. """

    def __init__(self, obj):

        super(MeasureDimensionPartition, self).__init__(obj)

        self.filters = {}

    def column(self, c_name):

        return PartitionColumn(self.table.column(c_name), self)

    @property
    def columns(self):
        """Iterate over all columns"""

        return [PartitionColumn(c, self) for c in self.table.columns]

    @property
    def primary_columns(self):
        """Iterate over the primary columns, columns which do not have a parent"""

        return [c for c in self.columns if not c.parent]

    @property
    def dimensions(self):
        """Iterate over all dimensions"""
        from ambry.valuetype.core import ROLE

        return [c for c in self.columns if c.role == ROLE.DIMENSION]

    @property
    def primary_dimensions(self):
        """Iterate over the primary columns, columns which do not have a parent and have a
        cardinality greater than 1"""
        from ambry.valuetype.core import ROLE

        return [c for c in self.columns
                if not c.parent and c.role == ROLE.DIMENSION and c.pstats.nuniques > 1]

    @property
    def measures(self):
        """Iterate over all measures"""
        from ambry.valuetype.core import ROLE

        return [c for c in self.columns if c.role == ROLE.MEASURE]

    def measure(self, vid):
        """Return a measure, given its vid or another reference"""

        from ambry.orm import Column

        if isinstance(vid, PartitionColumn):
            return vid
        elif isinstance(vid, Column):
            return PartitionColumn(vid)
        else:
            return PartitionColumn(self.table.column(vid), self)

    def dimension(self, vid):
        """Return a dimention, given its vid or another reference"""
        from ambry.orm import Column

        if isinstance(vid, PartitionColumn):
            return vid
        elif isinstance(vid, Column):
            return PartitionColumn(vid)
        else:
            return PartitionColumn(self.table.column(vid), self)

    @property
    def primary_measures(self):
        """Iterate over the primary measures, columns which do not have a parent"""

        return [c for c in self.measures if not c.parent]

    @property
    def dict(self):

        d = self.detail_dict


        d['dimension_sets'] = self.enumerate_dimension_sets()

        return d

    def dataframe(self, measure, p_dim, s_dim=None, filters={}, df_class=None):
        """
        Return a dataframe with a sumse of the columns of the partition, including a measure and one
        or two dimensions. FOr dimensions that have labels, the labels are included

        The returned dataframe will have extra properties to describe the conversion:

        * plot_axes: List of dimension names for the first and second axis
        * labels: THe names of the label columns for the axes
        * filtered: The `filters` dict
        * floating: The names of primary dimensions that are not axes nor filtered

        THere is also an iterator, `rows`, which returns the header and then all of the rows.

        :param measure: The column names of one or more measures
        :param p_dim: The primary dimension. This will be the index of the dataframe.
        :param s_dim: a secondary dimension. The returned frame will be unstacked on this dimension
        :param filters: A dict of column names, mapped to a column value, indicating rows to select. a
        row that passes the filter must have the values for all given rows; the entries are ANDED
        :param df_class:
        :return: a Dataframe, with extra properties
        """

        import numpy as np

        measure = self.measure(measure)

        p_dim = self.dimension(p_dim)

        assert p_dim

        if s_dim:
            s_dim = self.dimension(s_dim)

        columns = set([measure.name, p_dim.name])

        if p_dim.label:

            # For geographic datasets, also need the gvid
            if p_dim.geoid:
                columns.add(p_dim.geoid.name)

            columns.add(p_dim.label.name)

        if s_dim:

            columns.add(s_dim.name)

            if s_dim.label:
                columns.add(s_dim.label.name)

        def maybe_quote(v):
            from six import string_types
            if isinstance(v, string_types):
                return '"{}"'.format(v)
            else:
                return v

        # Create the predicate to filter out the filtered dimensions
        if filters:

            selected_filters = []

            for k, v in filters.items():
                if isinstance(v, dict):
                    # The filter is actually the whole set of possible options, so
                    # just select the first one
                    v = v.keys()[0]

                selected_filters.append("row.{} == {}".format(k, maybe_quote(v)))

            code = ' and '.join(selected_filters)

            predicate = eval('lambda row: {}'.format(code))
        else:
            code = None

            def predicate(row):
                return True

        df = self.analysis.dataframe(predicate, columns=columns, df_class=df_class)

        if df is None or df.empty or len(df) == 0:
            return None

        # So we can track how many records were aggregated into each output row
        df['_count'] = 1

        def aggregate_string(x):
            return ', '.join(set(str(e) for e in x))

        agg = {
            '_count': 'count',

        }

        for col_name in columns:
            c = self.column(col_name)

            # The primary and secondary dimensions are put into the index by groupby
            if c.name == p_dim.name or (s_dim and c.name == s_dim.name):
                continue

            # FIXME! This will only work if the child is only level from the parent. Should
            # have an acessor for the top level.
            if c.parent and (c.parent == p_dim.name or (s_dim and c.parent == s_dim.name)):
                continue

            if c.is_measure:
                agg[c.name] = np.mean

            if c.is_dimension:
                agg[c.name] = aggregate_string

        plot_axes = [p_dim.name]

        if s_dim:
            plot_axes.append(s_dim.name)

        df = df.groupby(list(columns - set([measure.name]))).agg(agg).reset_index()

        df._metadata = ['plot_axes', 'filtered', 'floating', 'labels', 'dimension_set', 'measure']

        df.plot_axes = [c for c in plot_axes]
        df.filtered = filters

        # Dimensions that are not specified as axes nor filtered
        df.floating = list(set(c.name for c in self.primary_dimensions) -
                           set(df.filtered.keys()) -
                           set(df.plot_axes))

        df.labels = [self.column(c).label.name if self.column(c).label else c for c in df.plot_axes]

        df.dimension_set = self.dimension_set(p_dim, s_dim=s_dim)

        df.measure = measure.name

        def rows(self):
            yield ['id'] + list(df.columns)

            for t in df.itertuples():
                yield list(t)

        # Really should not do this, but I don't want to re-build the dataframe with another
        # class
        df.__class__.rows = property(rows)

        return df

    def dimension_set(self, p_dim, s_dim=None, dimensions=None, extant=set()):
        """
        Return a dict that describes the combination of one or two dimensions, for a plot

        :param p_dim:
        :param s_dim:
        :param dimensions:
        :param extant:
        :return:
        """

        if not dimensions:
            dimensions = self.primary_dimensions

        key = p_dim.name

        if s_dim:
            key += '/' + s_dim.name

        # Ignore if the key already exists or the primary and secondary dims are the same
        if key in extant or p_dim == s_dim:
            return

        # Don't allow geography to be a secondary dimension. It must either be a primary dimension
        # ( to make a map ) or a filter, or a small-multiple
        if s_dim and s_dim.valuetype_class.is_geo():
            return

        extant.add(key)

        filtered = {}

        for d in dimensions:
            if d != p_dim and d != s_dim:
                filtered[d.name] = d.pstats.uvalues.keys()

        if p_dim.valuetype_class.is_time():
            value_type = 'time'
            chart_type = 'line'
        elif p_dim.valuetype_class.is_geo():
            value_type = 'geo'
            chart_type = 'map'
        else:
            value_type = 'general'
            chart_type = 'bar'

        return dict(
            key=key,
            p_dim=p_dim.name,
            p_dim_type=value_type,
            p_label=p_dim.label_or_self.name,
            s_dim=s_dim.name if s_dim else None,
            s_label=s_dim.label_or_self.name if s_dim else None,
            filters=filtered,
            chart_type=chart_type
        )

    def enumerate_dimension_sets(self):

        dimension_sets = {}

        dimensions = self.primary_dimensions

        extant = set()

        for d1 in dimensions:

            ds = self.dimension_set(d1, None, dimensions, extant)

            if ds:
                dimension_sets[ds['key']] = ds

        for d1 in dimensions:
            for d2 in dimensions:

                if d2.cardinality >= d1.cardinality:
                    d1, d2 = d2, d1

                ds = self.dimension_set(d1, d2, dimensions, extant)

                if ds:
                    dimension_sets[ds['key']] = ds

        return dimension_sets


class ColumnProxy(PartitionProxy):
    def __init__(self, obj, partition):
        object.__setattr__(self, "_obj", obj)
        object.__setattr__(self, "_partition", partition)


MAX_LABELS = 75  # Maximum number of uniques records before it's assume that the values aren't valid labels


class PartitionColumn(ColumnProxy):
    """A proxy on the Column that links a Column to a Partition, for direct access to the stats
    and column labels"""

    def __init__(self, obj, partition):
        super(PartitionColumn, self).__init__(obj, partition)
        object.__setattr__(self, "pstats", partition.stats_dict[obj.name])

    @property
    def children(self):
        """"Return the table's other column that have this column as a parent, excluding labels"""
        for child in self.children:
            yield PartitionColumn(child, self._partition)

    @property
    def label(self):
        """"Return first child that of the column that is marked as a label"""
        for c in self.table.columns:
            if c.parent == self.name and 'label' in c.valuetype:
                return PartitionColumn(c, self._partition)

    @property
    def value_labels(self):
        """Return a map of column code values mapped to labels, for columns that have a label column

        If the column is not assocaited with a label column, it returns an identity map.

        WARNING! This reads the whole partition, so it is really slow

        """

        from operator import itemgetter

        card = self.pstats.nuniques

        if self.label:
            ig = itemgetter(self.name, self.label.name)
        elif self.pstats.nuniques < MAX_LABELS:
            ig = itemgetter(self.name, self.name)
        else:
            return {}

        label_set = set()
        for row in self._partition:
            label_set.add(ig(row))

            if len(label_set) >= card:
                break

        d = dict(label_set)

        assert len(d) == len(label_set)  # Else the label set has multiple values per key

        return d

    @property
    def cardinality(self):
        """Returns the bymber of unique elements"""

        return self.pstats.nuniques

    def __repr__(self):
        return "<{} {}>".format(self.__class__.__name__, self.name)
