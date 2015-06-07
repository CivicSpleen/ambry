"""The Bundle object is the root object for a bundle, which includes acessors
for partitions, schema, and the filesystem.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import logging
import os

from ..filesystem import BundleFilesystem
from ambry.bundle.schema import Schema
from ..partitions import Partitions
from ..util import get_logger
from ..dbexceptions import ConfigurationError, ProcessError
from ..util import memoize


class Bundle(object):
    """Represents a bundle, including all configuration and top level
    operations."""

    _logger = None

    _database = None
    _schema = None
    _partitions = None
    _library = None
    _identity = None
    _repository = None
    # Needed in LibraryDbBundle to  disambiguate multiple datasets
    _dataset_id = None
    _library_name = 'default'

    def __init__(self, logger=None):
        """"""

        # This bit of wackiness allows the var(self.run_args) code
        # to work when there have been no args parsed.
        class null_args(object):
            none = None
            multi = False
            test = False

        self._errors = []
        self._warnings = []

        self.run_args = vars(null_args())

    def __del__(self):
        try:
            self.close()
        except NotImplementedError:
            pass

    def close(self):
        """Close the bundle database and all partition databases, committing
        and closing any sessions and connections."""
        from ambry.orm.exc import DatabaseMissingError
        from ambry.orm.exc import NotFoundError

        self.partitions.close()

        if self._database:
            try:
                # self.logger.debug("Closing bundle:
                # {}".format(self.identity.sname)) # self.identity makes a DB
                # call
                self._database.session.commit()
                self._database.close()
            except NotFoundError as e:
                self.logger.debug(
                    "Error closing {}: {}".format(
                        self._database.path,
                        e))
            except DatabaseMissingError:
                pass  # It was never really open

    @property
    def log_file(self):

        return self.path + ".log"



    def clear_logger(self):
        # Force the logger to re-open, which will re-create the file that just
        # got deleted
        self._logger = None
        # clear_logger(__name__)

    @property
    def database(self):
        """The database object for the bundle."""
        from ambry.orm.exc import DatabaseMissingError

        if self._database is None:
            raise DatabaseMissingError('Database has not been set')

        return self._database

    @property
    def schema(self):
        """The Schema object, which access all of the tables and columns in the
        bundle."""
        if self._schema is None:
            self._schema = Schema(self)

        return self._schema

    @property
    def partitions(self):
        """The Partitions object, for creating and accessing partitions."""
        if self._partitions is None:
            self._partitions = Partitions(self)

        return self._partitions

    def get_dataset(self):
        raise NotImplementedError()

    @property
    def dataset(self):
        """Return the dataset, the database object that holds the identity
        values like id, vid, vname, etc."""

        return self.get_dataset()

    def set_value(self, group, key, value):

        with self.session as s:
            return self.database.set_config_value( self.dataset.vid, group, key, value, session=s)



    def get_value(self, group, key, default=None):
        """Get a config value using the current bundle's configuration
        group."""
        v = self.database.get_config_value(self.dataset.vid, group, key)

        if v is None and default is not None:
            return default
        else:
            return v

    def get_value_group(self, group):
        return self.database.get_config_group(group, d_vid=self.dataset.vid)

    def _dep_cb(self, library, key, name, resolved_bundle):
        """A callback that is called when the library resolves a dependency.

        It stores the resolved dependency into the bundle database

        """

        if resolved_bundle.partition:
            ident = resolved_bundle.partition.identity
        else:
            ident = resolved_bundle.identity

        if not self.database.is_empty():
            with self.session:
                self.set_value('rdep', key, ident.dict)


    def sub_template(self, t):
        """Substitute some data values into a format() template.
        Deprecated. Use JINJA format substitutions, builtin to the metadata"""
        d = {}
        for r in self.metadata.rows:

            if r[0][0] in ('about', 'identity', 'names', 'config', 'external_documentation'):
                k = '_'.join([str(x) for x in r[0] if x])
                d[k] = r[1]

        try:
            # This should not be necessary, but it handles old templates that get substituted with Jina format
            # titles and such.
            return unicode(t).format(**d)
        except KeyError as e:
            import json

            self.error("Failed to substitute template in {}. Key Error: {}".format(self.identity,e))

            self.error("Available keys are:\n {}".format(json.dumps(d,indent=4)))
            return t

    @property
    def library(self):
        """Return the library set for the bundle, or local library from
        get_library() if one was not set."""

        from ..library import new_library

        if self._library:
            l = self._library
        else:
            l = new_library(self.config.library(self._library_name))

        l.logger = self.logger
        l.database.logger = self.logger
        l._bundle = self
        l.dep_cb = self._dep_cb

        return l

    @library.setter
    def library(self, value):
        self._library = value

    @property
    def library_name(self):
        return self._library_name

    @library_name.setter
    def library_name(self, value):
        self._library_name = value
        self._library = None

    @property
    def path(self):
        """Return the base path for the bundle, usually the path to the bundle
        database, but withouth the database extension."""
        raise NotImplementedError("Abstract")

    def sub_dir(self, *args):
        """Return a subdirectory relative to the bundle's database root path
        which based on the path of the database. For paths relative to the
        directory of a BuildBundle, use the Filesystem object.

        :param args: Zero or more path elements that will be concatenated and suffixed to the root path

        """
        return os.path.join(self.path, *args)

    def query(self, *args, **kwargs):
        """Convience function for self.database.connection.execute()"""
        return self.database.query(*args, **kwargs)



    def _build_info(self):
        from collections import OrderedDict

        process = self.get_value_group('process')

        return OrderedDict(
            created=process.get('dbcreated', ''),
            prepared=process.get('prepared', ''),
            built=process.get('built' ''),
            build_time=str(round(float(process['buildtime']), 2)) + 's' if process.get('buildtime', False) else '')

    def _info(self, identity=None):
        """Return a nested, ordered dict  of information about the bundle."""
        from collections import OrderedDict

        d = OrderedDict()

        d['identity'] = identity if identity else self.identity._info()
        d['locations'] = str(self.identity.locations).strip()

        return d

    @property
    def info(self):
        """Display useful information about the bundle."""

        out = []

        key_lengths = set()
        d = self._info()
        for k, v in d.items():
            key_lengths.add(len(k))
            if isinstance(v, dict):
                for v_key, _ in v.items():
                    key_lengths.add(len(v_key))
        kw = max(key_lengths) + 4

        f1 = "{:<" + str(kw) + "s}: {}"
        f2 = "{:>" + str(kw) + "s}: {}"

        for k, v in d.items():

            if isinstance(v, dict):
                out.append("{}".format(k.title()))
                for v_key, v2 in v.items():
                    out.append(f2.format(v_key.title(), v2))
            else:
                out.append(f1.format(k.title(), v))

        return '\n'.join(out)

    def _repr_html_(self):
        """Called by ipython to display HTML."""
        out = []

        for k, v in self._info().items():
            if isinstance(v, dict):
                out.append("<tr><td><strong>{}</strong></td><td></td></tr>".format(k.title()))
                for v_key, v2 in v.items():
                    out.append('<tr><td align="right">{}</td><td>{}</td></tr>'.format(v_key.title(), v2))
            else:
                out.append('<tr><td align="left">{}</td><td>{}</td></tr>'.format(k.title(), v))

        return "<table>\n" + "\n".join(out) + "\n</table>"


class DbBundleBase(Bundle):
    """Base class for DbBundle and LibraryDbBundle.

    A better design would for one to derive fro the other; this is
    temporary solution

    """

    @property
    def path(self):
        raise NotImplementedError()

    def get_dataset(self):
        raise NotImplementedError()

    def sub_path(self, *args):
        """For constructing paths to partitions."""
        raise NotImplementedError()

    @property
    @memoize
    def metadata(self):
        from ambry.metadata.meta import Top

        ds = self.get_dataset()

        rows = self.database.get_config_rows(ds.vid)

        t = Top()
        t.load_rows(rows)

        # The database config rows don't hold name and identity
        t.identity = self.identity.ident_dict
        t.names = self.identity.names_dict


        return t

    def _info(self, identity=None):
        """Return a nested, ordered dict  of information about the bundle."""
        from collections import OrderedDict

        d = super(DbBundleBase, self)._info(identity)

        d['source'] = OrderedDict(
            db=self.database.dsn
        )

        d['source'].update(self._build_info())

        d['partitions'] = self.partitions.count

        return d

    @property
    def identity(self):
        """Return an identity object."""
        from ..identity import LocationRef

        if not self._identity:
            self._identity = self.get_dataset().identity
            self._identity.locations.set(LocationRef.LOCATION.LIBRARY)

        return self._identity

    @property
    def dict(self):
        """Return most important information, excluding the complete schema, as
        a dict, suitable for conversion to json."""

        import markdown

        d = {}
        metadata = self.metadata.dict

        d['identity'] = metadata['identity']
        d['identity'].update(metadata['names'])
        del metadata['names']
        del metadata['identity']
        d['meta'] = metadata

        d['partitions'] = {p.vid: p.nonull_dict for p in self.partitions}
        d['tables'] = {t.vid: t.nonull_dict for t in self.schema.tables}

        # Linked_stores and linked_manifests is only in the library record
        ds = self.library.dataset(self.identity.vid)

        d['stores'] = {s.ref: s.dict for s in ds.linked_stores}
        d['manifests'] = {m.ref: m.dict for m in ds.linked_manifests}

        # Convert the list of table names in the partition record to a dict,
        # indexed by tvid.
        tables_by_name = {t.name: t.nonull_dict for t in self.schema.tables}

        for pvid, p in d['partitions'].items():
            p['table_vids'] = [tables_by_name[t]['vid'] for t in p['tables']]

        d['counts'] = dict(
            tables=len(self.schema.tables),
            partitions=self.partitions.count
        )

        if "documentation" in d['meta']:
            d['meta']['documentation']['main'] = markdown.markdown(
                self.sub_template(  d['meta']['documentation']['main'] if d['meta']['documentation']['main'] else ''))

        d['meta']['resolved_dependencies'] = self.get_value_group('rdep')

        d['meta']['process'] = self.get_value_group('process')

        return d

    @property
    def summary_dict(self):
        """A reduced version of the dict.

        WARNING: This will only produce 'other_versions' if the bundle is produced from library.list_bundles

        """

        return dict(
            meta=self.metadata.dict,
            identity=self.identity.dict,
            other_versions=[ov.dict for ov in self.identity.data.get('other_versions', [])]
        )


class DbBundle(DbBundleBase):
    def __init__(self, database_file, logger=None):
        """Initialize a db and all of its sub-components.

        If it does not exist, creates the db database and initializes the
        Dataset record and Config records from the db.yaml file. Through the
        config object, will trigger a re-load of the db.yaml file if it
        has changed.

        Order of operations is:
            Create db.db if it does not exist
        """
        from ..database.sqlite import SqliteBundleDatabase  # @UnresolvedImport

        super(DbBundle, self).__init__(logger=logger)

        self.database_file = database_file

        self._database = SqliteBundleDatabase(self, database_file)

        # Set in Library.get() and Library.find() when the user requests a
        # partition.
        self.partition = None

    def get_dataset(self):
        """Return the dataset."""

        return self.database.get_dataset()

    @property
    def path(self):
        base, _ = os.path.splitext(self.database_file)
        return base

    def sub_path(self, *args):
        """For constructing paths to partitions."""
        return os.path.join(self.path, *args)


class LibraryDbBundle(DbBundleBase):
    """A database bundle that is built in place from the data in a library."""

    def __init__(self, database, dataset_id, logger=None):
        """Initialize a db and all of its sub-components.
        """

        super(LibraryDbBundle, self).__init__(logger=logger)

        self._dataset_id = dataset_id
        self._database = database

        # Set in Library.get() and Library.find() when the user requests a
        # partition. s
        self.partition = None

    def get_dataset(self):
        """Return the dataset."""
        from sqlalchemy.orm.exc import NoResultFound
        from sqlalchemy.exc import OperationalError
        from ambry.orm.exc import NotFoundError

        from ambry.orm import Dataset

        try:
            return self.database.session.query(Dataset).filter(Dataset.vid == self._dataset_id).one()

        except NoResultFound:
            from ambry.orm.exc import NotFoundError

            raise NotFoundError("Failed to find dataset for id {} in {} ".format(self._dataset_id, self.database.dsn))

        except OperationalError:
            raise NotFoundError("No dataset record found. Probably not a bundle (c): '{}'".format(self.path))

        except Exception as e:
            from ..util import get_logger
            # self.logger can get caught in a recursion loop
            logger = get_logger(__name__)
            logger.error("Failed to get dataset: {}; {}".format(e.message, self.database.dsn))
            raise


class BuildBundle(Bundle):
    """A bundle class for building bundle files.

    Uses the bundle.yaml file for identity configuration

    """

    META_COMPLETE_MARKER = '.meta_complete'
    PROTO_SCHEMA_FILE = 'protoschema.csv'
    SCHEMA_FILE = 'schema.csv'
    SCHEMA_REVISED_FILE = 'schema-revised.csv'
    SCHEMA_OLD_FILE = 'schema-old.csv'
    CODE_FILE = 'codes.csv'

    DOC_FILE = 'meta/documentation.md'
    DOC_HTML = 'meta/documentation.html'

    SOURCES_FILE = 'meta/sources.csv'

    # Partition where to get a map from source domains to full name
    SOURCE_TERMS = 'civicknowledge.com-terms-sources'

    def __init__(self, bundle_dir=None):
        """"""
        from ..database.sqlite import BundleLockContext

        super(BuildBundle, self).__init__()

        if bundle_dir is None:
            import inspect

            bundle_dir = os.path.abspath(os.path.dirname(inspect.getfile(self.__class__)))

        if bundle_dir is None or not os.path.isdir(bundle_dir):
            from ambry.dbexceptions import BundleError

            raise BundleError("BuildBundle must be constructed on a cache. " + str(bundle_dir) + " is not a directory")

        self.bundle_dir = bundle_dir

        self._database = None

        self.filesystem = BundleFilesystem(self, self.bundle_dir)

        import base64

        self.logid = base64.urlsafe_b64encode(os.urandom(6))
        self.ptick_count = 0

        # Library for the bundle
        lib_dir = self.filesystem.path('lib')
        if os.path.exists(lib_dir):
            import sys

            sys.path.append(lib_dir)

        self._build_time = None
        self._update_time = None

        self.exit_on_fatal = True

        self._bundle_lock_context = BundleLockContext(self)



    @property
    def build_dir(self):

        try:
            cache = self.filesystem.get_cache_by_name('build')
            return cache.cache_dir
        except ConfigurationError:
            return self.filesystem.path(self.filesystem.BUILD_DIR)



    @property
    def session(self):
        return self._bundle_lock_context

    @property
    def has_session(self):
        return self.database.has_session

    @property
    @memoize
    def metadata(self):
        from ambry.metadata.meta import Top

        t = Top(path=self.bundle_dir)
        t.load_all()

        return t

    @property
    @memoize
    def config(self):
        from ..run import get_runconfig

        from ambry.cli import global_run_config

        if global_run_config:
            return global_run_config

        return get_runconfig()

    @property
    def vid(self):
        """Shortcut to the vid."""
        return self.identity.vid

    @property
    def identity(self):
        """Return an identity object."""
        from ..identity import Identity, LocationRef

        if not self._identity:
            try:

                names = self.metadata.names.items()
                idents = self.metadata.identity.items()

            except KeyError as e:
                raise ConfigurationError("Bad bundle config in: {}: {} ".format(self.bundle_dir, e))
            except AttributeError:
                raise AttributeError(
                    "Failed to get required sections of config.\nconfig_source = {}\n".format(self.config.source_ref))

            self._identity = Identity.from_dict(dict(names + idents))
            self._identity.locations.set(LocationRef.LOCATION.SOURCE)

        return self._identity

    def increment_revision(self, description):
        """Increament the revision and set a message"""
        from ..identity import Identity
        from datetime import datetime

        identity = self.identity

        # Get the latest installed version of this dataset
        prior_ident = self.library.resolve(self.identity.name)

        if prior_ident:
            prior_version = prior_ident.on.revision
        else:
            prior_version = identity.on.revision

        # If the source bundle is already incremented past the installed versions
        # use that instead.
        if self.identity.on.revision > prior_version:
            prior_version = self.identity.on.revision
            self.close()

        self.clean()
        self.prepare()
        self.close()

        identity.on.revision = prior_version + 1

        identity = Identity.from_dict(identity.ident_dict)

        self.update_configuration(identity=identity)

        # Create a new revision entry
        md = self.metadata
        md.load_all()
        md.versions[identity.on.revision] = {
            'description': description,
            'version': md.identity.version,
            'date': datetime.now().isoformat()
        }

        md.write_to_dir()

        return identity



    def update_configuration(self, identity=None, rewrite_database=True):
        # Re-writes the bundle.yaml file, with updates to the identity and partitions
        # sections.

        md = self.metadata
        md.load_all()

        if len(md.errors) > 0:
            self.error("Metadata errors in {}".format(md._path))
            for k, v in md.errors.items():
                self.error( "    {} = {}".format('.'.join([str(x) for x in k if x]), v))
            raise Exception("Metadata errors: {}".format(md.errors))

        if not identity:
            identity = self.identity

        md.identity = identity.ident_dict
        md.names = identity.names_dict

        # Partitions is hanging around in some old bundles
        if 'partitions' in md._term_values:
            del md._term_values['partitions']

        # Ensure there is an entry for every revision, if only to nag the maintainer to fill it in.
        # for i in range(1, md.identity.revision+1):
        # md.versions[i]
        #    if i == md.identity.revision:
        #        md.versions[i].version = md.identity.version

        # Load the documentation

        def read_file(fn):
            try:
                with open(self.filesystem.path(fn)) as f:
                    return f.read()
            except IOError:
                return ''

        self.update_source()

        self.write_doc_html()

        md.documentation.main = self.sub_template(read_file(self.DOC_FILE))
        md.documentation.title = md.about.title.text
        md.documentation.summary = md.about.summary.text
        md.documentation.source = md.about.source.text
        md.documentation.processed = md.about.processed.text
        md.documentation.summary = md.about.summary.text

        md.write_to_dir(write_all=True)

        # Reload some of the values from bundle.yaml into the database
        # configuration

        if rewrite_database:
            if self.database.exists():

                if self.config.build.get('dependencies'):
                    for k, v in self.config.build.get('dependencies').items():
                        self.set_value('odep', k, v)

                self.database.rewrite_dataset()

    def update_source(self):
        """Set contact_source.create or and url to one defined in the cannonical source list,
        from the SOURCE_TERMS dataset """
        from ambry.orm.exc import NotFoundError

        if not bool(self.metadata.contact_source.creator):

            source_domain = self.identity.source

            try:
                p = self.library.get(self.SOURCE_TERMS).partition

                source = p.query("SELECT * FROM sources WHERE domain = ?", source_domain).first()

                if source:
                    self.metadata.contact_source.creator.org = source.name
                    self.metadata.contact_source.creator.url = source.homepage
                else:
                    self.error("Failed to find source domain '{}' in {}".format(source_domain,self.SOURCE_TERMS))

            except NotFoundError:
                self.error("Can't expand sources; didn't find source partition '{}'".format(self.SOURCE_TERMS))

    def write_doc_html(self):
        import markdown
        import ambry.support as sdir

        html_template_file = os.path.join(
            os.path.dirname( sdir.__file__),'documentation.html')

        with open(html_template_file) as fo:
            html_template = fo.read()

        df = self.filesystem.path(self.DOC_FILE)
        hdf = self.filesystem.build_path(self.DOC_HTML)

        if os.path.exists(df):
            with open(df) as dfo:
                with open(self.filesystem.path(hdf), 'w') as hdfo:
                    in_text = dfo.read()
                    in_text_sub = self.sub_template(in_text)
                    md = markdown.markdown(in_text_sub)
                    html = unicode(html_template).format(content=md)
                    hdfo.write(html.encode("utf-8"))

    def sources(self):
        """Iterate over the sources.

        If the file is a zip file, unzip it, and if 'file' is specified,
        extract  first file that matches the 'file' regex.

        """

        for name in self.metadata.sources:
            yield (str(name), self.source(name))

    def source(self, name):
        """Return a source file path, downloaded and unzipped."""

        import re

        v = self.metadata.sources.get(name)

        if not v:
            # Try fetching as an int; a lot of the keys are years, which YAML
            # always interprets as an int
            try:
                name = int(name)
                v = self.metadata.sources.get(name)
            except ValueError:
                v = None

        if not v:
            from ..dbexceptions import ConfigurationError

            raise ConfigurationError(
                "No key in sources for '{}' ".format(name))

        if '.zip' in v.url:
            if 'file' in v and v['file']:
                unzip = re.compile(v.file)
            else:
                unzip = True

            r = self.filesystem.download(name, unzip=unzip)

            if isinstance(r, basestring):
                return r
            else:
                for fn in r:
                    return fn

        else:
            return self.filesystem.download(name)

    @property
    def dependencies(self):
        return self.config.build.get('dependencies')

    def clean(self, clean_meta=False, force_delete=False):
        """Remove all files generated by the build process."""
        from ..util import rm_rf

        self.close()

        # Remove partitions

        # Remove the database

        if self.database.exists():
            self.log("Removing {}".format(self.database.path))
            self.database.delete()

        # Remove the sqlite journal files, if they exists
        files = [
            self.database.path +"-wal",
            self.database.path +"-shm",
            self.database.path +"-journal"]

        for f in files:
            if os.path.exists(f):
                self.log("Removing {}".format(f))
                os.remove(f)

        if clean_meta:
            mf = self.filesystem.meta_path(self.META_COMPLETE_MARKER)
            if os.path.exists(mf):
                self.log("Removing {}".format(mf))
                os.remove(mf)

        ed = self.filesystem.path('extracts')
        if os.path.exists(ed):
            self.log("Removing {}".format(ed))
            rm_rf(ed)

        if os.path.exists(self.log_file):
            self.log("Removing {}".format(self.log_file))
            os.remove(self.log_file)

        # Should check for a shared download file -- specified
        # as part of the library; Don't delete that.
        # if not self.cache_downloads :
        # self.rm_rf(self.filesystem.downloads_path())

        self.set_build_state('cleaned')

        self.close()

        self.clear_logger()

        if force_delete:  # Obviates some of the earlier statements, but ...
            if os.path.exists(self.database.path):
                os.remove(self.database.path)

    def progress(self, message):
        """print message to terminal, in place."""
        import sys

        sys.stdout.write("\033[K{}\r".format(message))
        sys.stdout.flush()

    def ptick(self, message):
        """Writes a tick to the stdout, without a space or newline."""
        import sys

        sys.stdout.write(message)
        sys.stdout.flush()

        self.ptick_count += len(message)

        if self.ptick_count > 72:
            sys.stdout.write("\n")
            self.ptick_count = 0

    def init_log_rate(self, N=None, message='', print_rate=None):
        from ..util import init_log_rate as ilr

        return ilr(self.log, N=N, message=message, print_rate=print_rate)

    def run_function(self):
        pass

    # Prepare is run before building, part of the devel process.
    def pre_meta(self):
        """Skips the meta stage if the :class:.`META_COMPLETE_MARKER` file already exists"""

        self.load_requirements()

        mf = self.filesystem.meta_path(self.META_COMPLETE_MARKER)

        if os.path.exists(mf) and not self.run_args.get('clean', None):
            self.log("Meta information already generated")
            # raise ProcessError("Bundle has already been prepared")
            return False

        return True

    def meta(self):
        return True

    def post_meta(self):
        """Create the :class:.`META_COMPLETE_MARKER` meta marker so we don't run the meta process again"""
        import datetime

        mf = self.filesystem.meta_path(self.META_COMPLETE_MARKER)
        with open(mf, 'w+') as f:
            f.write(str(datetime.datetime.now()))

        return True

    # Prepare is run before building, part of the devel process.

    def load_requirements(self):
        """If there are python library requirements set, append the python dir
        to the path."""

        import sys

        if self.metadata.build.requirements.items:
            python_dir = self.config.python_dir()
            sys.path.append(python_dir)

    def pre_prepare(self):

        self.log('---- Pre-Prepare ----')

        if self.is_prepared:
            self.log("Bundle has already been prepared")
            # raise ProcessError("Bundle has already been prepared")

            return False

        if self.metadata.build.get('requirements', False):
            from ..util.packages import install
            import sys
            import imp

            python_dir = self.config.python_dir()

            if not python_dir:
                raise ConfigurationError(
                    "Can't install python requirements without a configuration item for filesystems.python")

            if not os.path.exists(python_dir):
                os.makedirs(python_dir)

            sys.path.append(python_dir)

            self.log("Installing required packages in {}".format(python_dir))

            for k, v in self.metadata.build.requirements.items():

                try:
                    imp.find_module(k)
                    self.log(
                        "Required package already installed: {}->{}".format(k, v))
                except ImportError:
                    self.log("Installing required package: {}->{}".format(k, v))
                    install(python_dir, k, v)

        try:
            from ..orm import Dataset

            b = self.library.resolve(self.identity.id_,location=[ Dataset.LOCATION.LIBRARY])

            if b and b.on.revision >= self.identity.on.revision and not self.run_args.force:
                self.fatal(
                    ("Can't build this version. Library {} has version {} of {}"
                     " which is less than or equal this version {}").format(
                        self.library.database.dsn,b.on.revision,b.fqname,self.identity.on.revision))
                return False

        except Exception:
            raise

        self.read_sources()

        if not self.metadata.about.access:
            raise ConfigurationError("about.access must be set to the name of a remote")

        if not self.metadata.about.title:
            raise ConfigurationError("Must set a title in about.title")

        if not self.metadata.about.summary:
            raise ConfigurationError("Must set a summary in about.summary")

        return True

    def _prepare_load_schema(self, fast=False):

        sf_path = self.filesystem.path('meta', self.SCHEMA_FILE)

        if os.path.exists(sf_path):
            with open(sf_path, 'rbU') as f:
                self.log("Loading schema from file: {}".format(sf_path))
                self.schema.clean()
                with self.session:
                    warnings, errors = self.schema.schema_from_file(f, fast=fast)

                    self.schema.expand_table_prototypes()

                with self.session:
                    self.schema.expand_column_prototypes()

                for title, s, f in (("Errors", errors, self.error), ("Warnings", warnings, self.warn)):
                    if s:
                        self.log("----- Schema {} ".format(title))
                        for table_name, column_name, message in s:
                            f("{:20s} {}".format(
                                "{}.{}".format(table_name if table_name else '', column_name if column_name else ''),
                                message))

                if errors:
                    self.fatal("Schema load failed. Exiting")
        else:
            self.log("No schema file ('{}') not loading schema".format(sf_path))

        cf = self.filesystem.path('meta', self.CODE_FILE)



        if os.path.exists(cf):
            self.log("Loading codes file: {}".format(cf))
            with self.session:
                self.schema.read_codes()

    def update_copy_partitions(self):
        """Create partition references for all of the partitions from the
        previous version of the bundle."""

        prior_ident = self.library.resolve(self.identity.name)
        prior = self.library.get(prior_ident.vname)

        self.partitions.clean(self.database.session)

        with self.session:
            for pp in prior.partitions:
                d = pp.record.dict
                p, _ = self.partitions._find_or_new(
                    d, format=d.get('format', None),
                    tables=d.get('tables', None),
                    data=d.get('data', None), create=False)
                # The referenced partition is also a reference.
                if pp.record.ref:
                    p.record.ref = pp.record.ref
                else:
                    p.record.ref = pp.vid

                self.log("Referenced partition {} to {}".format(pp.identity, p.identity))

    def update_copy_schema(self):
        """Copy the schema from a previous version, updating the vids."""
        from ..orm import Table, Column
        from sqlalchemy.exc import IntegrityError

        self.schema.clean()

        tables = []
        columns = []

        prior_ident = self.library.resolve(self.identity.name)
        prior = self.library.get(prior_ident.vname)

        s = self.database.session

        ds = self.dataset

        for table in prior.dataset.tables:
            d = table.dict
            del d['vid']
            t = Table(ds, **d)

            tables.append(t.insertable_dict)

            for column in table.columns:
                c = Column(t, **column.dict)

                columns.append(c.insertable_dict)

        if tables:
            s.execute(Table.__table__.insert(), tables)
            s.execute(Column.__table__.insert(), columns)

        try:
            s.commit()
        except IntegrityError as e:
            self.logger.error("Failed to merge into {}".format(self.dsn))
            s.rollback()
            raise e

    def post_update(self):
        from time import time

        with self.session:
            self.set_build_state('updated')
            self.set_value('process', 'updatetime', time() - self._update_time)
            self.update_configuration()

            self._revise_schema()

            self.schema.move_revised_schema()

            self.post_build_finalize()

            self.write_config_to_bundle()

            self.set_build_state('updated')

        self.close()

        return True

    def do_update(self):

        if not self.is_prepared:
            self.do_prepare()

        if self.pre_update():
            self.log("---- Update ---")
            if self.update_main():
                self.post_update()
                self.log("---- Done Update ---")
                r = True
            else:
                self.log("---- Update exited with failure ---")
                r = False
        else:
            self.log("---- Skipping Update ---- ")
            r = False

        return r

    # Submit the package to the library

    def pre_install(self):

        with self.session:
            self.update_configuration()

        return True

    def install(self, library_name=None, delete=False, force=True):
        """Install the bundle and all partitions in the default library."""

        self.run_args.get('force', force)

        if not self.is_built:
            self.error("Bundle hasn't been successfully built")
            return

        with self.session:

            library = self.library

            self.log("Install   {} to  library {}".format(self.identity.name,library.database.dsn))
            dest = library.put_bundle(self, install_partitions=False, file_state = 'new', force = True)
            if dest[1]:
                self.log("Installed {}".format(dest[0]))
            else:
                self.log("Previously Installed {}".format(dest[0]))

            for partition in self.partitions:
                # Skip files that don't exist, but not if the partition is a reference to an
                # other partition.
                if not os.path.exists(partition.database.path) and not partition.ref:
                    self.log("{} File does not exist, skipping".format( partition.database.path))
                    continue

                self.log("Install   {}".format(partition.name))
                dest = library.put_partition(partition, file_state = 'new')
                if dest[0]:
                    self.log("Installed {}".format(dest[0]))

                if delete:
                    os.remove(partition.database.path)
                    self.log("Deleted {}".format(partition.database.path))

                pass

        return True

    def post_install(self):

        self.set_build_state('installed')

        return True

    def repopulate(self):
        """Pull bundle files from the library back into the working
        directory."""
        import shutil

        self.log('---- Repopulate ----')

        b = self.library.get(self.identity.name)

        self.log(
            'Copy bundle from {} to {} '.format(
                b.database.path,
                self.database.path))

        if not os.path.isdir(os.path.dirname(self.database.path)):
            os.makedirs(os.path.dirname(self.database.path))

        shutil.copy(b.database.path, self.database.path)

        # Restart with the new bundle database.
        newb = BuildBundle(self.bundle_dir)

        for newp in newb.partitions:
            self.log('Copy partition: {}'.format(newp.identity.name))

            b = self.library.get(newp.identity.vname)

            dir_ = os.path.dirname(newp.database.path)

            if not os.path.isdir(dir_):
                os.makedirs(dir_)

            shutil.copy(b.partition.database.path, newp.database.path)
            self.log(
                'Copied {} to {}'.format(
                    b.partition.database.path,
                    newp.database.path))

    def set_args(self, args):

        from ..util import AttrDict

        self.run_args = AttrDict(vars(args))

    def run_mp(self, method, arg_sets):
        from ..run import mp_run
        from multiprocessing import Pool, cpu_count

        if len(arg_sets) == 0:
            return

        # Argsets should be tuples, but for one ag functions, the
        # caller may pass in a scalar, which we have to convert.
        if not isinstance(arg_sets[0], (list, tuple)):
            arg_sets = ((arg,) for arg in arg_sets)

        n = int(self.run_args.get('multi'))

        if n == 0:
            n = cpu_count()

        if n == 1:
            self.log(
                "Requested MP run, but for only 1 process; running in process instead")
            for args in arg_sets:
                method(*args)
        else:
            self.log("Multi processor run with {} processes".format(n))

            # Closing is really important!
            self.database.close()
            # Don't let the database file descriptor cross into the child
            # process.
            self.library.database.close()

            pool = Pool(n)

            pool.map(mp_run, [(self.bundle_dir, dict(self.run_args), method.__name__, args) for args in arg_sets])

    def _info(self, identity=None):
        """Return a nested, ordered dict  of information about the bundle."""
        from collections import OrderedDict

        d = super(BuildBundle, self)._info(identity)

        d['source'] = OrderedDict(bundle=self.bundle_dir)

        deps = self.config.build.get('dependencies')
        d['build'] = OrderedDict(dependencies=deps if deps else '')

        if self.is_built:
            d['build'].update(self._build_info())

        return d
