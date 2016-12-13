#
# Class to manage the test directory and prototypes

"""

This class manages the test library directory and databases, primarily by maintaining a "prototype"
of the databases. Tests that require a bundle to operate on can be sped up by copyin the protoitype
rather than creating a new library and building the bundles in it.

For Sqlite libraries, the prototype is held in the /proto directory and copied to the /sqlite directory when
then lirbary is initalized

For postgres libraries, a prototype database is constructed by appending -proto to the end of the name of the
test database. The proto databse is created and populated, and then flagged for use as a template. When a test
library is created, it is constructed with the proto library as its template.

"""

import logging
import os
import unittest

from ambry.util import ensure_dir_exists, memoize, get_logger
from ambry.library import Library

logger = get_logger(__name__, level=logging.INFO, propagate=False)

DEFAULT_ROOT = '/tmp/ambry-test'  # Default root for the library roots ( The library root is one level down )


class ProtoLibrary(object):
    """Manage test libraries. Creates a proto library, with pre-built bundles, that can be
    copied quickly into a test library, providing bundles to test against"""

    def __init__(self, config_path=None):
        """

        :param config_path:
        :return:
        """

        from ambry.run import load_config, update_config, load_accounts
        from ambry.util import parse_url_to_dict, unparse_url_dict

        self._root = DEFAULT_ROOT
        # TODO: Update root from config.

        ensure_dir_exists(self._root)

        if config_path is None:
            import test.support
            config_path = os.path.join(os.path.dirname(test.support.__file__), 'test-config')

        self.config = load_config(config_path)

        self.config.update(load_accounts())

        update_config(self.config, use_environ=False)

        assert self.config.loaded[0] == config_path + '/config.yaml'

        # Populate library and proto DSNs
        if os.environ.get('AMBRY_TEST_DB'):
            library_dsn = os.environ['AMBRY_TEST_DB']
        else:
            # Derive from library.database setting.
            dsn = self.config.library.database
            if dsn.startswith('post'):
                # postgres case.
                p = parse_url_to_dict(dsn)
                parsed_library = dict(p, path=p['path'] )
            elif dsn.startswith('sqlite'):
                # sqlite case
                p = parse_url_to_dict(dsn)
                parsed_library = dict(p, path=p['path'] )
            library_dsn = unparse_url_dict(parsed_library)

        if library_dsn.startswith('post'):
            self._db_type = 'postgres'
            p = parse_url_to_dict(library_dsn)
            parsed_proto = dict(p, path=p['path'] + '-proto')
            proto_dsn = unparse_url_dict(parsed_proto)

        elif library_dsn.startswith('sqlite'):
            self._db_type = 'sqlite'
            p = parse_url_to_dict(library_dsn)
            parsed_proto = dict(p, path=p['path'] )
            proto_dsn = unparse_url_dict(parsed_proto)
        else:
            raise Exception('Do not know how to process {} database.'.format(library_dsn))

        self.proto_dsn = proto_dsn
        self.config.library.database = library_dsn

    def __str__(self):
        return """
root:      {}
dsn:       {}
proto-dsn: {}
""".format(self._root, self.config.library.database, self.proto_dsn)

    def _ensure_exists(self, dir):
        """Ensure the full path to a directory exists. """

        if not os.path.exists(dir):
            os.makedirs(dir)

    def proto_dir(self, *args):
        """Directory where the prototype library is built, and copied from each run """

        base = os.path.join(self._root, 'proto')

        self._ensure_exists(base)

        return os.path.join(base, *args)

    def sqlite_dir(self, create=True, *args):

        base = os.path.join(self._root, 'sqlite')

        if create:
            self._ensure_exists(base)

        return os.path.join(base, *args)

    def pg_dir(self, *args):

        base = os.path.join(self._root, 'pg')

        self._ensure_exists(base)

        return os.path.join(base, *args)

    def _create_database(self, pg_dsn=None):
        """Create the database, if it does not exist"""

    def import_bundle(self, l, cache_path):
        """Import a test bundle into a library"""
        from test import bundle_tests

        orig_source = os.path.join(os.path.dirname(bundle_tests.__file__), cache_path)
        imported_bundles = l.import_bundles(orig_source, detach=True, force=True)

        b = next(b for b in imported_bundles).cast_to_subclass()
        b.clean()
        b.sync_in(force=True)
        return b

    def clean_proto(self):
        import shutil
        shutil.rmtree(self.proto_dir())

    def _proto_config(self):
        config = self.config.clone()
        self.proto_dir()  # Make sure it exists
        config.library.filesystem_root = self.proto_dir()
        config.library.database = self.proto_dsn
        return config

    def remove(self, ref):

        l = Library(self._proto_config())

        l.remove(ref)


    def build_proto(self):
        """Builds the prototype library, by building or injesting any bundles that don't
        exist in it yet. """

        from ambry.orm.exc import NotFoundError

        l = Library(self._proto_config())


        try:
            b = l.bundle('ingest.example.com-headerstypes')
        except NotFoundError:
            b = self.import_bundle(l, 'ingest.example.com/headerstypes')
            b.log('Build to: {}'.format(b.build_fs))
            b.ingest()
            b.close()

        try:
            b = l.bundle('ingest.example.com-stages')
        except NotFoundError:
            b = self.import_bundle(l, 'ingest.example.com/stages')
            b.ingest()
            b.close()

        try:
            b = l.bundle('ingest.example.com-basic')
        except NotFoundError:
            b = self.import_bundle(l, 'ingest.example.com/basic')
            b.ingest()
            b.close()

        try:
            b = l.bundle('build.example.com-coverage')
        except NotFoundError:
            b = self.import_bundle(l, 'build.example.com/coverage')
            b.ingest()
            b.source_schema()
            b.schema()
            b.build()
            b.finalize()
            b.close()

        try:
            b = l.bundle('build.example.com-generators')
        except NotFoundError:
            b = self.import_bundle(l, 'build.example.com/generators')
            b.run()
            b.finalize()
            b.close()

        try:
            b = l.bundle('build.example.com-plot')
        except NotFoundError:
            b = self.import_bundle(l, 'build.example.com/plot')
            b.build()
            b.finalize()
            b.close()

        try:
            b = l.bundle('build.example.com-casters')
        except NotFoundError:
            b = self.import_bundle(l, 'build.example.com/casters')
            b.ingest()
            b.source_schema()
            b.schema()
            b.build()
            b.finalize()
            b.close()

        try:
            b = l.bundle('build.example.com-sql')

        except NotFoundError:
            b = self.import_bundle(l, 'build.example.com/sql')
            b.build(sources=['integers', 'integers2', 'integers3'])

    def init_library(self, use_proto=True):
        """Initialize either the sqlite or pg library, based on the DSN """
        if self._db_type == 'sqlite':
            return self.init_sqlite(use_proto=use_proto)
        else:
            return self.init_pg(use_proto=use_proto)

    def init_sqlite(self, use_proto=True):

        import shutil

        shutil.rmtree(self.sqlite_dir())

        self.config.library.filesystem_root = self.sqlite_dir(create=False)

        if use_proto:
            self.build_proto()

            shutil.copytree(self.proto_dir(), self.sqlite_dir(create=False))

            return Library(self.config)

        else:
            self.sqlite_dir()  # Ensure it exists
            l = Library(self.config)
            l.create()
            return l

    def init_pg(self, use_proto=True):

        if use_proto:
            # self.create_pg_template()
            # self.build_proto()
            self.create_pg(re_create=True)
        else:
            self.create_pg(re_create=True, template_name='template1')

        l = Library(self.config)
        l.create()
        return l

    @memoize
    def pg_engine(self, dsn):
        """Return a Sqlalchemy engine for a database, by dsn. The result is cached. """
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool

        return create_engine(dsn, poolclass=NullPool)

    @property
    @memoize
    def pg_root_engine(self):
        """Return an engine connected to the postgres database, for executing operations on other databases"""
        from ambry.util import set_url_part
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool

        root_dsn = set_url_part(self.config.library.database, path='postgres')

        return create_engine(root_dsn, poolclass=NullPool)

    def dispose(self):
        self.pg_engine(self.config.library.database).dispose()
        self.pg_root_engine.dispose()

    @classmethod
    def postgres_db_exists(cls, db_name, conn):
        """ Returns True if database with given name exists in the postgresql. """
        from sqlalchemy.sql.expression import text

        result = conn\
            .execute(
                text('SELECT 1 FROM pg_database WHERE datname=:db_name;'), db_name=db_name)\
            .fetchall()
        return result == [(1,)]

    @classmethod
    def postgres_extension_installed(cls, extension, conn):
        """ Returns True if extension with given name exists in the postgresql. """
        from sqlalchemy.sql.expression import text

        result = conn\
            .execute(
                text('SELECT 1 FROM pg_extension WHERE extname=:extension;'), extension=extension)\
            .fetchall()
        return result == [(1,)]

    def drop_pg(self, database_name):

        with self.pg_root_engine.connect() as conn:
            conn.execute('COMMIT')  # we have to close opened transaction.

            if self.postgres_db_exists(database_name, conn):

                try:
                    conn.execute('DROP DATABASE "{}";'.format(database_name))
                    conn.execute('COMMIT;')
                except Exception as e:
                    logger.warn("Failed to drop database '{}': {}".format(database_name, e))
                    conn.execute('ROLLBACK;')
                    raise
                finally:
                    conn.close()

            else:
                logger.warn('Not dropping {}; does not exist'.format(database_name))

            conn.close()

    def create_pg_template(self, template_name=None):
        """Create the test template database"""
        from ambry.util import select_from_url

        if template_name is None:
            flag_templ = True
            template_name = select_from_url(self.proto_dsn, 'path').strip('/')
        else:
            flag_templ = False

        # Create the database
        with self.pg_root_engine.connect() as conn:

            if self.postgres_db_exists(template_name, conn):
                return

            conn.execute('COMMIT;')  # we have to close opened transaction.

            query = 'CREATE DATABASE "{}" OWNER postgres TEMPLATE template1 encoding \'UTF8\';' \
                .format(template_name)
            conn.execute(query)
            if flag_templ:
                conn.execute("UPDATE pg_database SET datistemplate = TRUE WHERE datname = '{}';"
                             .format(template_name))
            conn.execute('COMMIT;')

            conn.close()

        # Create the extensions, if they aren't already installed
        with self.pg_engine(self.proto_dsn).connect() as conn:
            conn.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm;')
            # Prevents error:   operator class "gist_trgm_ops" does not exist for access method "gist"
            conn.execute('alter extension pg_trgm set schema pg_catalog;')
            conn.execute('CREATE EXTENSION IF NOT EXISTS multicorn;')
            conn.execute('COMMIT;')

            conn.close()

    def create_pg(self, re_create=False, template_name=None):
        from ambry.util import select_from_url

        database_name = select_from_url(self.config.library.database, 'path').strip('/')

        if template_name is None:
            template_name = select_from_url(self.proto_dsn, 'path').strip('/')
            load_extensions = False  # They are already in template
        else:
            load_extensions = True

        username = select_from_url(self.config.library.database, 'username')

        if re_create:
            self.drop_pg(database_name)

        with self.pg_root_engine.connect() as conn:

            conn.execute('COMMIT;')  # we have to close opened transaction.

            query = 'CREATE DATABASE "{}" OWNER "{}" TEMPLATE "{}" encoding \'UTF8\';' \
                .format(database_name, username, template_name)

            conn.execute(query)

            conn.close()

        # Create the extensions, if they aren't already installed
        if load_extensions:
            with self.pg_engine(self.config.library.database).connect() as conn:
                conn.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm;')
                # Prevents error:   operator class "gist_trgm_ops" does not exist for access method "gist"
                conn.execute('alter extension pg_trgm set schema pg_catalog;')
                conn.execute('CREATE EXTENSION IF NOT EXISTS multicorn;')
                conn.execute('COMMIT;')

                conn.close()


class TestBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._proto = ProtoLibrary()
        cls.config = cls._proto.config
        cls._db_type = cls._proto._db_type

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def clean_proto(self):
        self._proto.clean_proto()

    def import_single_bundle(self, cache_path, clean=True):
        from test import bundle_tests
        import fs
        from fs.opener import fsopendir

        l = self.library(use_proto=False)

        if clean:
            l.clean()
            l.create()

        orig_source = os.path.join(os.path.dirname(bundle_tests.__file__), cache_path)
        l.import_bundles(orig_source, detach=True, force=True)

        b = next(b for b in l.bundles).cast_to_subclass()
        b.clean()
        b.sync_in(force=True)

        if os.path.exists(os.path.join(orig_source, 'data')):
            source = fsopendir(os.path.join(orig_source, 'data'))
            b.source_fs.makedir('data',allow_recreate=True)

            dest = b.source_fs.opendir('data')

            for d, files in source.walk('/'):
                if d.startswith('.'):
                    continue

                dest.makedir(d,recursive=True, allow_recreate=True)

                for f in files:
                    if f.startswith('.'):
                        continue

                    path = d+'/'+f

                    dest.setcontents(path, source.getcontents(path) )



        return b

    def library(self, use_proto=True):
        """Return a new proto library. """
        return self._proto.init_library(use_proto=use_proto)
