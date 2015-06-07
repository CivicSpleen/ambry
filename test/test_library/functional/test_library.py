"""
Created on Jun 30, 2012

@author: eric
"""
import json
import logging
import os.path
import shutil
import unittest
import yaml

import ckcache.filesystem
from ckcache.filesystem import FsCache, FsCompressionCache
from ckcache import new_cache

from test.test_base import TestBase  # Must be first ambry import to get logger set to internal logger.

from ambry.bundle import LibraryDbBundle
from ambry.dbexceptions import ConfigurationError
from ambry.identity import Identity
from ambry.library import new_library
from ambry.library.database import LibraryDb, ROOT_CONFIG_NAME_V
from ambry.library.query import Resolver
from ambry.orm import Dataset, Partition, Table, Column, ColumnStat, Code, Config, File
from ambry.run import get_runconfig, RunConfig
from ambry import util

from test.bundles.testbundle.bundle import Bundle

global_logger = util.get_logger(__name__)
global_logger.setLevel(logging.FATAL)

ckcache.filesystem.global_logger = global_logger


class Test(TestBase):
    def setUp(self):

        super(Test, self).setUp()

        import test.bundles.testbundle.bundle

        self.bundle_dir = os.path.dirname(test.bundles.testbundle.bundle.__file__)
        self.rc = get_runconfig((os.path.join(self.bundle_dir, 'library-test-config.yaml'),
                                 os.path.join(self.bundle_dir, 'bundle.yaml'),
                                 RunConfig.USER_ACCOUNTS))

        self.copy_or_build_bundle()

        self.bundle = Bundle()

        Test.rm_rf(self.rc.group('filesystem').root)

    @staticmethod
    def rm_rf(d):

        if not os.path.exists(d):
            return

        for path in (os.path.join(d, f) for f in os.listdir(d)):
            if os.path.isdir(path):
                Test.rm_rf(path)
            else:
                os.unlink(path)
        os.rmdir(d)

    def get_library(self, name='default'):
        """Clear out the database before the test run"""
        config = self.rc.library(name)
        return new_library(config, reset=True)

    def tearDown(self):
        pass

    @staticmethod
    def new_db():
        db_file = util.temp_file_name() + '.db'
        db = LibraryDb(driver='sqlite', dbname=db_file)
        return db_file, db

    def test_database(self):

        f, db = self.new_db()

        #
        # Test basic creation
        #

        self.assertFalse(db.exists())

        db.create()

        self.assertTrue(db.exists())

        db.set_config_value('test', 'one', 1)
        db.set_config_value('test', 'two', 2)

        self.assertEquals(1, db.get_config_value('test', 'one').value)
        self.assertEquals(2, db.get_config_value('test', 'two').value)

        self.assertIn(('test', 'one'), db.config_values)
        self.assertIn(('test', 'two'), db.config_values)
        self.assertEquals(2, db.config_values[('test', 'two')])

        self.assertEquals(0, len(db.list()))

        db.drop()

        self.assertTrue(os.path.exists(f))
        self.assertFalse(db.exists())

        os.remove(f)

    def test_database_query(self):

        f, db = self.new_db()

        db.create()

        db.install_bundle(self.bundle)

        #
        # Get a bunch of names from the existing bundles. This will check the simple
        # queries for single objects.
        #

        tests = {}
        for r in db.session.query(Dataset, Partition).filter(Dataset.vid != ROOT_CONFIG_NAME_V).all():
            di = r.Dataset.identity

            tests[di.sname] = di.vid
            tests[di.vname] = di.vid
            tests[di.fqname] = di.vid
            tests[di.vid] = di.vid

            pi = r.Partition.identity

            tests[pi.sname] = pi.vid
            tests[pi.vname] = pi.vid
            tests[pi.fqname] = pi.vid
            tests[pi.vid] = pi.vid

        r = Resolver(db.session)

        for ref, vid in tests.items():
            ip, results = r.resolve_ref_all(ref)

            self.assertEquals(1, len(results))

            first = results.values().pop(0)
            vid2 = first.vid if not first.partitions else first.partitions.values()[0].vid

            self.assertEquals(vid, vid2)

    def test_simple_install(self):

        l = self.get_library()
        ldsq = l.database.session.query

        bdsq = self.bundle.database.session.query

        self.assertEquals(2, len(bdsq(Partition).all()))

        r = l.put_bundle(self.bundle, install_partitions=True)

        r = l.get(self.bundle.identity.sname)
        self.assertTrue(r is not False)
        self.assertEquals(self.bundle.identity.sname, r.identity.sname)

        self.assertEquals(2, len(ldsq(Partition).all()))
        self.assertEquals(9, len(ldsq(Table).all()))
        self.assertEquals(45, len(ldsq(Column).all()))
        self.assertEquals(20, len(ldsq(Code).all()))
        self.assertEquals(14, len(ldsq(ColumnStat).all()))
        self.assertEquals(43, len(ldsq(Config).all()))

        self.assertEquals(3, len(ldsq(File).all()))

        installed = False
        for p in self.bundle.partitions.all:
            l.get(p.identity.vid)
            installed = True
        self.assertTrue(installed)

    def test_library_install(self):
        '''Install the bundle and partitions, and check that they are
        correctly installed. Check that installation is idempotent'''

        l = self.get_library()
        l.clean()

        l.put_bundle(self.bundle)

        ldsq = l.database.session.query
        self.assertEquals(2, len(ldsq(Partition).all()))
        self.assertEquals(9, len(ldsq(Table).all()))
        self.assertEquals(45, len(ldsq(Column).all()))
        self.assertEquals(20, len(ldsq(Code).all()))
        self.assertEquals(14, len(ldsq(ColumnStat).all()))

        l.put_bundle(self.bundle)

        l.put_bundle(self.bundle)

        self.assertEquals(2, len(ldsq(Partition).all()))
        self.assertEquals(9, len(ldsq(Table).all()))
        self.assertEquals(45, len(ldsq(Column).all()))
        self.assertEquals(20, len(ldsq(Code).all()))
        self.assertEquals(14, len(ldsq(ColumnStat).all()))

        r = l.get(self.bundle.identity)

        self.assertIsNotNone(r)
        self.assertTrue(r is not False)
        self.assertEquals(r.identity.id_, r.identity.id_)

        num_tables = 9
        self.assertEquals(num_tables, len(l.database.session.query(Table).all()))

        b = l.get(self.bundle.identity.vid)
        self.assertEquals(num_tables, len(b.schema.tables))

        l.remove(b)

        self.assertEquals(0, len(ldsq(Partition).all()))
        self.assertEquals(0, len(ldsq(Table).all()))
        self.assertEquals(0, len(ldsq(Column).all()))
        self.assertEquals(0, len(ldsq(Code).all()))
        self.assertEquals(0, len(ldsq(ColumnStat).all()))

        # Re-install the bundle, then check that the partitions are still properly installed

        l.put_bundle(self.bundle)

        for partition in self.bundle.partitions.all:
            r = l.get(partition.identity.vid)
            self.assertIsNotNone(r)
            self.assertEquals(r.partition.identity.id_, partition.identity.id_)

            r = l.get(partition.identity.vid)
            self.assertIsNotNone(r)
            self.assertEquals(r.partition.identity.id_, partition.identity.id_)

    def test_library_push(self):
        """Install the bundle and partitions, and check that they are
        correctly installed. Check that installation is idempotent"""

        l = self.get_library('local-remoted')

        l.put_bundle(self.bundle, install_partitions=True, file_state='new')

        r = l.get(self.bundle.identity)

        self.assertIsNotNone(r)
        self.assertTrue(r is not False)
        self.assertEquals(r.identity.id_, r.identity.id_)

        self.assertEquals(3, len(l.files.query.state('new').all))

        for remote_name, remote in l.remotes.items():
            remote.clean()

        a = l.remotes.values()
        b = l.files.query.state('new').all

        def cb(what, metadata, start):
            pass  # print "PUSH ", what, metadata['name'], start

        # The zippy bit rotates the files through the three caches.
        rf = [(remote, file_.ref) for remote, file_ in zip(a*(len(b)/len(a)+1), b)]
        for remote, ref in rf:

            l.push(remote, ref, cb=cb)

        #  NOTE! This is a really crappy test, and it will fail if gdal is not installed, since the
        #  geot1.geodb database will be geot1.db

        try:  # Can't create geodbs if don't have gdal installed
            import gdal

            out_string = """source/dataset-subset-variation-0.0.1.db:
  caches: [/tmp/library-test/remote-cache-2]
source/dataset-subset-variation-0.0.1/geot1.geodb:
  caches: [/tmp/library-test/remote-cache-3]
source/dataset-subset-variation-0.0.1/geot2.geodb:
  caches: [/tmp/library-test/remote-cache-1]
source/dataset-subset-variation-0.0.1/tone/missing.db:
  caches: [/tmp/library-test/remote-cache-1]
source/dataset-subset-variation-0.0.1/tthree.db:
  caches: [/tmp/library-test/remote-cache-2]
"""
        except ImportError:

            out_string = """source/dataset-subset-variation-0.0.1.db:
  caches: [/tmp/library-test/remote-cache-2]
source/dataset-subset-variation-0.0.1/tone/missing.db:
  caches: [/tmp/library-test/remote-cache-3]
source/dataset-subset-variation-0.0.1/tthree.db:
  caches: [/tmp/library-test/remote-cache-1]
"""

        self.assertEquals(
            out_string,
            yaml.safe_dump(l.remote_stack.list(include_partitions=True)))

        l.purge()

        fn = l.remote_stack.get(self.bundle.identity.cache_key)

        self.assertTrue(bool(fn))
        self.assertEquals('/tmp/library-test/remote-cache-2/source/dataset-subset-variation-0.0.1.db', fn)

        c = l.cache

        self.assertNotIn('source/dataset-subset-variation-0.0.1.db', c.list())
        c.attach(l.remote_stack)
        self.assertIn('source/dataset-subset-variation-0.0.1.db', c.list())
        c.detach()
        self.assertNotIn('source/dataset-subset-variation-0.0.1.db', c.list())

        l.sync_remotes()

        b = l.get(self.bundle.identity.vid)

        self.assertIsNotNone(b)

        self.assertEquals('source-dataset-subset-variation-0.0.1', str(b.identity.vname))

        vnames = [
            'source-dataset-subset-variation-geot2-0.0.1',
            'source-dataset-subset-variation-geot1-0.0.1',
            'source-dataset-subset-variation-tthree-0.0.1',
            'source-dataset-subset-variation-tone-missing-0.0.1'
        ]
        for p in b.partitions:
            bp = l.get(p.identity.vid, remote=l.remote_stack)
            self.assertIn(bp.partition.identity.vname, vnames)

    def test_s3_push(self):
        """Install the bundle and partitions, and check that they are
        correctly installed. Check that installation is idempotent"""

        try:
            l = self.get_library('s3-remoted')
        except ConfigurationError:
            raise unittest.SkipTest('devtest.sandiegodata.org bucket is not configured')

        remote = l.remotes['0']

        l.purge()
        l.put_bundle(self.bundle)

        def cb(what, metadata, start):
            pass  # print "PUSH ", what, metadata['name'], start

        l.push(remote, cb=cb)

        l.purge()

        l.sync_remotes()

        b = l.get(self.bundle.identity.vid)

        self.assertTrue(bool(b))

        self.assertEquals(self.bundle.identity.vname, b.identity.vname)

        for p in b.partitions:
            bp = l.get(p.identity.vid)

            self.assertEquals(self.bundle.identity.vname, bp.identity.vname)
            self.assertEquals(p.identity.vname, bp.partition.vname)

            l.files.query.ref(bp.partition.identity.vid).one

        dataset = l.resolve(p.identity.vid)

        self.assertEquals('piEGPXmDC8005001', dataset.partition.vid)

        l.remotes.values()[0].store_list()

    def test_http_cache(self):

        l = self.get_library('http-remoted')

        r = l.remotes.values()[0]

        self.assertFalse(r.has('foobar'))
        self.assertTrue(r.has(self.bundle.identity.cache_key))

        l.purge()

        l.sync_remotes()

        b = l.get(self.bundle.identity.cache_key)

        self.assertTrue(bool(b))

        self.assertEquals(self.bundle.identity.vname, b.identity.vname)

        for p in b.partitions:
            bp = l.get(p.identity.vid)

            self.assertEquals(self.bundle.identity.vname, bp.identity.vname)
            self.assertEquals(p.identity.vname, bp.partition.vname)

    def test_versions(self):

        idnt = self.bundle.identity

        l = self.get_library()

        orig = os.path.join(self.bundle.bundle_dir, 'bundle.yaml')
        save = os.path.join(self.bundle.bundle_dir, 'bundle.yaml.save')
        shutil.copyfile(orig, save)

        datasets = {}

        try:
            for i in [1, 2, 3]:
                idnt._on.revision = i
                idnt.name.version_major = i
                idnt.name.version_minor = i * 10

                bundle = Bundle()
                get_runconfig.clear()  # clear runconfig cache

                bundle.metadata.load_all()

                bundle.metadata.identity = idnt.ident_dict
                bundle.metadata.names = idnt.names_dict

                bundle.metadata.write_to_dir(write_all=True)

                bundle = Bundle()

                bundle.clean()
                bundle.pre_prepare()
                bundle.prepare()
                bundle.post_prepare()
                bundle.pre_build()

                bundle.build_small()
                # bundle.build()
                bundle.post_build()

                bundle = Bundle()

                l.put_bundle(bundle)

        finally:
            os.rename(save, orig)

        #
        # Save the list of datasets for version analysis in other
        # tests
        #

        db = l.database

        for d in db.list(with_partitions=True).values():
            datasets[d.vid] = d.dict
            datasets[d.vid]['partitions'] = {}

            for p_vid, p in d.partitions.items():
                datasets[d.vid]['partitions'][p_vid] = p.dict

        with open(self.bundle.filesystem.path('meta', 'version_datasets.json'), 'w') as f:
            f.write(json.dumps(datasets))

        r = Resolver(db.session)

        # ref = idnt.id_

        ref = 'source-dataset-subset-variation-=2.20'

        ip, results = r.resolve_ref_all(ref)
        # FIXME: where is test?

    def test_version_resolver(self):

        l = self.get_library()

        db = l.database
        db.enable_delete = True
        db.drop()
        db.create()

        l.put_bundle(self.bundle)

        r = Resolver(db.session)

        vname = 'source-dataset-subset-variation-0.0.1'
        name = 'source-dataset-subset-variation'

        ip, results = r.resolve_ref_one(vname)
        self.assertEquals(vname, results.vname)

        ip, results = r.resolve_ref_one(name)
        self.assertEquals(vname, results.vname)

        # Cache keys

        ip, result = r.resolve_ref_one('source/dataset-subset-variation-0.0.1.db')
        self.assertEquals('source-dataset-subset-variation-0.0.1~diEGPXmDC8001', str(result))

        ip, result = r.resolve_ref_one('source/dataset-subset-variation-0.0.1/tthree.db')
        self.assertEquals('source-dataset-subset-variation-tthree-0.0.1~piEGPXmDC8001001',
            str(result.partition))

        # Now in the library, which has a slightly different interface.
        ident = l.resolve(vname)
        self.assertEquals(vname, ident.vname)

        ident = l.resolve('source-dataset-subset-variation-0.0.1~diEGPXmDC8001')
        self.assertEquals('diEGPXmDC8001', ident.vid)

        ident = l.resolve('source-dataset-subset-variation-tthree-0.0.1~piEGPXmDC8001001')
        self.assertEquals('diEGPXmDC8001', ident.vid)
        self.assertEquals('piEGPXmDC8001001', ident.partition.vid)

        #
        # Test semantic version matching
        # WARNING! The Mock object below only works for testing semantic versions.
        #

        with open(self.bundle.filesystem.path('meta', 'version_datasets.json')) as f:
            datasets = json.loads(f.read())

        # This mock object only works on datasets; it will return all of the
        # partitions for each dataset, and each of the datasets. It is only for testing
        # version filtering.
        class TestResolver(Resolver):
            def _resolve_ref(self, ref, location=None):
                ip = Identity.classify(ref)
                return ip, {k: Identity.from_dict(ds) for k, ds in datasets.items()}

        r = TestResolver(db.session)

        ip, result = r.resolve_ref_one('source-dataset-subset-variation-==1.10.1')
        self.assertEquals('source-dataset-subset-variation-1.10.1~diEGPXmDC8001', str(result))

        ip, result = r.resolve_ref_one('source-dataset-subset-variation->=1.10.1,<3.0.0')
        self.assertEquals('source-dataset-subset-variation-2.20.2~diEGPXmDC8002', str(result))

        ip, result = r.resolve_ref_one('source-dataset-subset-variation->=1.10.1,<2.0.0')
        self.assertEquals('source-dataset-subset-variation-1.10.1~diEGPXmDC8001', str(result))

        ip, result = r.resolve_ref_one('source-dataset-subset-variation->2.0.0')
        self.assertEquals('source-dataset-subset-variation-3.30.3~diEGPXmDC8003', str(result))

        ip, result = r.resolve_ref_one('source-dataset-subset-variation-<=3.0.0')
        self.assertEquals('source-dataset-subset-variation-2.20.2~diEGPXmDC8002', str(result))

    def test_compression_cache(self):
        """Test a two-level cache where the upstream compresses files """

        root = self.rc.group('filesystem').root

        l1_repo_dir = os.path.join(root, 'comp-repo-l1')
        os.makedirs(l1_repo_dir)
        l2_repo_dir = os.path.join(root, 'comp-repo-l2')
        os.makedirs(l2_repo_dir)

        testfile = self.new_rand_file(os.path.join(root, 'testfile'))

        # Create a cache with an upstream wrapped in compression
        l3 = FsCache(l2_repo_dir)
        l2 = FsCompressionCache(l3)
        l1 = FsCache(l1_repo_dir, upstream=l2)

        f1 = l1.put(testfile, 'tf1')

        self.assertTrue(os.path.exists(f1))

        l1.remove('tf1', propagate=False)

        self.assertFalse(os.path.exists(f1))

        f1 = l1.get('tf1')

        self.assertIsNotNone(f1)

        self.assertTrue(os.path.exists(f1))

    def test_partitions(self):

        l = self.get_library()

        l.purge()

        l.put_bundle(self.bundle)  # Install the partition references in the library.

        l.get(self.bundle.identity)

        for partition in self.bundle.partitions:

            l.put_partition(partition)
            l.put_partition(partition)

            r = l.get(partition.identity)
            self.assertIsNotNone(r)
            self.assertEquals(partition.identity.id_, r.partition.identity.id_)

            r = l.get(partition.identity.id_)
            self.assertIsNotNone(r)
            self.assertEquals(partition.identity.id_, r.partition.identity.id_)

        #
        # Create all possible combinations of partition names
        #
        self.bundle.schema.tables[0]
        # table = self.bundle.schema.tables[0]

    def test_s3(self):
        try:
            cache = new_cache(self.rc.filesystem('s3'))
        except ConfigurationError:
            raise unittest.SkipTest('devtest.sandiegodata.org bucket is not configured')

        repo_dir = cache.cache_dir

        # Set up the test directory and make some test files.

        root = self.rc.group('filesystem').root
        os.makedirs(root)

        testfile = os.path.join(root, 'testfile')

        with open(testfile, 'w+') as f:
            for i in range(1024):
                f.write('.' * 1023)
                f.write('\n')

        for i in range(0, 10):
            global_logger.info('Putting ' + str(i))
            cache.put(testfile, 'many' + str(i))

        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many1')))
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many2')))
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many3')))

        p = cache.get('many1')
        self.assertTrue(p is not None)

        self.assertTrue(os.path.exists(os.path.join(repo_dir, 'many1')))
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many2')))
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many3')))

        p = cache.get('many2')
        self.assertTrue(p is not None)

        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many3')))
        self.assertTrue(os.path.exists(os.path.join(repo_dir, 'many7')))

        p = cache.get('many3')
        self.assertTrue(p is not None)

        self.assertTrue(os.path.exists(os.path.join(repo_dir, 'many3')))
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many7')))

    def test_files(self):

        lib = self.get_library()

        lib.purge()

        for e in [(str(i), str(j)) for i in range(10) for j in range(3)]:
            lib.files.new_file(
                path='path' + e[0], ref='{}-{}'.format(*e),
                source_url='foo', group=e[1], type_=e[1])

        self.assertEquals(30, len(lib.files.query.all))

        # Will throw an exception on duplicate error
        lib.files.new_file(path='ref-a', type='type-a', source='source-a', state='a')
        self.assertEquals('a', lib.files.query.path('ref-a').one.state)

        # Test that it overwrites inistead of duplicates
        lib.files.new_file(path='ref-a', type='type-a', source='source-a', state='b')
        self.assertEquals('b', lib.files.query.path('ref-a').one.state)
        self.assertEquals(31, len(lib.files.query.all))
