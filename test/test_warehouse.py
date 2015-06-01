"""
Created on Jun 30, 2012

@author: eric
"""

import unittest
import os

from bundles.testbundle.bundle import Bundle
from ambry.run import get_runconfig
from ambry.warehouse.manifest import Manifest
from test_base import TestBase
from ambry.util import get_logger, Constant
from ambry.orm import Config
import manifests


class Test(TestBase):
    EXAMPLE = Constant()
    EXAMPLE.CONF_DB_SQLITE = 'sqlite'
    EXAMPLE.CONF_DB_POSTGRES = 'postgres1'

    def setUp(self):
        import bundles.testbundle.bundle
        from ambry.run import RunConfig

        import configs
        from shutil import rmtree

        super(Test, self).setUp()

        self.bundle_dir = os.path.dirname(bundles.testbundle.bundle.__file__)
        self.config_dir = os.path.dirname(configs.__file__)

        self.rc = get_runconfig((os.path.join(self.config_dir, 'test.yaml'),
                                 os.path.join(self.bundle_dir, 'bundle.yaml'),
                                 RunConfig.USER_ACCOUNTS))

        # Delete the whole test tree every run.
        test_folder = self.rc.group('filesystem').root
        if os.path.exists(test_folder):
            rmtree(test_folder)

        self.mf = os.path.join(os.path.dirname(manifests.__file__), 'test.ambry')

        self.bundle = Bundle()
        self.waho = None

    def tearDown(self):
        from ambry.library import clear_libraries
        from ambry.database.relational import close_all_connections

        close_all_connections()

        if self.waho:
            self.waho.database.enable_delete = True
            self.waho.database.delete()
            self.waho.close()

        # new_library() caches the library
        clear_libraries()

    def resolver(self, name):
        if (name == self.bundle.identity.name
           or name == self.bundle.identity.vname):
            return self.bundle
        else:
            return False

    def get_library(self, name='default'):
        """Clear out the database before the test run"""
        from ambry.library import new_library

        # create database
        self.copy_or_build_bundle()

        config = self.rc.library(name)
        l = new_library(config, reset=True)

        return l

    def get_warehouse(self, l, name, delete=True):
        from ambry.warehouse import new_warehouse

        waho = new_warehouse(self.rc.warehouse(name), l)

        if delete:
            waho.database.enable_delete = True
            waho.database.delete()
            waho.create()

        return waho

    def _default_warehouse(self, name=None):
        name = self.EXAMPLE.CONF_DB_SQLITE if not name else name
        l = self.get_library()
        l.put_bundle(self.bundle)

        return self.get_warehouse(l, name)

    def test_manifest(self):
        """
        Load the manifest and convert it to a string to check the round-trip
        """

        m_contents = None

        with open(self.mf) as f:
            m_contents = f.read()
        mf = Manifest(self.mf, get_logger('TL'))

        orig_mf = m_contents.replace('\n', '').strip()
        conv_mf = str(mf).replace('\n', '').strip()

        self.assertEqual(orig_mf, conv_mf)

    def test_extract_table(self):
        """
        Extract data from table to file
        """
        from ambry.dbexceptions import NotFoundError

        test_table = 'geot1'
        test_view = 'test_view'
        test_mview = 'test_mview'

        self.waho = self._default_warehouse()
        mf = Manifest(self.mf, get_logger('TL'))
        self.waho.install_manifest(mf)
        tb = self.waho.tables.next()

        # test installed table
        self.waho.extract_table(tb.vid, 'csv')
        # test view
        self.waho.extract_table(test_view, 'csv')
        # + letter case
        self.waho.extract_table(test_mview, 'CsV')

        self.waho.extract_table(test_table, 'csv')

        self.waho.extract_table(test_table, 'json')

        try:
            import osgeo
        except ImportError:
            pass
        else:
            self.waho.extract_table(test_table, 'shapefile')
            self.waho.extract_table(test_table, 'geojson')
            self.waho.extract_table(test_table, 'kml')

        self.assertRaises(NotFoundError, self.waho.extract_table, 'blabla')

    def test_stream_table(self):
        """
        Stream data from table to file
        """
        from ambry.dbexceptions import NotFoundError

        test_view = 'test_view'
        test_mview = 'test_mview'

        self.waho = self._default_warehouse()
        mf = Manifest(self.mf, get_logger('TL'))
        self.waho.install_manifest(mf)
        tb = self.waho.tables.next()

        # test installed table
        self.waho.stream_table(tb.vid, 'csv')
        # test view
        self.waho.stream_table(test_view, 'csv')
        # + letter case
        self.waho.stream_table(test_mview, 'CsV')

        self.assertRaises(NotFoundError, self.waho.stream_table, 'blabla')

    def test_remove(self):
        """
        Remove partition or bundle
        """
        from sqlalchemy.exc import OperationalError, ProgrammingError
        self.waho = self._default_warehouse()
        mf = Manifest(self.mf, get_logger('TL'))
        self.waho.install_manifest(mf)

        # remove bundle
        self.waho.remove('d000')
        try:
            self.waho.remove('d000')
        except AttributeError:
            pass

        # remove partition
        self.waho.run_sql('drop table piEGPXmDC8002001_geot1')
        self.waho.remove('piEGPXmDC8002001')

        self.waho.remove('piEGPXmDC8001001')
        try:
            self.waho.remove('piEGPXmDC8001001')
        except ProgrammingError:
            pass
        except OperationalError:
            pass

    def test_load_insert(self):
        """
        Load data from one table to another
        """

        self.waho = self._default_warehouse()
        mf = Manifest(self.mf, get_logger('TL'))
        self.waho.install_manifest(mf)

        p = self.bundle.partitions.find(name='source-dataset-subset-variation-tthree')
        self.waho.load_local(p, 'tthree', 'piEGPXmDC8003001_tthree')
        # self.waho.load_insert(p, 'tthree', 'piEGPXmDC8003001_tthree')

    def test_has(self):
        """
        Does warehouse has partition
        """
        self.waho = self._default_warehouse()
        mf = Manifest(self.mf, get_logger('TL'))
        self.waho.install_manifest(mf)

        self.assertTrue(self.waho.has('source-dataset-subset-variation-tthree-0.0.1'))
        self.assertFalse(self.waho.has('test'))

    def test_postgres_dbcreate(self):
        """
        Create postgres test DB
        """
        self.waho = self._default_warehouse(self.EXAMPLE.CONF_DB_POSTGRES)
        # TODO: Add database_config test here

    def test_dbobj_create_from_manifest(self):
        """
        Test creating tables, views, mviews, indexs and executing custom sql
        """
        from sqlalchemy.exc import OperationalError

        test_table = 'geot1'
        test_view = 'test_view'
        test_mview = 'test_mview'
        augmented_table_name = 'piEGPXmDC8002_geot1'

        self.waho = self._default_warehouse()

        mf = Manifest(self.mf, get_logger('TL'))
        self.waho.install_manifest(mf)

        all_tbvw = (t.name for t in self.waho.tables)

        # tables
        self.assertIn(test_table, all_tbvw)
        self.assertEqual(test_table, self.waho.orm_table_by_name(test_table).name)
        self.assertTrue(self.waho.has_table(Config.__tablename__))

        # views
        self.assertIn(test_view, all_tbvw)
        self.assertIn(test_mview, all_tbvw)

        self.assertEqual('view', self.waho.orm_table_by_name(test_view).type)
        self.assertEqual('mview', self.waho.orm_table_by_name(test_mview).type)

        # augmented_table_name test
        self.assertEqual(
            augmented_table_name,
            self.waho.orm_table_by_name(augmented_table_name).name)

        # indexs
        self.assertRaises(
            OperationalError,
            self.waho.run_sql,
            'Create index test_index on files (f_id)')

        # SQL
        self.assertTrue(self.waho.has_table('sql_test'))

    def test_clean(self):
        self.waho = self._default_warehouse()
        self.waho.clean()
        self.assertFalse(self.waho.has_table(Config.__tablename__))

    def test_delete_create_db(self):
        self.waho = self._default_warehouse()
        self.waho.delete()
        self.assertFalse(self.waho.database.exists())
        self.assertFalse(self.waho.wlibrary.database.exists())
        self.waho.create()
        self.assertTrue(self.waho.database.exists())
        self.assertTrue(self.waho.wlibrary.database.exists())

    def test_manifest_parser(self):
        lines = [
            "sangis.org-business-sites-orig-businesses-geo-0.1.1",
            "table from sangis.org-business-sites-orig-businesses-geo-0.1.1",
            "table FROM sangis.org-business-sites-orig-businesses-geo-0.1.1",
            "table1, table2 FROM sangis.org-business-sites-orig-businesses-geo-0.1.1",
            "table1, table2 FROM sangis.org-business-sites-orig-businesses-geo-0.1.1 WHERE foo and bar and bas",
            "table1, table2 , table3,table4 FROM sangis.org-business-sites-orig-businesses-geo-0.1.1 # Wot you got?",
        ]

        for line in lines:
            Manifest.parse_partition_line(line)

    def test_manifest_parts(self):
        from old.ipython.manifest import ManifestMagicsImpl

        mf = Manifest('', get_logger('TL'))
        mmi = ManifestMagicsImpl(mf)

        m_head = """
TITLE:  A Test Manifest, For Testing
UID: b4303f85-7d07-471d-9bcb-6980ea1bbf18
DATABASE: spatialite:///tmp/census-race-ethnicity.db
DIR: /tmp/warehouse
        """

        mmi.manifest('', m_head)

        mmi.extract('foobar AS csv TO /bin/bar/bingo')
        mmi.extract('foobar AS csv TO /bin/bar/bingo')
        mmi.extract('foobar AS csv TO /bin/bar/bingo2')
        mmi.extract('foobar AS csv TO /bin/bar/bingo')

        mmi.partitions('', 'one\ntwo\nthree\nfour')

        mmi.view('foo_view_1', '1234\n5678\n')
        mmi.view('foo_view_2', '1234\n5678\n')

        mmi.mview('foo_mview_1', '1234\n5678\n')
        mmi.mview('foo_mview_2', '1234\n5678\n')

        mmi.view('foo_view_1', '1234\n5678\n')
        mmi.view('foo_view_2', '1234\n5678\n')

        mmi.mview('foo_mview_1', '1234\n5678\n')
        mmi.mview('foo_mview_2', '1234\n5678\n')

    def test_sql_parser(self):
        sql = """
SELECT
    geo.state, -- comment 1
    geo.county, -- comment 2
    geo.tract,
    geo.blkgrp,
    weqwe
    bb.geometry,
    CAST(Area(Transform(geometry,26946)) AS REAL) AS area,
    CAST(b02001001 AS INTEGER) AS total_pop,
FROM d02G003_geofile  AS geo
 JOIN d024004_b02001_estimates AS b02001e ON geo.stusab = b02001e.stusab AND geo.logrecno = b02001e.logrecno
 JOIN blockgroup_boundaries AS bb ON geo.state = bb.state AND geo.county = bb.county AND bb.tract = geo.tract AND bb.blkgrp = geo.blkgrp
WHERE geo.sumlevel = 150 AND geo.state = 6 and geo.county = 73
"""

        import sqlparse
        import sqlparse.sql

        r = sqlparse.parse(sql)

        for t in r[0].tokens:
            if isinstance(t, sqlparse.sql.IdentifierList):
                for i in t.get_identifiers():
                    pass

    def test_meta(self):
        """
        Test meta
        """
        self.waho = self._default_warehouse()
        title = 'my title'
        summary = 'my summary'
        name = 'my name '
        url = 'http://www.example.com'

        self.waho.title = title
        self.waho.summary = summary
        self.waho.name = name
        self.waho.url = url

        self.assertEquals(title, self.waho.title)
        self.assertEquals(summary, self.waho.summary)
        self.assertEquals(name, self.waho.name)
        self.assertEquals(url, self.waho.url)

    def test_return_type(self):
        from ambry.identity import Identity
        from ambry.warehouse import NullLogger

        self.waho = self._default_warehouse()

        self.assertIsNotNone(self.waho.cache)

        self.assertIsInstance(self.waho.dict, dict)
        self.assertIsInstance(
            self.waho.get('ambry-djitnip4ju001-0.0.1~djItnip4ju001'),
            Identity)

        # always pass
        self.assertFalse(self.waho.cache.has('test'))
        self.assertRaises(NotImplementedError,
                          self.waho._ogr_args,
                          'source-dataset-subset-variation-geot1')


        lg = NullLogger()
        lg.progress(0, 100)
        lg.log('test log message')
        lg.info('test info message')
        lg.error('test error message')

    def test_augmented_table_name(self):
        from ambry.partition import PartitionBase

        self.waho = self._default_warehouse()
        mf = Manifest(self.mf, get_logger('TL'))
        self.waho.install_manifest(mf)
        partition = self.waho.partitions.next()
        identity = partition.identity
        # without geo no grain, so add it
        identity.grain = 'test'

        basepart = self.bundle.partitions.all[0]
        self.assertEqual(basepart.d_vid,
                         self.waho._partition_to_dataset_vid(basepart))

        name, alias = self.waho.augmented_table_name(identity, 'geot1')
        self.assertEqual('piEGPXmDC8003001_geot1_test', name)
        self.assertEqual('piEGPXmDC8003_geot1_test', alias)

    def test_to_vid(self):
        from ambry.warehouse import ResolutionError
        self.waho = self._default_warehouse()
        mf = Manifest(self.mf, get_logger('TL'))
        self.waho.install_manifest(mf)
        partition = self.waho.partitions.next()
        basepart = self.bundle.partitions.all[0]

        self.assertEqual(self.waho._to_vid(partition), partition)
        self.assertEqual(self.waho._to_vid(basepart), basepart.vid)
        self.assertEqual(self.waho._to_vid(partition.identity), partition.vid)
        self.assertRaises(ResolutionError, self.waho._to_vid, basepart.d_vid)
        self.assertRaises(ResolutionError, self.waho._to_vid, 'testerror')

    def test_create_index(self):
        from sqlalchemy.exc import OperationalError, ProgrammingError

        self.waho = self._default_warehouse()
        mf = Manifest(self.mf, get_logger('TL'))
        self.waho.install_manifest(mf)
        self.waho.create_index('files', ['f_process'], 'test_files_index')
        self.waho.create_index('files', ['f_process'])

        try:
            self.waho.create_index('files', ['f_process'])
        except ProgrammingError:
            pass
        except OperationalError:
            pass

    def test_create_table(self):
        from sqlalchemy.exc import OperationalError, ProgrammingError

        self.waho = self._default_warehouse()
        mf = Manifest(self.mf, get_logger('TL'))
        self.waho.install_manifest(mf)
        partition = self.waho.partitions.next()
        # test logger write index_exists
        self.waho.create_table(partition, partition.table.name)

    def test_database_config(self):
        from ambry.warehouse import new_warehouse
        from ambry.warehouse.sqlite import SqliteWarehouse
        from ambry.dbexceptions import ConfigurationError

        self.waho = self._default_warehouse()
        l = self.get_library()
        l.put_bundle(self.bundle)
        self.assertIsInstance(
            # plus added for test
            new_warehouse('sqlite+:///' + self.waho.database.path, l),
            SqliteWarehouse)

        self.assertRaises(ConfigurationError, new_warehouse, 'sqlite:test', l)
        self.assertRaises(ValueError, new_warehouse, 'test:test', l)

    def test_warehouse_logger(self):
        import logging
        from ambry.warehouse import Logger
        self.waho = self._default_warehouse()
        lgr = logging.getLogger('test_logger')

        lgr.setLevel(logging.DEBUG)
        ch = logging.FileHandler('/tmp/test_logger.log')
        ch.setLevel(logging.DEBUG)
        lgr.addHandler(ch)

        lg = Logger(lgr, self.waho.logger.lr)
        lg.progress(0, 100, 'some message')
        lg.log('test log message')
        lg.info('test info message')
        lg.error('test error message')
        lg.copy('a', 'b')
        lg.fatal('test error message')
        lg.warn('test error message')

    def test_partitions_list(self):
        self.waho = self._default_warehouse()
        mf = Manifest(self.mf, get_logger('TL'))

        self.waho.install_manifest(mf)
        s = [str(c) for c in self.waho.list()]

        self.assertIn('source-dataset-subset-variation-geot1-0.0.1~piEGPXmDC8002001', s)
        self.assertIn('source-dataset-subset-variation-geot2-0.0.1~piEGPXmDC8001001', s)
        self.assertIn('source-dataset-subset-variation-tthree-0.0.1~piEGPXmDC8003001', s)

        tst = (mfile.path for mfile in self.waho.manifests)

        self.assertIn(mf.path, tst)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())
