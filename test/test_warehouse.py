"""
Created on Jun 30, 2012

@author: eric
"""

import unittest
import os.path

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
        if os.path.exists(self.rc.group('filesystem').root):
            rmtree(self.rc.group('filesystem').root)

        self.mf = os.path.join(os.path.dirname(manifests.__file__), 'test.ambry')

        self.bundle = Bundle()
        self.waho = None

    def tearDown(self):
        from ambry.library import clear_libraries

        if self.waho:
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

    def test_manifest_install(self):
        """
        Install test manifest and check the database for the table
        """
        self.waho = self._default_warehouse(self.EXAMPLE.CONF_DB_SQLITE)
        mf = Manifest(self.mf, get_logger('TL'))

        self.waho.install_manifest(mf)
        tst = (mfile.path for mfile in self.waho.manifests)

        self.assertIn(mf.path, tst)


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

    def test_sqlite_dbcreate(self):
        """
        Create sqlite test DB
        """
        self.waho = self._default_warehouse()
        self.assertTrue(self.waho.exists())

    def test_postgres_dbcreate(self):
        """
        Create postgres test DB
        """

        # FIXME: Neet to figure out how to do this in a flexible way that can run on
        # Travis-CI
        return

        self._test_manifest_install(self.EXAMPLE.CONF_DB_POSTGRES)

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

    def test_cache(self):
        self.waho = self._default_warehouse()
        self.assertIsNotNone(self.waho.cache)

    def test_dict(self):
        self.waho = self._default_warehouse()
        self.assertIsInstance(self.waho.dict, dict)

    def test_get(self):
        from ambry.identity import Identity

        self.waho = self._default_warehouse()
        self.assertIsInstance(
            self.waho.get('ambry-djitnip4ju001-0.0.1~djItnip4ju001'),
            Identity)

    def test_has(self):
        self.waho = self._default_warehouse()
        # FIXME: no idea how to test
        self.assertTrue(self.waho.has(self.waho.bundle.identity))

    def test_partitions_list(self):
        self.waho = self._default_warehouse()
        mf = Manifest(self.mf, get_logger('TL'))

        self.waho.install_manifest(mf)
        s = [str(c) for c in self.waho.list()]

        self.assertIn('source-dataset-subset-variation-geot1-0.0.1~piEGPXmDC8002001', s)
        self.assertIn('source-dataset-subset-variation-geot2-0.0.1~piEGPXmDC8001001', s)
        self.assertIn('source-dataset-subset-variation-tthree-0.0.1~piEGPXmDC8003001', s)

    def test_info(self):
        # FIXME: for details look in warehouse.info()
        return
        self.waho = self._default_warehouse()
        self.waho.info()

    # def test_extract(self):
    #     l = self.get_library()
    #     l.put_bundle(self.bundle)
    #     self.waho = self.get_warehouse(l, self.EXAMPLE.CONF_DB_SQLITE, delete=False)
    #     # cache = new_cache('s3://warehouse.sandiegodata.org/test', run_config = get_runconfig())
    #     self.waho.extract_all(force=True)

    def test_load_local(self):
        pass

    def test_load_insert(self):
        pass

    def test_remove(self):
        pass


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())
