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


class Test(TestBase):
    EXAMPLE = Constant()
    EXAMPLE.TABLE_NAME = 'geot1'
    EXAMPLE.CONF_DB_SQLITE = 'sqlite'
    EXAMPLE.CONF_DB_POSTGRES = 'postgres1'

    def setUp(self):
        import bundles.testbundle.bundle
        from ambry.run import RunConfig
        import manifests
        import configs
        from shutil import rmtree

        self.bundle_dir = os.path.dirname(bundles.testbundle.bundle.__file__)
        self.config_dir = os.path.dirname(configs.__file__)

        self.rc = get_runconfig((os.path.join(self.config_dir, 'test.yaml'),
                                 os.path.join(self.bundle_dir, 'bundle.yaml'),
                                 RunConfig.USER_ACCOUNTS))

        self.copy_or_build_bundle()

        self.bundle = Bundle()

        self.mf = os.path.join(os.path.dirname(manifests.__file__),'test.ambry')

        # Delete the whole test tree every run.
        if os.path.exists(self.rc.group('filesystem').root):
            rmtree(self.rc.group('filesystem').root)

        with open(self.mf) as f:
            self.m_contents = f.read()

    def tearDown(self):
        pass

    def resolver(self, name):
        if (name == self.bundle.identity.name
           or name == self.bundle.identity.vname):
            return self.bundle
        else:
            return False

    def get_library(self, name='default'):
        """Clear out the database before the test run"""
        from ambry.library import new_library

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

    def _test_manifest_install(self, name):
        """
        Install test manifest and check the database for the table
        """
        test_table = 'geot1'
        test_table_name = 'config'

        waho = self._default_warehouse(name)
        mf = Manifest(self.mf, get_logger('TL'))

        waho.install_manifest(mf)
        tst = (mfile.path for mfile in waho.manifests)

        # because of problems with the session!!! FIXME
        # doing some tests here
        self.assertIn(mf.path, tst)
        self.assertIn(test_table, (t.name for t in waho.tables))
        self.assertEqual(test_table, waho.orm_table_by_name(test_table).name)
        self.assertTrue(waho.has_table(test_table_name))

        # TODO: test here
        # install_material_view
        # install_view
        # install_table
        # install_partition
        # augmented_table_name
        # bundles
        # close

        waho.clean()
        self.assertFalse(waho.has_table(test_table_name))

        waho.delete()
        self.assertFalse(waho.database.exists())
        self.assertFalse(waho.wlibrary.database.exists())
        waho.create()
        self.assertTrue(waho.database.exists())
        self.assertTrue(waho.wlibrary.database.exists())

    def test_sqlite_install(self):
        """
        Install manifest with sqlite
        """
        self._test_manifest_install(self.EXAMPLE.CONF_DB_SQLITE)

    def test_postgres_install(self):
        """
        Install manifest with postgres
        """

        # FIXME: Need to figure out how to do this in a flexible way that can run on
        # Travis-CI
        return

        self._test_manifest_install(self.EXAMPLE.CONF_DB_POSTGRES)

    def test_manifest(self):
        """
        Load the manifest and convert it to a string to check the round-trip
        """

        mf = Manifest(self.mf, get_logger('TL'))

        orig_mf = self.m_contents.replace('\n', '').strip()
        conv_mf = str(mf).replace('\n', '').strip()

        self.assertEqual(orig_mf, conv_mf)

    def test_extract(self):
        l = self.get_library()
        l.put_bundle(self.bundle)
        waho = self.get_warehouse(l, self.EXAMPLE.CONF_DB_SQLITE, delete=False)
        # cache = new_cache('s3://warehouse.sandiegodata.org/test', run_config = get_runconfig())
        waho.extract_all(force=True)

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


    def test_exists(self):
        """
        Test the existence of the database
        """
        waho = self._default_warehouse()
        self.assertTrue(waho.exists())

    def test_meta(self):
        """
        Test meta
        """
        waho = self._default_warehouse()
        title = 'my title'
        summary = 'my summary'
        name = 'my name '
        url = 'http://www.example.com'

        waho.title = title
        waho.summary = summary
        waho.name = name
        waho.url = url

        self.assertEquals(title, waho.title)
        self.assertEquals(summary, waho.summary)
        self.assertEquals(name, waho.name)
        self.assertEquals(url, waho.url)

    def test_cache(self):
        waho = self._default_warehouse()
        self.assertIsNotNone(waho.cache)

    def test_dict(self):
        waho = self._default_warehouse()
        self.assertIsInstance(waho.dict, dict)

    def test_load_local(self):
        pass

    def test_load_insert(self):
        pass

    def test_remove(self):
        # delete all from wlibrary
        pass

    def test_run_sql(self):
        # + create_table
        pass

    def test_get(self):
        pass

    def test_has(self):
        pass

    def test_list(self):
        pass

    def test_info(self):
        pass


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())
