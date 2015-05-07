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
from ambry.util import get_logger


class Test(TestBase):

    def setUp(self):
        import bundles.testbundle.bundle
        from ambry.run import RunConfig
        import manifests
        import configs

        self.bundle_dir = os.path.dirname(bundles.testbundle.bundle.__file__)
        self.config_dir = os.path.dirname(configs.__file__)

        self.rc = get_runconfig((os.path.join(self.config_dir, 'test.yaml'),
                                 os.path.join(self.bundle_dir, 'bundle.yaml'),
                                 RunConfig.USER_ACCOUNTS))

        self.copy_or_build_bundle()

        self.bundle = Bundle()

        self.m = os.path.join(os.path.dirname(manifests.__file__), 'test.ambry')

        with open(self.m) as f:
            self.m_contents = f.read()

    def tearDown(self):
        pass

    def resolver(self, name):
        if name == self.bundle.identity.name or name == self.bundle.identity.vname:
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

        w = new_warehouse(self.rc.warehouse(name), l)

        if delete:
            w.database.enable_delete = True
            w.database.delete()
            w.create()

        return w

    def _test_manifest_install(self, name):
        l = self.get_library()
        l.put_bundle(self.bundle)
        w = self.get_warehouse(l, name)

        w.title = "This is the Warehouse!"
        w.about = "A Warehouse full of wonder"

        m = Manifest(self.m, get_logger('TL'))

        w.install_manifest(m)

    def test_sqlite_install(self):
        """
        Install manifest with sqlite
        """
        self._test_manifest_install('sqlite')

    def test_postgres_install(self):
        """
        Install manifest with postgres
        """
        self._test_manifest_install('postgres1')

    def test_manifest(self):
        """
        Load the manifest and convert it to a string to check the round-trip
        """

        m = Manifest(self.m, get_logger('TL'))

        orig_mf = self.m_contents.replace('\n', '').strip()
        conv_mf = str(m).replace('\n', '').strip()

        self.assertEqual(orig_mf, conv_mf)

        # w.extracts

    def test_extract(self):
        l = self.get_library()
        l.put_bundle(self.bundle)
        w = self.get_warehouse(l, 'sqlite', delete=False)
        # cache = new_cache('s3://warehouse.sandiegodata.org/test', run_config = get_runconfig())

        w.extract_all(force=True)

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

        m = Manifest('', get_logger('TL'))
        mmi = ManifestMagicsImpl(m)

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

    def x_test_install(self):

        def resolver(name):
            if name == self.bundle.identity.name or name == self.bundle.identity.vname:
                return self.bundle
            else:
                return False

        def progress_cb(lr, type, name, n):
            if n:
                lr("{} {}: {}".format(type, name, n))
            else:
                self.bundle.log("{} {}".format(type, name))

        from ambry.warehouse import new_warehouse
        from functools import partial

        w = new_warehouse(self.rc.warehouse('postgres'))

        w.database.enable_delete = True
        w.resolver = resolver
        w.progress_cb = progress_cb

        try:
            w.drop()
        except:
            pass

        w.create()

        # self.bundle.partitions.all

        for p in self.bundle.partitions:
            lr = self.bundle.init_log_rate(10000)
            w.install(p, progress_cb=partial(progress_cb, lr))

        self.assertTrue(w.has(self.bundle.identity.vname))

        for p in self.bundle.partitions:
            self.assertTrue(w.has(p.identity.vname))

        for p in self.bundle.partitions:
            w.remove(p.identity.vname)

        w.get(self.bundle.identity.name)
        w.get(self.bundle.identity.vname)
        w.get(self.bundle.identity.id_)

        w.install(self.bundle)

        w.get(self.bundle.identity.name)
        w.get(self.bundle.identity.vname)
        w.get(self.bundle.identity.id_)

        for p in self.bundle.partitions:
            lr = self.bundle.init_log_rate(10000)
            w.install(p, progress_cb=partial(progress_cb, lr))


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())
