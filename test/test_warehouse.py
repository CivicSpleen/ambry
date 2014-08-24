"""
Created on Jun 30, 2012

@author: eric
"""

import unittest
import os.path
from  bundles.testbundle.bundle import Bundle
from ambry.run import  get_runconfig
import logging
import ambry.util
from ambry.warehouse.manifest import Manifest

from test_base import  TestBase

global_logger = ambry.util.get_logger(__name__)
global_logger.setLevel(logging.DEBUG)


class TestLogger(object):
    def __init__(self, lr):
        self.lr = lr

    def progress(self, type_, name, n, message=None):
        self.lr("{} {}: {}".format(type_, name, n))

    def info(self, message):
        print("{}".format(message))

    def log(self, message):
        print("{}".format(message))

    def error(self, message):
        print("ERROR: {}".format(message))

    def warn(self, message):
        print("Warn: {}".format(message))


class Test(TestBase):
 
    def setUp(self):
        import bundles.testbundle.bundle
        from ambry.run import RunConfig

        self.bundle_dir = os.path.dirname( bundles.testbundle.bundle.__file__)
        self.rc = get_runconfig((os.path.join(self.bundle_dir,'warehouse-test-config.yaml'),
                                 os.path.join(self.bundle_dir,'bundle.yaml'),
                                 RunConfig.USER_ACCOUNTS))

        self.copy_or_build_bundle()

        self.bundle = Bundle()    

        print "Deleting: {}".format(self.rc.group('filesystem').root_dir)
        ambry.util.rm_rf(self.rc.group('filesystem').root_dir)

    m = """
TITLE: A Test Manifest, For Testing
UID: b4303f85-7d07-471d-9bcb-6980ea1bbf18
ACCESS: public

DOC: None
This is the test documentation for a file *that is* just for testing.

DESTINATION: spatialite:///tmp/census-race-ethnicity.db

DOC: None
Here is more documentation about the directory:

DIR: /tmp/warehouse

DOC: None
We've got a while lot of partitions.

PARTITIONS:
sangis.org-business-sites-orig-businesses-geo-0.1.1
table from sangis.org-business-sites-orig-businesses-geo-0.1.1
table FROM sangis.org-business-sites-orig-businesses-geo-0.1.1
table1, table2 FROM sangis.org-business-sites-orig-businesses-geo-0.1.1
table1, table2 FROM sangis.org-business-sites-orig-businesses-geo-0.1.1 WHERE foo and bar and bas
table1, table2 , table3,table4 FROM sangis.org-business-sites-orig-businesses-geo-0.1.1
census.gov-acs-geo-p5ye2012-geofile-0.0.3
census.gov-acs-p5ye2012-b02001-estimates-0.0.4

MVIEW: mview1
SELECT 'mview1'
FROM foobar

VIEW: view2
SELECT 'view2'
FROM foobar

INDEX: name ON table column1, column1
More Documentation About the following Extract.

EXTRACT: foobar AS csv TO /bin/bar/bingo

DOC: None
## Foodoc
Yet more documentation, about the Fringo extract.

EXTRACT: fringo AS geojson TO /bin/bar/geojson
"""

    def tearDown(self):
        pass

    def resolver(self,name):
        if name == self.bundle.identity.name or name == self.bundle.identity.vname:
            return self.bundle
        else:
            return False

    def get_library(self, name='default'):
        """Clear out the database before the test run"""
        from ambry.library import new_library

        config = self.rc.library(name)

        l = new_library(config, reset=True)

        l.database.enable_delete = True
        l.database.drop()
        l.database.create()

        return l


    def get_warehouse(self, l, name):
        from  ambry.util import get_logger
        from ambry.warehouse import new_warehouse

        w = new_warehouse(self.rc.warehouse(name), l)
        w.logger = get_logger('unit_test')


        lr = self.bundle.init_log_rate(10000)
        w.logger = TestLogger(lr)

        w.database.enable_delete = True
        w.database.delete()

        w.create()

        return w

    def _test_local_install(self, name):

        l = self.get_library('local')
        l.clean()


        l.put_bundle(self.bundle)

        w = self.get_warehouse(l, name)
        print "Warehouse: ", w.database.dsn
        print "Library: ", l.database.dsn

        w.install("source-dataset-subset-variation-tone-0.0.1")
        w.install("source-dataset-subset-variation-tthree-0.0.1")
        w.install("source-dataset-subset-variation-geot1-geo-0.0.1")

        w = self.get_warehouse(l, 'spatialite')
        print "WAREHOUSE: ", w.database.dsn

        w.install("source-dataset-subset-variation-tone-0.0.1")
        w.install("source-dataset-subset-variation-tthree-0.0.1")
        w.install("source-dataset-subset-variation-geot1-geo-0.0.1")

    def test_local_sqlite_install(self):
        self._test_local_install('sqlite')

    def test_local_postgres_install(self):
        self._test_local_install('postgres1')

    def _test_remote_install(self, name):

        self.start_server(self.rc.library('server'))

        l = self.get_library('client')
        l.put_bundle(self.bundle)

        w = self.get_warehouse(l, name)
        print "WAREHOUSE: ", w.database.dsn

        w.install("source-dataset-subset-variation-tone-0.0.1")
        w.install("source-dataset-subset-variation-tthree-0.0.1")
        w.install("source-dataset-subset-variation-geot1-geo-0.0.1")

        w = self.get_warehouse(l, 'spatialite')
        print "WAREHOUSE: ", w.database.dsn

        w.install("source-dataset-subset-variation-tone-0.0.1")
        w.install("source-dataset-subset-variation-tthree-0.0.1")
        w.install("source-dataset-subset-variation-geot1-geo-0.0.1")

    def test_remote_sqlite_install(self):
        self._test_remote_install('sqlite')

    def test_remote_postgres_install(self):
        self._test_remote_install('postgres1')

    def test_manifest(self):
        """Load the manifest and convert it to a string to check the round-trip"""
        from ambry.warehouse.manifest import Manifest
        from ambry.util import get_logger

        m = Manifest(self.m,get_logger('TL'), base_dir = '/tmp' )

        import yaml
        #print yaml.dump(m.sections, default_flow_style=False)

        self.assertEqual(self.m.strip(), str(m).strip())


    def test_manifest_doc(self):
        from ambry.util import get_logger
        import logging
        from ambry.text import ManifestDoc

        l  = get_logger('TL')
        l.setLevel(logging.DEBUG)

        m = Manifest(self.m, l, base_dir = '/tmp' )

        print m.html_doc()

    def test_manifest_parser(self):

        import pprint
        lines = [
            "sangis.org-business-sites-orig-businesses-geo-0.1.1",
            "table from sangis.org-business-sites-orig-businesses-geo-0.1.1",
            "table FROM sangis.org-business-sites-orig-businesses-geo-0.1.1",
            "table1, table2 FROM sangis.org-business-sites-orig-businesses-geo-0.1.1",
            "table1, table2 FROM sangis.org-business-sites-orig-businesses-geo-0.1.1 WHERE foo and bar and bas",
            "table1, table2 , table3,table4 FROM sangis.org-business-sites-orig-businesses-geo-0.1.1 # Wot you got?",
        ]


        for line in lines:
            print '----', line
            pprint.pprint( Manifest.parse_partition_line(line))


    def test_manifest_parts(self):
        from ambry.warehouse.manifest import Manifest
        from ambry.util import get_logger
        from ambry.ipython.manifest import ManifestMagicsImpl
        import yaml

        m = Manifest('', get_logger('TL'), base_dir='/tmp')
        mmi = ManifestMagicsImpl(m)

        m_head = """
TITLE:  A Test Manifest, For Testing
UID: b4303f85-7d07-471d-9bcb-6980ea1bbf18
DESTINATION: spatialite:///tmp/census-race-ethnicity.db
DIR: /tmp/warehouse
        """

        mmi.manifest('',m_head)

        mmi.extract('foobar AS csv TO /bin/bar/bingo')
        mmi.extract('foobar AS csv TO /bin/bar/bingo')
        mmi.extract('foobar AS csv TO /bin/bar/bingo2')
        mmi.extract('foobar AS csv TO /bin/bar/bingo')

        mmi.partitions('','one\ntwo\nthree\nfour')

        mmi.view('foo_view_1','1234\n5678\n')
        mmi.view('foo_view_2', '1234\n5678\n')

        mmi.mview('foo_mview_1', '1234\n5678\n')
        mmi.mview('foo_mview_2', '1234\n5678\n')

        mmi.view('foo_view_1', '1234\n5678\n')
        mmi.view('foo_view_2', '1234\n5678\n')

        mmi.mview('foo_mview_1', '1234\n5678\n')
        mmi.mview('foo_mview_2', '1234\n5678\n')

        #print yaml.dump(m.sections, default_flow_style=False)

        print str(m)

    def test_sql_parser(self):

        sql = """
SELECT
    geo.state, -- comment 1
    geo.county, -- comment 2
    geo.tract,
    geo.blkgrp,
    bb.geometry,
    CAST(Area(Transform(geometry,26946)) AS REAL) AS area,
    CAST(b02001001 AS INTEGER) AS total_pop,
    CAST(b03003001 AS INTEGER) AS total_hisp,
    CAST(b02001002 AS INTEGER) AS wht_alone,
    CAST(b02001002*1.0/b02001001 AS REAL) AS wht_frac,
    CAST(b02001003 AS INTEGER) AS blk_alone,
    CAST(b02001003*1.0/b02001001 AS REAL) AS blk_frac,
    CAST(b02001005 AS INTEGER) AS as_alone,
    CAST(b02001005*1.0/b02001001 AS REAL) AS as_frac,
    CAST(b03003002 AS INTEGER) AS not_hisp,
    CAST(b03003003 AS INTEGER) AS hisp,
    CAST(b03003003*1.0/b02001001 AS REAL) AS hisp_frac,
    CAST(C17002002+C17002003+C17002004 AS VARCHAR) AS poverty,
    CAST((C17002002+C17002003+C17002004)*1.0/b02001001 AS REAL) AS pov_frac
FROM d02G003_geofile  AS geo
 JOIN d024004_b02001_estimates AS b02001e ON geo.stusab = b02001e.stusab AND geo.logrecno = b02001e.logrecno
 JOIN d024004_b03002_estimates AS b03002e ON geo.stusab = b03002e.stusab AND geo.logrecno = b03002e.logrecno
 JOIN d024004_b03003_estimates AS b03003e ON geo.stusab = b03003e.stusab AND geo.logrecno = b03003e.logrecno
 JOIN d024004_b01001_estimates AS b01001e ON geo.stusab = b01001e.stusab AND geo.logrecno = b01001e.logrecno
 JOIN d024004_c17002_estimates AS c17002e ON geo.stusab = c17002e.stusab AND geo.logrecno = c17002e.logrecno
 JOIN blockgroup_boundaries AS bb ON geo.state = bb.state AND geo.county = bb.county AND bb.tract = geo.tract AND bb.blkgrp = geo.blkgrp
WHERE geo.sumlevel = 150 AND geo.state = 6 and geo.county = 73


"""

        import sqlparse
        import sqlparse.sql


        r =  sqlparse.parse(sql)

        for t in  r[0].tokens:
            if isinstance(t, sqlparse.sql.IdentifierList):
                for i in t.get_identifiers():
                    print i, i.get_alias(),  type(i)


        #print sqlparse.format(sql, strip_comments = True, reindent = True)

    def test_extract(self):

        from ambry.warehouse.extractors import extract

        p = self.bundle.partitions.find(table='tthree')

        print extract(p.database, 'tthree', 'csv', '/tmp/extract/', 'foo/bar')


    def x_test_install(self):
        
        def resolver(name):
            if name == self.bundle.identity.name or name == self.bundle.identity.vname:
                return self.bundle
            else:
                return False
        
        def progress_cb(lr, type,name,n):
            if n:
                lr("{} {}: {}".format(type, name, n))
            else:
                self.bundle.log("{} {}".format(type, name))
        
        from ambry.warehouse import new_warehouse
        from functools import partial
        print "Getting warehouse"
        w = new_warehouse(self.rc.warehouse('postgres'))

        print "Re-create database"
        w.database.enable_delete = True
        w.resolver = resolver
        w.progress_cb = progress_cb
        
        try: w.drop()
        except: pass
        
        w.create()

        ps = self.bundle.partitions.all
        
        print "{} partitions".format(len(ps))
        
        for p in self.bundle.partitions:
            lr = self.bundle.init_log_rate(10000)
            w.install(p, progress_cb = partial(progress_cb, lr) )

        self.assertTrue(w.has(self.bundle.identity.vname))

        for p in self.bundle.partitions:
            self.assertTrue(w.has(p.identity.vname))

        for p in self.bundle.partitions:
            w.remove(p.identity.vname)

        print w.get(self.bundle.identity.name)
        print w.get(self.bundle.identity.vname)
        print w.get(self.bundle.identity.id_)
        
        w.install(self.bundle)
         
        print w.get(self.bundle.identity.name)
        print w.get(self.bundle.identity.vname)
        print w.get(self.bundle.identity.id_)

        for p in self.bundle.partitions:
            lr = self.bundle.init_log_rate(10000)
            w.install(p, progress_cb = partial(progress_cb, lr))




def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())