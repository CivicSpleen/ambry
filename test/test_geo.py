"""
Created on Aug 31, 2012

@author: eric
"""
import unittest

from bundles.testbundle.bundle import Bundle
from test_base import TestBase  # @UnresolvedImport


class Test(TestBase):
    def setUp(self):
        import os
        from ambry.run import get_runconfig, RunConfig

        self.copy_or_build_bundle()

        self.bundle = Bundle()
        self.bundle_dir = self.bundle.bundle_dir

        self.rc = get_runconfig(
            (os.path.join(self.bundle_dir, 'geo-test-config.yaml'),
             os.path.join(self.bundle_dir, 'bundle.yaml'),
             RunConfig.USER_ACCOUNTS)
        )

    def tearDown(self):
        pass

    def test_wkb(self):
        b = Bundle()
        p = b.partitions.find(table='geot2')

        for row in p.query(
                "SELECT quote(AsBinary(GEOMETRY)) as wkb, quote(GEOMETRY) "
                "FROM geot2"):
            print row
            # g = row['GEOMETRY']
            # print g.encode('hex')
            # print type(row['GEOMETRY'])
            # pnt = loads(str(row['GEOMETRY']))
            # print pnt

    def get_library(self, name='default'):
        """Clear out the database before the test run"""
        from ambry.library import new_library

        config = self.rc.library(name)

        l = new_library(config, reset=True)

        return l

    def test_ba_geocoding(self):
        from ambry.geo.geocoder import Geocoder

        l = self.bundle.library

        gp = l.get('clarinova.com-geocode-casnd-geocoder->=2.0.8').partition

        g = Geocoder(gp)

        p = l.get('sandiegodata.org-bad_addresses-casnd-addresses').partition

        for row in p.query(
                "SELECT text from addresses where address_id is NULL limit 10"):
            text = row.text

            text = text.replace('La Jolla', 'San Diego')

            addr_id, r, parsed = g.parse_and_code(text)

            score = r['score'] if r else None

            print '------', score, addr_id
            print row.p
            print '> ', text
            print '< ', parsed

    def test_csv_geocoding(self):
        from ambry.geo.geocoder import Geocoder
        import test.support as ts
        import os.path

        import csv

        l = self.bundle.library

        gp = l.get('clarinova.com-geocode-casnd-geocoder->=2.0.8').partition

        g = Geocoder(gp)

        with open(os.path.join(os.path.dirname(ts.__file__),
                               'bad_geocodes.csv')) as f:
            reader = csv.DictReader(f)

            for row in reader:
                text = row['text']

                addr_id, r, parsed = g.parse_and_code(text)

                score = r['score'] if r else None

                print '------', score, addr_id
                print '> ', text
                print '< ', parsed

    def test_geocoding_csv_geocoder(self):
        from ambry.geo.geocoder import Geocoder

        l = self.bundle.library

        gp = l.get('clarinova.com-geocode-casnd-geocoder->=2.0.8').partition

        g = Geocoder(gp)

        for row in gp.query(
                "select * from geocoder where number > 0 limit 1000"):

            text = "{number} {dir} {name} {suffix}, {city}, {state} {zip}".format(
                number=row.number, name=row.name, state=row.state,
                city=row.city if row.city else '',
                dir=row.direction if row.direction != '-' else '',
                suffix=row.suffix if row.suffix != '-' else '',
                zip=row.zip if row.zip > 0 else ''
            )

            addr_id, r, parsed = g.parse_and_code(text)

            if not r:
                score = r['score'] if r else None

                print '------', score, addr_id
                print '> ', text
                print '< ', parsed

    def test_txt_geocoding(self):
        from ambry.geo.geocoder import Geocoder
        import test.support as ts
        import os.path

        l = self.bundle.library

        gp = l.get('clarinova.com-geocode-casnd-geocoder->=2.0.8').partition

        city_subs = {
            'La Jolla': 'San Diego'
        }

        g = Geocoder(gp, city_subs)

        with open(os.path.join(os.path.dirname(ts.__file__),
                               'bad_geocodes.txt')) as f:
            for line in f:
                text = line.strip()

                addr_id, r, parsed = g.parse_and_code(text)

                score = r['score'] if r else None

                print '------', score, addr_id
                print '> ', text
                print '< ', parsed

    def test_dstk(self):
        from ambry.util import parse_url_to_dict, unparse_url_dict

        # Test that the services configuration works

        urls = [
            'http://scott:tiger@localhost:5432/mydatabase?foo=bar'
            'http://scott:tiger@localhost/mydatabase?foo=bar'
            'http://scott@localhost:5432/mydatabase?foo=bar'
            'http://localhost:5432/mydatabase/'
        ]

        for url in urls:
            self.assertEquals(url, unparse_url_dict(parse_url_to_dict(url)))

        self.assertEquals(
            'postgres://account-username:account-password@foo.bar.bingo:5432/mydatabase',
            unparse_url_dict(self.rc.service('dstk')))

        self.assertEquals(
            'postgres://account-username:account-password@foo.bar.bingo:5432/mydatabase',
            unparse_url_dict(self.rc.service('geocoder')))

    def test_dstk_geocoding(self):
        from ambry.geo.geocoders import DstkGeocoder
        import pprint

        l = self.bundle.library
        p = l.get('sandiegodata.org-bad_addresses-casnd-addresses').partition

        dstk_service = self.rc.service('dstk')

        def address_gen():
            for row in p.query("SELECT text from addresses where address_id is NULL limit 20"):
                text = row.text
                yield text

        dstk_gc = DstkGeocoder(dstk_service, address_gen())

        for k, r in dstk_gc.geocode():
            print '---'
            print "{:6s} {}".format(str(r['confidence']) if r else '', k)
            pprint.pprint(r)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite


if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())