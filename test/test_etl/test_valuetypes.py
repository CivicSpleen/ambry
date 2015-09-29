# -*- coding: utf-8 -*-
import os
import unittest


class Test(unittest.TestCase):


    def test_basic(self):

        from ambry.valuetype.usps import StateAbr

        sa =  StateAbr('AZ')
        self.assertEqual('AZ' ,sa)
        self.assertEqual(4, sa.fips)
        self.assertEqual('Arizona', sa.name)

        # Convert to a FIPS code
        self.assertEqual('04', sa.fips.str)
        self.assertEqual('04000US04', sa.fips.geoid)
        self.assertEqual('04', sa.fips.tiger)
        self.assertEqual('0E04', sa.fips.gvid)
        self.assertEqual('Arizona', sa.fips.name)
        self.assertEqual('AZ', sa.fips.usps.fips.usps)

        print

        from ambry.valuetype.census import AcsGeoid

        g = AcsGeoid('15000US530330018003')

        self.assertEqual('Washington', g.state.name)
        self.assertEqual('WA', g.state.usps)