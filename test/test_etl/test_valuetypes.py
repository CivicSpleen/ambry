# -*- coding: utf-8 -*-
import os
import unittest


class Test(unittest.TestCase):


    def test_basic(self):

        from ambry.valuetype.usps import StateAbr

        sa =  StateAbr('AZ')
        self.assertEquals('AZ' ,sa)
        self.assertEquals(4, sa.fips)
        self.assertEquals('Arizona', sa.name)

        # Convert to a FIPS code
        self.assertEquals('04', sa.fips.str)
        self.assertEquals('04000US04', sa.fips.geoid)
        self.assertEquals('04', sa.fips.tiger)
        self.assertEquals('0E04', sa.fips.gvid)
        self.assertEquals('Arizona', sa.fips.name)

        print sa.fips.usps.fips.usps

        from ambry.valuetype.census import AcsGeoid

        g = AcsGeoid('15000US530330018003')

        print g.state.name, g.state.usps




