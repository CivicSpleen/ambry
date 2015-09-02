""""
These are random tests used in development. They aren't meant to be comprehensive or to exercise any specific bugs. """

from test.test_base import TestBase
import unittest

from ambry.bundle import Bundle

class Test(TestBase):

    @unittest.skip("Development Test")
    def test_install(self):
        """Test copying a bundle to a remote, then streaming it back"""
        from boto.exception import S3ResponseError
        b = self.setup_bundle('simple', source_url='temp://')
        l = b._library

        b.sync_in()

        b.run()

        self.assertEqual(1, len(list(l.bundles)))

        p = list(b.partitions)[0]
        p_vid = p.vid

        self.assertEquals(497054, int(sum(row[3] for row in p.stream(skip_header=True))))

        self.assertEqual('build', l.partition(p_vid).location)

        try:
            remote_name, path = b.checkin()
        except S3ResponseError as exc:
            if exc.status == 403:  # Forbidden.
                raise unittest.SkipTest(
                    'Skip S3 error - {}. It seems S3 credentials are not valid.'.format(exc))
            else:
                raise

        print remote_name, path

    @unittest.skip("Development Test")
    def test_search(self):
        """Test copying a bundle to a remote, then streaming it back"""
        from ambry.library import new_library

        l = new_library(self.get_rc())

        l.sync_remote('test')

        b = list(l.bundles)[0]
        p = list(b.partitions)[0]

        self.assertEqual(1, len(list(l.bundles)))

        self.assertEqual('remote', l.partition(p.vid).location)

        #self.assertEquals(497054, int(sum(row[3] for row in p.stream(skip_header=True))))
        #self.assertEqual(10000, len(list(p.stream(skip_header=True))))
        #self.assertEqual(10001, len(list(p.stream(skip_header=False))))

        search = l.search

        search.index_library_datasets()

        self.assertEquals([u'd000simple003', u'p000simple002003'], list(search.list_documents()))

        print search.search_datasets('d000simple003')[0].vid

        print search.search_datasets('Example')[0].vid

        print search.search_datasets('2010')

    @unittest.skip("Development Test")
    def test_sequence(self):
        from urlparse import urlparse
        from ambry.orm import Database

        conf = self.get_rc()

        if 'database' in conf.dict and 'postgresql-test' in conf.dict['database']:
            dsn = conf.dict['database']['postgresql-test']
            parsed_url = urlparse(dsn)
            db_name = parsed_url.path.replace('/', '')
            self.postgres_dsn = parsed_url._replace(path='postgres').geturl()
            self.postgres_test_db = '{}_test_db1ae'.format(db_name)
            self.postgres_test_dsn = parsed_url._replace(path=self.postgres_test_db).geturl()

        db = Database(self.postgres_test_dsn)

        for i in range(10):

            print b.dataset.next_number('foobar')


    def test_cluster(self):

        # Headers from the HCI bundle
        headers = [
 ["abuse_neglect", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_fips", "county_name", "region_code", "region_name", "strata_name_code", "strata_name", "strata_level_name_code", "strata_level_name", "allegations_children", "total_children", "percent", "ll_95ci", "ul_95ci", "se", "rse", "ca_decile", "ca_rr", "version"],
 ["air_quality", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "numerator", "poppt", "pm25_concentration", "ll_95ci", "ul_95ci", "se", "rse", "pm25_decile", "pm25ratio_ca", "version"],
 ["alcohol_outlets_an", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_fips", "county_name", "region_code", "region_name", "license_type", "numerator", "denominator", "percent", "ll_95ci", "ul_95ci", "se", "rse", "ca_decile", "ca_rr", "version"],
 ["alcohol_outlets_oy", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_fips", "county_name", "region_code", "region_name", "license_type", "numerator", "denominator", "percent", "ll_95ci", "ul_95ci", "se", "rse", "ca_decile", "ca_rr", "version"],
 ["food_affordability", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "cost_yr", "median_income", "affordability_ratio", "ll95_affordability_ratio", "ul95_affordability_ratio", "se_food_afford", "rse_food_afford", "food_afford_decile", "ca_rr_affordability", "ave_fam_size", "version"],
 ["healthy_food", "ind_id", "ind_definition", "reportyear", "race_eth_name", "race_eth_code", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "mrfei", "ll95ci", "ul95ci", "se", "rse", "ca_decile", "ca_rr", "pop00", "version"],
 ["high_school_ed", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "pop25pl_hs", "pop25pl", "p_hs_edatt", "se", "rse", "ll_95ci", "ul_95ci", "ca_decile", "ca_rr", "version"],
 ["household_crowding", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "income_level", "tenure", "crowding_cat", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "total_hshlds", "crowded_hshlds", "percent", "ll95ci", "ul95ci", "se", "rse", "ca_decile", "ca_rr", "version"],
 ["household_type", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "strata_name_code", "strata_name", "strata_level_name_code", "strata_level_name", "households", "total_households", "households_percent", "ll95ci_percent", "ul95ci_percent", "percent_se", "percent_rse", "ca_decile", "ca_rr", "version"],
 ["household_type_tracts", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "strata_name_code", "strata_name", "strata_level_name_code", "strata_level_name", "households", "total_households", "households_percent", "ll95ci_percent", "ul95ci_percent", "percent_se", "percent_rse", "ca_decile", "ca_rr", "version"],
 ["housing_cost_an", "ind_id", "ind_definition", "datasource", "reportyear", "burden", "tenure", "race_eth_code", "race_eth_name", "income_level", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "total_households", "burdened_households", "percent", "ll95ci", "ul95ci", "se", "rse", "ca_decile", "ca_rr", "version"],
 ["housing_cost_os", "ind_id", "ind_definition", "datasource", "reportyear", "burden", "tenure", "race_eth_code", "race_eth_name", "income_level", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "total_households", "burdened_households", "percent", "ll95ci", "ul95ci", "se", "rse", "ca_decile", "ca_rr", "version"],
 ["housing_cost_ty", "ind_id", "ind_definition", "datasource", "reportyear", "burden", "tenure", "race_eth_code", "race_eth_name", "income_level", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "total_households", "burdened_households", "percent", "ll95ci", "ul95ci", "se", "rse", "ca_decile", "ca_rr", "version"],
 ["income_inequality", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "numerator", "households", "gini_index", "ll_95ci", "ul_95ci", "se", "rse", "ca_decile", "ca_rr", "median_hh_income", "median_hh_decile", "version"],
 ["jobs_employed_ratio", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "msa_name", "msa_code", "strata_name_code", "strata_name", "strata_level_name_code", "strata_level_name", "jobs", "employed_res", "ratio", "ll_95ci", "ul_95ci", "ratio_se", "ratio_rse", "ratio_decile", "ms_rr", "version"],
 ["jobs_housing_ratio", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "msa_name", "msa_code", "strata_name_code", "strata_name", "strata_level_name_code", "strata_level_name", "jobs", "housing", "ratio", "ll_95ci", "ul_95ci", "ratio_se", "ratio_rse", "ratio_decile", "ms_rr", "version"],
 ["living_wage", "ind_id", "ind_definition", "reportyear", "family_type", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "fam_lt_lw", "families", "pct_lt_lw", "ll_95ci", "ul_95ci", "se", "rse", "family_type_decile", "ca_rr", "livingwage", "version"],
 ["miles_traveled", "ind_id", "ind_definition", "reportyear", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "mode", "avmttotal", "totalpop", "mpcratio", "ll95ci_mpcratio", "ul95ci_mpcratio", "mpcratio_se", "mpcratio_rse", "ca_decile_mpcratio", "ca_rr_mpc", "areasqmi", "mpsqmiratio", "ll95ci_mpsqmiratio", "ul95ci_mpsqmiratio", "mpsqmiratio_se", "mpsqmiratio_rse", "ca_decile_mpsqmiratio", "ca_rr_mpsqmi", "groupquarters", "version"],
 ["neighborhood_change_al", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "strata_name_code", "strata_name", "strata_level_name_code", "strata_level_name", "numberhh00_10dol", "numberhh10_10dol", "difference", "ll_95ci", "ul_95ci", "se", "rse", "place_decile", "ca_rr", "version"],
 ["neighborhood_change_ls", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "strata_name_code", "strata_name", "strata_level_name_code", "strata_level_name", "numberhh00_10dol", "numberhh10_10dol", "difference", "ll_95ci", "ul_95ci", "se", "rse", "place_decile", "ca_rr", "version"],
 ["neighborhood_change_sy", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "strata_name_code", "strata_name", "strata_level_name_code", "strata_level_name", "numberhh00_10dol", "numberhh10_10dol", "difference", "ll_95ci", "ul_95ci", "se", "rse", "place_decile", "ca_rr", "version"],
 ["open_space", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "pop_park_acc", "pop2010", "p_parkacc", "ll_95ci", "ul_95ci", "se", "rse", "ca_decile", "ca_rr", "version"],
 ["ozone", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "numerator", "poppt", "o3_unhealthy_days", "ll_95ci", "ul_95ci", "se", "rse", "ozone_decile", "o3ratio_ca", "version"],
 ["poverty_rate", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "poverty", "totalpop", "numpov", "percent", "ll_95ci_percent", "ul_95ci_percent", "percent_se", "percent_rse", "place_decile", "ca_rr", "concentratedct", "version"],
 ["public_transit_bay", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "pop_trans_acc", "pop2010", "p_trans_acc", "ll_95ci", "ul_95ci", "se", "rse", "mtc_decile", "mtc_rr", "version"],
 ["public_transit_sac", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "pop_trans_acc", "pop2010", "p_trans_acc", "ll_95ci", "ul_95ci", "se", "rse", "sac_decile", "sac_rr", "version"],
 ["public_transit_sc", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "pop_trans_acc", "pop2010", "p_trans_acc", "ll_95ci", "ul_95ci", "se", "rse", "sc_decile", "sc_rr", "version"],
 ["public_transit_sd", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "pop_trans_acc", "pop2010", "p_trans_acc", "ll_95ci", "ul_95ci", "se", "rse", "sd_decile", "sd_rr", "version"],
 ["registered_voters", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_fips", "county_name", "region_code", "region_name", "type", "numerator", "denominator", "percent", "ll_95ci", "ul_95ci", "se", "rse", "ca_decile", "ca_rr", "vap", "version"],
 ["traffic_fatalities_an", "ind_id", "ind_definition", "reportyear", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "mode", "severity", "injuries", "totalpop", "poprate", "ll95ci_poprate", "ul95ci_poprate", "poprate_se", "poprate_rse", "ca_decile_pop", "ca_rr_poprate", "avmttotal", "avmtrate", "ll95ci_avmtrate", "ul95ci_avmtrate", "avmtrate_se", "avmtrate_rse", "ca_decile_avmt", "ca_rr_avmtrate", "groupquarters", "version"],
 ["traffic_fatalities_oy", "ind_id", "ind_definition", "reportyear", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "mode", "severity", "injuries", "totalpop", "poprate", "ll95ci_poprate", "ul95ci_poprate", "poprate_se", "poprate_rse", "ca_decile_pop", "ca_rr_poprate", "avmttotal", "avmtrate", "ll95ci_avmtrate", "ul95ci_avmtrate", "avmtrate_se", "avmtrate_rse", "ca_decile_avmt", "ca_rr_avmtrate", "groupquarters", "version"],
 ["transport_work", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "mode", "mode_name", "pop_total", "pop_mode", "percent", "ll95ci_percent", "ul95ci_percent", "percent_se", "percent_rse", "ca_decile", "ca_rr", "version"],
 ["unemployment", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_fips", "county_name", "region_code", "region_name", "unemployment", "labor_force", "unemployment_rate", "ll_95ci", "ul_95ci", "se", "rse", "place_decile", "ca_rr", "version"],
 ["unsafe_water", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_fips", "county_name", "region_code", "region_name", "category", "numerator", "denominator", "percent", "ll_95ci", "ul_95ci", "se", "rse", "ca_decile", "ca_rr", "tot_pop", "coverage", "version"],
 ["unsafe_water_oy", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_fips", "county_name", "region_code", "region_name", "category", "numerator", "denominator", "percent", "ll_95ci", "ul_95ci", "se", "rse", "ca_decile", "ca_rr", "tot_pop", "coverage", "version"],
 ["violent_crime", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "numerator", "denominator", "ratex1000", "ll_95ci", "ul_95ci", "se", "rse", "ca_decile", "rr_city2state", "version"],
 ["walk_bicycyle", "ind_id", "ind_definition", "reportyear", "race_eth_code", "race_eth_name", "geotype", "geotypevalue", "geoname", "county_name", "county_fips", "region_name", "region_code", "modetwk", "n_total", "n_time_mode", "percent", "ll95ci_percent", "ul95ci_percent", "percent_se", "percent_rse", "ca_decile", "ca_rr", "version"],
        ]



