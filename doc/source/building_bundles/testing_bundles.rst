.. _testing_bundles:

Review and Testing
==================

After building a bundle you can review which partitions were built with :command:`bambry info`:

.. code-block:: bash

    $ bambry info 
    
    Title     : Agricultural Productivity in the U.S.
    Summary   : This data set provides estimates of productivity growth in the U.S. farm sector for the 1948-2011 period, and estimates of the growth and relative levels of productivity across the States for the period 1960-2004.
    VID       : du0GgFx9SE001
    VName     : ers.usda.gov-agg_product-0.0.1
    DB        : /Users/eric/proj/virt/ambry-master/data/source/example-bundles/ers.usda.gov/agg_product/build/ers.usda.gov/agg_product-0.0.1.db
    Geo cov   : ['0a']
    Grain cov : []
    Time cov  : 1948/2011
    Created   : 2015-06-09T15:20:21.143165
    Prepared  : 2015-06-09T15:20:26.011596
    Built     : 2015-06-09T15:21:05.484239
    Build time: 16.7s
    Parts     : 24
    -----Partitions-----
              : ers.usda.gov-agg_product-labor_input
              : ers.usda.gov-agg_product-chemical_input
              : ers.usda.gov-agg_product-other_inputs
              : ers.usda.gov-agg_product-crop_output
              : ers.usda.gov-agg_product-output_growth
              : ers.usda.gov-agg_product-capital_input
              : ers.usda.gov-agg_product-energy_input
              : ers.usda.gov-agg_product-statepriceindicesandq
              : ... and 18 more
    

Review that the Title and summary are set, and that the partitions you expect are in place. Next, run the command again, but with the ``-P -S`` options to display all of the partitions and column statistics:


.. code-block:: bash 

    $ bambry info -P -S
    ...
    Col Name            :   Count    Uniq       Mean Sample Values                                                         
    va                  :      45      45   5.23e-01 ▉▃ ▁▁▁▃▂▂▅                                                            
    co                  :      45      45   8.86e-01 ▃▁▁▁▄▉▄▃▃▄                                                            
    ca                  :      45      45   2.41e+00 ▄▆▉▄▂▃▂▁▂▂
    ...
    
Check that the VARCHAR columns have the values you expect, and that the count and uniq value are in the right range. 

Writing Tests
*************

Bundles should have some test to ensure that the bundle built correctly. Typically, the test will get a partition, convert it to a PANDAS data frame, compute statistics, and assert the results. For instance: 

.. code-block:: python 

        
    def test(self):
        
        import numpy as np
        
        df = self.partitions.all[0].pandas

        df['total_births_prenat'] = df[self.all_prenat_cols].sum(axis=1).astype('int')
        df['total_births_age'] = df[self.all_age_cols].sum(axis=1).astype('int')
        df['total_births_weight'] = df[self.all_weight_cols].sum(axis=1).astype('int')
        df['total_births'] = df['total_births'].astype('int')

        assert (all(df['total_births_prenat'] == df['total_births' ]))
        assert (all(df['total_births_age'] == df['total_births' ]))
        assert (all(df['total_births_age'] == df['total_births' ]))
        
        # The 99998 and 99999 catch-all zipcodes are broken for 2006
        not_broke = np.logical_not(((df.zipcode == 99998) | (df.zipcode == 99999)) &  (df.year == 2006))
        
        assert (all(df[not_broke]['total_births_weight'] == df[not_broke]['total_births' ]))


