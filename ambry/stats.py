"""Statistical operations on Civic Knowledge datasets.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""
import pandas as pd

class StandardError(object):
    """Propagate standard errors"""

    def __init__(self,df):
        self.df = df

    # Find the standard error for a column

    def se(self, col):

        col_name = col.name

        if col_name+'_se' in self.df:
            return  self.df[col_name+'_se']

        if col_name+'_m' in self.df:
            source = self.df[col_name+'_m'].astype(float) / 1.645
        elif col_name+'_m90' in self.df:
            source = self.df[col_name+'_m90'].astype(float) / 1.645
        elif col_name+'_m95' in self.df:
            source = self.df[col_name+'_m95'].astype(float) / 1.96
        else:
            raise ValueError("Didn't find a margin of error or standard error column")

        self.df[col_name+'_se'] = source

        return self.df[col_name+'_se']

    def add_se(self,*cols):

        for col in cols:
            if not col.name+'_se' in self.df:
                self.df[col.name+'_se'] = self.se(col)

    def cv(self, col):
        return (self.se(col).astype(float) / col.astype(float)) * 100.0

    def cvt(self, col):
        """Tuple with input column and CV"""
        if isinstance(col, (tuple, list)):
            # The SE is in the tuple
            return ( col[0], col[1].astype(float)/col[0].astype(float) * 100.0)
        else:
            return (col, self.cv(col))

    def sum(self,  *cols):
        import numpy as np

        if not isinstance(cols[0], (tuple, list)):
            cols = [ (col, self.se(col)) for col in cols ]

        subset = self.df[[col[0].name for col in cols]]
        subset_se = self.df[[col[1].name for col in cols]]

        return (
                sum([c[0] for c in cols]),
                np.sqrt(sum([c[1]**2 for c in cols]))
            )

    def _calc_div_se(self, rate, num, denom):

        import numpy as np

        t1 = num[1].astype(float) ** 2
        t2 = ((rate ** 2) * (denom[1].astype(float) ** 2))
        t3 = denom[0].astype(float)

        se = np.sqrt(t1 - t2) / t3

        # In the case of a neg arg to a square root, the ACS Handbook recommends using the
        # method for "Calculating MOEs for Derived Ratios", where the numerator
        # is not a subset of the denominator. Since our numerator is a subset, the
        # handbook says " use the formula for derived ratios in the next section which
        # will provide a conservative estimate of the MOE."
        se_ne_sqrt = np.sqrt(t1 + t2) / t3

        se.fillna(se_ne_sqrt, inplace=True)

        return se

    def div(self, num, denom):
        import numpy as np

        if not isinstance(num, (tuple, list)):
            num = (num, self.se(num))

        if not isinstance(denom, (tuple, list)):
            denom = (denom, self.se(denom))

        rate = num[0].astype(float) / denom[0].astype(float)

        # SE for proportion = sqrt( SEnum**2 - p**2*SEdenom**2 ) / denom

        se = self._calc_div_se(rate, num, denom)

        return (rate, se)

    def pct(self, num, denom):
        """Division that results in a percent"""
        d = self.div(num, denom)

        return (d[0]*100, d[1]*100)

class AmbrySEDataFrame(pd.DataFrame):
    """Augment a dataframe with setters that handle percentage and StandardError columns"""

    def __init__(self, std_err, data=None, index=None, columns=None, dtype=None, copy=False):
        super(AmbrySEDataFrame, self).__init__(data, index, columns, dtype, copy)

        self.se = std_err

    def aug_cv(self, column_name, col ):
        """Add a column to the dataframe, along with it's Coefficient of Variation """

        self[column_name], self[column_name+'_cv'] = self.se.cvt(col)


    def aug_pct_cv(self, column_name, num, denom):
        """Add a column to the dataframe with the numerator, its CV, and it's percentage and percentage CV"""

        self[column_name], self[column_name + '_cv'] = self.se.cvt(num)
        self[column_name+'_pct'], self[column_name + '_pct_cv'] =  self.se.cvt(self.se.pct(num, denom))

    def aug_div_cv(self, column_name, num, denom):
        """Add a column to the dataframe with the numerator, its CV, and it's percentage and percentage CV"""

        self[column_name], self[column_name + '_cv'] = self.se.cvt(self.se.div(num, denom))