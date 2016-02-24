"""Support for Pandas Dataframes

Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""


from pandas import DataFrame, Series
import numpy as np
from six import string_types



class AmbrySeries(Series):

    def m90(self):
        raise NotImplementedError()

    def se(self):
        """Return a standard error series"""
        raise NotImplementedError()

    def m95(self):
        """Return the 95% margins as an AmbrySeries"""
        raise NotImplementedError()


    def m99(self):
        """Return the 99% margins, as an AmbrySeries"""
        raise NotImplementedError()

    @property
    def ambry_column(self):
        from ambry.orm.exc import NotFoundError

        if not hasattr(self, 'partition'):
            return None

        if not self.name:
            return None

        try:
            return self.partition.table.column(self.name)
        except NotFoundError:
            return None


class AmbryDataFrame(DataFrame):

    def __init__(self, partition, *args, **kwargs):

        super(AmbryDataFrame, self).__init__(*args, **kwargs)

        self.partition = partition

    @classmethod
    def subclass(cls, o, partition=None):
        """Change the class of a dataframe to An AmbryDataFrame and set the partition. """
        o.__class__ == cls

        if partition:
            o.partition = partition

        return o

    def __getitem__(self, key):
        """

        """
        result = super(AmbryDataFrame, self).__getitem__(key)

        if isinstance(result, DataFrame):
            result.__class__ = AmbryDataFrame
            result.partition = self.partition

        elif isinstance(result, Series):
            result.__class__ = AmbrySeries
            result._dataframe = self
            result.partition = self.partition

        return result