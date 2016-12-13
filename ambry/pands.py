"""Support for Pandas Dataframes

Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""


from pandas import DataFrame, Series
import numpy as np
from six import string_types


class AmbrySeries(Series):

    _metadata = ['partition', 'name'] # Name is defined in the parent

    @property
    def _constructor(self):
        return AmbrySeries

    @property
    def _constructor_expanddim(self):
        return AmbryDataFrame

    @property
    def column(self):
        """Return the ambry column"""
        from ambry.orm.exc import NotFoundError

        if not hasattr(self, 'partition'):
            return None

        if not self.name:
            return None

        try:
            try:
                return self.partition.column(self.name)
            except AttributeError:
                return self.partition.table.column(self.name)
        except NotFoundError:
            return None


class AmbryDataFrame(DataFrame):

    _metadata = ['partition', 'plot_axes']

    def __init__(self, data=None, index=None, columns=None, dtype=None, copy=False, partition=None):
        from ambry.orm import Partition

        if partition:
            self.partition = partition
        else:
            self.partition = None

        self.plot_axes = []

        super(AmbryDataFrame, self).__init__(data, index, columns, dtype, copy)

    @property
    def _constructor(self):

        return AmbryDataFrame

    @property
    def _constructor_sliced(self):
        return AmbrySeries

    def _getitem_column(self, key):
        c = super(AmbryDataFrame, self)._getitem_column(key)
        c.partition = self.partition
        return c

    @property
    def rows(self):
        """Yield rows like a partition does, with a header first, then rows. """

        yield [self.index.name] + list(self.columns)

        for t in self.itertuples():
            yield list(t)


