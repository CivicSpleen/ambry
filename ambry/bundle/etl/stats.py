# coding: utf-8
"""

Computing stats on the fly for data written to a partition

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ambry.util import Constant
from collections import Counter
from livestats import livestats
from geoid import civick

def text_hist(nums):
    parts = u' ▁▂▃▄▅▆▇▉'

    nums = list(nums)
    fraction = max(nums) / float(len(parts) - 1)
    if fraction:
        return ''.join(parts[int(round(x / fraction))] for x in nums)
    else:
        return ''


class StatSet(object):
    LOM = Constant()  # Level of Measurement, More or Less

    # Real levels of Measurement
    LOM.NOMINAL = 'n'
    LOM.ORDINAL = 'o'
    LOM.INTERVAL = 'i'
    LOM.RATIO = 'r'

    def __init__(self, column):

        self.is_gvid = bool("gvid" in column.name)
        self.is_year = bool("year" in column.name)
        self.is_time = column.type_is_time()
        self.is_date = column.type_is_date()

        if column.is_primary_key or self.is_year or self.is_time or self.is_date:
            lom = StatSet.LOM.ORDINAL
        elif column.type_is_text() or self.is_gvid:
            lom = StatSet.LOM.NOMINAL
        elif column.type_is_number():
            lom = StatSet.LOM.INTERVAL


        self.column_name = column.name
        self.lom = lom
        self.n = 0
        self.counts = Counter()
        self.stats = livestats.LiveStats() #runstats.Statistics()

        self.bin_min = None
        self.bin_max = None
        self.bin_width = None
        self.bin_primer_count = 5000 # how many point to collect before creating hist bins
        self.num_bins = 16
        self.bins = [0] * self.num_bins

    def add(self, v):
        from math import sqrt

        self.n += 1

        if self.lom == self.LOM.NOMINAL or self.lom == self.LOM.ORDINAL:
            if self.is_time or self.is_date:
                self.counts[str(v)] += 1
            else:
                self.counts[v] += 1

        elif self.lom == self.LOM.INTERVAL or self.lom == self.LOM.RATIO:

            # To build the histogram, we need to collect counts, but would rather
            # not collect all of the values. So, collect the first 5K, then use that
            # to determine the 4sigma range of the histogram.
            # HACK There are probably a lot of 1-off errors in this

            if self.n < 5000:
                self.counts[v] += 1
            elif self.n == 5000:
                # If less than 1% are unique, assume that this number is actually an ordinal
                if self.nuniques < 50:
                    self.lom = self.LOM.ORDINAL
                    self.stats = livestats.LiveStats()
                else:
                    self.bin_min = self.stats.mean() - sqrt(self.stats.variance()) * 2
                    self.bin_max = self.stats.mean() + sqrt(self.stats.variance()) * 2
                    self.bin_width = (self.bin_max - self.bin_min) / self.num_bins

                self.counts = Counter()
            elif self.n > 5000 and v >= self.bin_min and v <= self.bin_max:
                bin = int((v - self.bin_min) / self.bin_width)
                self.bins[bin] += 1

            self.stats.add(v)
        else:
            assert False, "Really should be one or the other ... "

    @property
    def uniques(self):
        return list(self.counts)

    @property
    def nuniques(self):
        return len(self.counts.items())

    @property
    def mean(self):
        return self.stats.mean()

    @property
    def stddev(self):
        from math import sqrt
        return sqrt(self.stats.variance())

    @property
    def min(self):
        return self.stats.minimum()

    @property
    def p25(self):
        try:
            return self.stats.quantiles()[0][1]
        except IndexError:
            return None

    @property
    def median(self):
        try:
            return  self.stats.quantiles()[1][1]
        except IndexError:
            return None

    @property
    def p50(self):
        try:
            return self.stats.quantiles()[1][1]
        except IndexError:
            return None

    @property
    def p75(self):
        try:
            return self.stats.quantiles()[2][1]
        except IndexError:
            return None

    @property
    def max(self):
        return self.stats.maximum()

    @property
    def skewness(self):
        return self.stats.skewness()

    @property
    def kurtosis(self):
        return self.stats.kurtosis()

    @property
    def hist(self):
        return text_hist(self.bins)

    @property
    def dict(self):
        """Return a  dict that can be passed into the ColumnStats constructor"""
        return dict(
            count=self.n,
            uvalues=dict(self.counts.most_common(100)),
            nuniques=self.nuniques,
            mean=self.mean,
            std=self.stddev,
            min=self.min,
            p25=self.p25,
            p50=self.p50,
            p75=self.p75,
            max=self.max,
            skewness=self.skewness,
            kurtosis=self.kurtosis,
            hist=self.bins
        )



class Stats(object):
    """Constructed on an interator, and an iterator itself, the Stats object reads
    rows from the input iterator, processes the row, and yields it back out"""

    def __init__(self, itr=None):
        self._itr = itr

        self._stats = []
        self._func = None

    def add(self, i, column):
        """Determine the LOM from a ORM Column"""

        if len(self._stats) <= i:
            self._stats.extend([None]*(i-len(self._stats)+1))

        self._stats[i] = StatSet(column)

        # Doing it for every add() is less efficient, but it's insignificant time, and
        # it means we don't have to remember to call a the build phase before processing
        self._func = self.build()

    def build(self):

        parts = []

        for i in range(len(self._stats)):
            if self._stats[i] is not None:
                parts.append("stats[{}].add(row[{}])".format(i,i))

        f = 'def _process_row(stats, row):\n    {}'.format('\n    '.join(parts))

        exec f

        return locals()['_process_row']

    def stats(self):

        return [ (i, self._stats[i]) for i, stat in enumerate(self._stats) if stat]

    def __iter__(self):
        return self

    def process(self, row):
        self._func(self._stats, row)
        return row

    def __next__(self):
        yield self.process(self._itr.next())
