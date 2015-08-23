# coding: utf-8
"""

Computing stats on the fly for data written to a partition

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from collections import Counter

from livestats import livestats
from six import iteritems, iterkeys

from ambry.util import Constant

from .pipeline import PipelineError


def text_hist(nums, ascii=False):

    if ascii:
        parts = u' _.,,-=T#'
    else:
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


        try:
            # Try using column as an orm.Column
            self.is_gvid = bool("gvid" in column.name)
            self.is_year = bool("year" in column.name)
            self.is_time = column.type_is_time()
            self.is_date = column.type_is_date()

            self.flags = " G"[self.is_gvid] + " Y"[self.is_year] + " T"[self.is_time] + " D"[self.is_date]

            if column.is_primary_key or self.is_year or self.is_time or self.is_date:
                lom = StatSet.LOM.ORDINAL
            elif column.type_is_text() or self.is_gvid:
                lom = StatSet.LOM.NOMINAL
            elif column.type_is_number():
                lom = StatSet.LOM.INTERVAL

            self.column_name = column.name
        except AttributeError as e:
            # Nope, assume it is a string

            self.is_gvid = self.is_year = self.is_time = self.is_date = False
            lom = StatSet.LOM.ORDINAL
            self.column_name = column
            self.flags = None

        self.lom = lom
        self.n = 0
        self.counts = Counter()
        self.size = None
        self.stats = livestats.LiveStats([0.25, 0.5, 0.75])  # runstats.Statistics()

        self.bin_min = None
        self.bin_max = None
        self.bin_width = None
        self.bin_primer_count = 5000  # how many points to collect before creating hist bins
        self.num_bins = 16
        self.bins = [0] * self.num_bins

    def add(self, v):
        from math import sqrt

        self.n += 1

        try:
            unival = unicode(v)
        except UnicodeError:
            unival = v.decode('ascii', 'ignore')

        self.size = max(self.size, len(unival))

        if self.lom == self.LOM.NOMINAL or self.lom == self.LOM.ORDINAL:
            if self.is_time or self.is_date:
                self.counts[unival] += 1
            else:
                self.counts[unival] += 1

        elif self.lom == self.LOM.INTERVAL or self.lom == self.LOM.RATIO:

            # To build the histogram, we need to collect counts, but would rather
            # not collect all of the values. So, collect the first 5K, then use that
            # to determine the 4sigma range of the histogram.
            # HACK There are probably a lot of 1-off errors in this

            if self.n < self.bin_primer_count:
                self.counts[unival] += 1

            elif self.n == self.bin_primer_count:
                # If less than 1% are unique, assume that this number is actually an ordinal
                if self.nuniques < (self.bin_primer_count/100):
                    self.lom = self.LOM.ORDINAL
                    self.stats = livestats.LiveStats()
                else:
                    self.bin_min = self.stats.mean() - sqrt(self.stats.variance()) * 2
                    self.bin_max = self.stats.mean() + sqrt(self.stats.variance()) * 2
                    self.bin_width = (self.bin_max - self.bin_min) / self.num_bins

                    for v, count in iteritems(self.counts):
                        if v >= self.bin_min and v <= self.bin_max:
                            bin_ = int((v - self.bin_min) / self.bin_width)
                            self.bins[bin_] += count

                self.counts = Counter()

            elif self.n > self.bin_primer_count and v >= self.bin_min and v <= self.bin_max:
                bin_ = int((v - self.bin_min) / self.bin_width)
                self.bins[bin_] += 1

            try:
                self.stats.add(float(v))
            except (ValueError, TypeError):
                self.counts[unival] += 1
        else:
            assert False, "Really should be one or the other ... "

    @property
    def uniques(self):
        return list(self.counts)

    @property
    def nuniques(self):
        return len(list(self.counts.items()))

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
            return self.stats.quantiles()[1][1]
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
        from collections import OrderedDict

        try:
            skewness = self.skewness
            kurtosis = self.kurtosis
        except ZeroDivisionError:
            skewness = kurtosis = float('nan')

        return OrderedDict([
            ('name', self.column_name),
            ('flags', self.flags),
            ('lom', self.lom),
            ('count', self.n),
            ('nuniques', self.nuniques),
            ('mean', self.mean),
            ('std', self.stddev),
            ('min', self.min),
            ('p25', self.p25),
            ('p50', self.p50),
            ('p75', self.p75),
            ('max', self.max),
            ('skewness', skewness),
            ('kurtosis', kurtosis),
            ('hist', self.bins),
            ('uvalues', dict(self.counts.most_common(100)))]
        )


class Stats(object):
    """ Stats object reads rows from the input iterator, processes the row, and yields it back out"""

    def __init__(self, table):

        self._table = table
        self._stats = {}
        self._func = None
        self._func_code = None
        self.headers = None

        for c in self._table.columns:
            self.add(c, build=False)

        self._func, self._func_code = self.build()

    def add(self, column, build=True):
        """Determine the LOM from a ORM Column"""

        # Try it as an orm.column, otherwise try to look up in a table,
        # otherwise, as a string
        try:
            column.name
            self._stats[column.name] = StatSet(column)
        except AttributeError:

            if self.table:
                column = self.table.column(column)

                self._stats[column.name] = StatSet(column)
            else:
                self._stats[column] = StatSet(column)

        # Doing it for every add() is less efficient, but it's insignificant time, and
        # it means we don't have to remember to call the build phase before processing
        if self.build:
            self._func, self._func_code = self.build()

    def build(self):

        parts = []

        for name in iterkeys(self._stats):
            if self._stats[name] is not None:
                parts.append("stats['{name}'].add(row['{name}'])".format(name=name))

        if not parts:
            error_msg = 'Did not get any stats variables for table {}. Was add() or init() called first?'\
                .format(self.table.name)
            raise PipelineError(error_msg)

        code = 'def _process_row(stats, row):\n    {}'.format('\n    '.join(parts))

        exec(code)

        f = locals()['_process_row']

        return f, code

    def stats(self):
        return [(name, self._stats[name]) for name, stat in iteritems(self._stats)]

    def process(self, row):
        try:
            self._func(self._stats, row)
        except KeyError:
            raise KeyError(
                'Failed to find key in row. headers = "{}", code = "{}" '.format(self.headers, self._func_code))

        return row

    def process_header(self, row):
        """ """

        self.headers = row

        return row

    def process_body(self, row):

        self.process(dict(list(zip(self.headers, row))))

        return row

    def __str__(self):
        from tabulate import tabulate

        rows = []

        for name, stats in iteritems(self._stats):
            stats_dict = stats.dict
            del stats_dict["uvalues"]
            stats_dict["hist"] = text_hist(stats_dict["hist"], True)
            if not rows:
                rows.append(list(stats_dict.keys()))

            rows.append(list(stats_dict.values()))
        if rows:
            return 'Statistics \n' + str(tabulate(rows[1:], rows[0], tablefmt="pipe"))
        else:
            return 'Statistics: None \n'
