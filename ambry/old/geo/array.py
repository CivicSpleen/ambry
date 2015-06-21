"""Array operations."""

# Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

from numpy import *
from ambry.geo import Point


def std_norm(a):
    """Normalize to +-4 sigma on the range 0 to 1"""

    mean = a.mean()
    std = a.std()

    o = ((a - mean) / std).clip(-4, 4)  # Def of z-score
    o += 4
    o /= 8

    try:
        o.set_fill_value(0)
    except AttributeError:
        # If it isn't a masked array
        pass

    return o


def unity_norm(a):
    """scale to the range 0 to 1."""

    range = a.max() - a.min()
    o = (a - a.min()) / range

    try:
        o.set_fill_value(0)
    except AttributeError:
        # If it isn't a masked array
        pass

    return o


def statistics(a):

    from numpy import sum as asum

    r = ("Min, Max: {},{}\n".format(amin(a), amax(a)) +
         "Range    : {}\n".format(ptp(a)) +
         "Average  : {}\n".format(average(a)) +
         "Mean     : {}\n".format(mean(a)) +
         "Median   : {}\n".format(median(a)) +
         "StdDev   : {}\n".format(std(a)) +
         "Sum      : {}\n".format(asum(a))
         )

    try:
        # Try the method for masked arrays. The other method will not
        # respect the mask
        r += "Histogram:{}".format(histogram(a.compressed())
                                   [0].ravel().tolist())
    except:
        r += "Histogram: {}".format(histogram(a)[0].ravel().tolist())

    return r


def add(s, v, m):
    return v + (m * s)


def apply_copy(kernel, a, func=add, nodata=None, mult=True):
    """For all cells in a, or all nonzero cells, apply the kernel to a new
    output array."""
    from itertools import izip

    o = zeros_like(a)

    #
    #  Generate indices,
    if nodata == 0:
        indx = nonzero(a)
        z = izip(indx[0], indx[1])

    elif nodata is not None:
        indx = nonzero(a != nodata)
        z = izip(indx[0], indx[1])

    else:
        z = ndindex(a.shape)

    for row, col in z:
        kernel.apply(o, Point(col, row), func, a[row, col])

    return o
