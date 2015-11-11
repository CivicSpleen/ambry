"""Pipes, pipe segments and piplines, for flowing data from sources to partitions.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from .types import transform_generator

def test_exception(exception, bundle):
    print(exception)
    return None

@transform_generator
def test_tg(a,b,c, pipe, source, bundle):

    def _test_tg(v, row, **kwargs):

        print(v, a, b, c, row[0])
        return v

    return _test_tg


def test_transform_v(v):

    print(v)
    return v


def test_transform_vr(v, row):

    print(v, row[0])
    return v

def test_transform_vrh(v, row, header_d):

    print(v, row[0], header_d)
    return v