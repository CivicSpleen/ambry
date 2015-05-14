# -*- coding: utf-8 -*-
import inspect


def assert_spec(fn, expected_args):
    """ Matches function arguments against the `expected_args`. Raises AssertionError on
        mismatch.
    """
    fn_args = inspect.getargspec(fn).args
    msg = '{} function requires {} args, but you expect {}'\
        .format(fn, fn_args, expected_args)
    assert fn_args == expected_args, msg
