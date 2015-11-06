"""Base class for testing and test support functions

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import unittest
from ambry.util import Constant
import functools

PHASE = Constant()
PHASE.INGEST = 'ingest'
PHASE.BUILD = 'build'


TAG = Constant()
TAG.BEFORE_INGEST = 'before_ingest'
TAG.AFTER_INGEST = 'after_ingest'

TAG.BEFORE_BUILD = 'before_build'
TAG.AFTER_BUILD = 'after_build'

# At the start and end of Bundle.run_stages. Each only runs once for all stages
TAG.BEFORE_RUN = 'before_run'
TAG.AFTER_RUN = 'after_run'

# Before an after Bundle.run_stage. Run once for each stage
TAG.BEFORE_STAGE = 'before_stage'
TAG.AFTER_STAGE = 'after_stage'

import logging
import sys

class BundleTest(unittest.TestCase):

    bundle = None
    library = None

    def setUp(self):
        # noinspection PyUnresolvedReferences
        from ambry.build import bundle, library # From the codes loaded in the bundles test.py file

        self.bundle = bundle()
        self.library = library()

        self.logging_handler = logging.StreamHandler(sys.stdout)
        self.bundle.logger.addHandler(self.logging_handler)

    def tearDown(self):
        self.bundle.logger.removeHandler(self.logging_handler)

def _identity(obj):
    return obj

def _runable_test(f, tag, stage):

    if not hasattr(f, '__ambry_test__'):
        return False

    f_tag, f_stage = f.__ambry_test__

    if not stage:
        stage = 1

    #print f, f_tag, tag,  f_stage, stage

    if tag != f_tag or stage != f_stage:
        return False

    return True

def _wrap_test(tag, stage):
    def wrap(f):
        f.__ambry_test__ = (tag, stage)

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            self = args[0]
            f(*args, **kwargs)

        return wrapper

    return wrap


def before_ingest(stage=1):
    return _wrap_test(TAG.BEFORE_INGEST, stage)

def after_ingest(stage=1):
    return _wrap_test(TAG.AFTER_INGEST, stage)

def before_build(stage=1):
    return _wrap_test(TAG.BEFORE_BUILD, stage)

def after_build(stage=1):
    return _wrap_test(TAG.AFTER_BUILD, stage)

def before_run(f):
    return _wrap_test(TAG.BEFORE_RUN,1)(f)

def after_run(f):
    return _wrap_test(TAG.AFTER_RUN,1)(f)

def before_stage(stage=1):
    return _wrap_test(TAG.BEFORE_STAGE, stage)

def after_stage(stage=1):
    return _wrap_test(TAG.AFTER_STAGE, stage)