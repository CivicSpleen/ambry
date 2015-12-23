""" Functions and constants for events, used for running codes at points in the build process.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""
import functools

from ambry.util import Constant

PHASE = Constant()
PHASE.INGEST = 'ingest'
PHASE.BUILD = 'build'

TAG = Constant()
TAG.BEFORE_INGEST = 'before_ingest'
TAG.AFTER_INGEST = 'after_ingest'

TAG.BEFORE_SCHEMA = 'before_schema'
TAG.AFTER_SCHEMA = 'after_schema'

TAG.BEFORE_SOURCESCHEMA = 'before_sourceschema'
TAG.AFTER_SOURCESCHEMA = 'after_sourceschema'


TAG.BEFORE_BUILD = 'before_build'
TAG.AFTER_BUILD = 'after_build'

# At the start and end of Bundle.run_stages. Each only runs once for all stages
TAG.BEFORE_RUN = 'before_run'
TAG.AFTER_RUN = 'after_run'

# Before an after Bundle.run_stage. Run once for each stage
TAG.BEFORE_STAGE = 'before_stage'
TAG.AFTER_STAGE = 'after_stage'


def _runable_for_event(f, tag, stage):
    """Loot at the event property for a function to see if it should be run at this stage. """

    if not hasattr(f, '__ambry_event__'):
        return False

    f_tag, f_stage = f.__ambry_event__

    if stage is None:
        stage = 0

    if tag != f_tag or stage != f_stage:
        return False

    return True


def _wrap_for_events(tag, stage):

    if callable(stage):
        # if stage is a callable, it means the code used the decorator without invocation,
        # ie: @before_ingest, instead of @before_ingest, so we have to do the invocation here.
        f = stage
        stage = 1
    else:
        f = None

    def wrap(f):
        f.__ambry_event__ = (tag, stage)

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            self = args[0]
            f(*args, **kwargs)

        return wrapper

    if f:
        return wrap(f)
    else:
        return wrap


def before_ingest(stage=1):
    return _wrap_for_events(TAG.BEFORE_INGEST, stage)


def after_ingest(stage=1):
    return _wrap_for_events(TAG.AFTER_INGEST, stage)


def before_build(stage=1):
    return _wrap_for_events(TAG.BEFORE_BUILD, stage)


def after_build(stage=1):
    return _wrap_for_events(TAG.AFTER_BUILD, stage)


def before_sourceschema(stage=1):
    return _wrap_for_events(TAG.BEFORE_SOURCESCHEMA, stage)


def after_sourceschema(stage=1):
    return _wrap_for_events(TAG.AFTER_SOURCESCHEMA, stage)


def before_schema(stage=1):
    return _wrap_for_events(TAG.BEFORE_SCHEMA, stage)


def after_schema(stage=1):
    return _wrap_for_events(TAG.AFTER_SCHEMA, stage)


def before_run(f):
    return _wrap_for_events(TAG.BEFORE_RUN, 1)(f)


def after_run(f):
    return _wrap_for_events(TAG.AFTER_RUN, 1)(f)


def before_stage(stage=1):
    return _wrap_for_events(TAG.BEFORE_STAGE, stage)


def after_stage(stage=1):
    return _wrap_for_events(TAG.AFTER_STAGE, stage)
