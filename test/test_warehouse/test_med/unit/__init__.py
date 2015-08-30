# -*- coding: utf-8 -*-

import os

from sqlalchemy import Column as SAColumn, Integer, String, Date, DateTime

from ambry.util import AttrDict

from test.test_base import TestBase

TEST_FILES_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), '../', 'files'))


class BaseMEDTest(TestBase):

    def _get_fake_datetime_partition(self, vid):
        """ Creates fake partition with int, date, datetime fields. """
        table = AttrDict(
            columns=[
                SAColumn('rowid', Integer, primary_key=True),
                SAColumn('col1', Date),
                SAColumn('col2', DateTime)])
        datafile = AttrDict(
            syspath=os.path.join(TEST_FILES_DIR, 'rowid_int_col1_date_col2_datetime_100_rows_gzipped.msg'))
        partition = AttrDict(vid=vid, table=table, datafile=datafile)
        return partition

    def _get_fake_partition(self, vid):
        """ Creates fake partition from test msgpack file with int, int, str fields. """
        table = AttrDict(
            columns=[
                SAColumn('rowid', Integer, primary_key=True),
                SAColumn('col1', Integer),
                SAColumn('col2', String(8))])
        datafile = AttrDict(
            syspath=os.path.join(TEST_FILES_DIR, 'rowid_int_col1_int_col2_str_100_rows_gzipped.msg'))
        partition = AttrDict(vid=vid, table=table, datafile=datafile)
        return partition
