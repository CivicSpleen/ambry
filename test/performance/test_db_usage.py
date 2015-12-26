# -*- coding: utf-8 -*-

import unittest

from sqlalchemy import event

from ambry.metadata.proptree import get_or_create
from ambry.metadata.schema import Top
from ambry.orm import Config

from test.factories import DatasetFactory
from test.test_base import TestBase


class DbUsageTreeFromDatabaseTest(TestBase):

    def setUp(self):
        super(self.__class__, self).setUp()
        self.db = self.library().database
        DatasetFactory._meta.sqlalchemy_session = self.db.session
        self.dataset = DatasetFactory()

    # helpers
    def _create_db_tree(self, configs):
        """ Creates tree in the database.

        Args:
            configs (list of tuples): first element of tuple is key of the config, second
                is value of the config.

        Examples:
            _create_db_tree(('names.name', 'the-name'))
        """
        db = self.db
        dataset = self.dataset
        top_config = Config(
            d_vid=dataset.vid, parent=None, type='metadata', sequence_id=1)
        db.session.add(top_config)
        db.session.commit()
        counter = 1

        for key, value in configs:
            counter += 1
            group_keys = key.split('.')[:-1]
            value_key = key.split('.')[-1]
            parent = top_config

            # create all groups
            for group in group_keys:
                cfg_instance, created = get_or_create(
                    db.session, Config,
                    d_vid=dataset.vid, key=group, group=group,
                    parent=parent, type='metadata',
                    sequence_id=counter)
                parent = cfg_instance

            # populate value
            counter += 1
            value_config = Config(
                d_vid=dataset.vid, key=value_key, value=value,
                parent=parent, type='metadata', sequence_id=counter)
            db.session.add(value_config)
            db.session.commit()

    @unittest.skip('FIXME: UNIQUE constraint failed')
    def test_uses_exactly_two_db_hit_to_build_config(self):

        # create appropriate tree in the database
        configs = [
            ('names.fqname', self.dataset.fqname),
            ('names.name', self.dataset.name),
            ('names.vid', self.dataset.vid),
            ('names.vname', self.dataset.vname),

            # about
            ('about.access', 'restricted'),
            ('about.footnote', None),
            ('about.grain', 'hospital')]

        self._create_db_tree(configs)

        # now collect all queries
        queries = []

        def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            queries.append((statement, parameters))
        event.listen(self.db.engine, 'before_cursor_execute', before_cursor_execute)

        # build from db
        top = Top()
        top.build_from_db(self.dataset)
        expected_queries = [
            'dataset retrieve',
            'all configs retrieve while cache building']
        self.assertEqual(len(expected_queries), len(queries))
