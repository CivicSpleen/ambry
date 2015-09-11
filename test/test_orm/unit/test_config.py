# -*- coding: utf-8 -*-
import unittest
from time import time

from sqlalchemy.exc import IntegrityError

from six import binary_type

import fudge

from ambry.orm.config import Config

from test.test_base import TestBase
from test.test_orm.factories import ConfigFactory, DatasetFactory


class TestConfig(TestBase):

    def setUp(self):
        super(TestConfig, self).setUp()
        db = self.new_database()
        ConfigFactory._meta.sqlalchemy_session = db.session
        DatasetFactory._meta.sqlalchemy_session = db.session

    # dict tests
    def test_returns_dictionary_representation_of_the_config(self):
        ds = DatasetFactory()
        config1 = ConfigFactory(d_vid=ds.vid)
        fields = [
            'id',
            'sequence_id',
            'dataset',
            'd_vid',
            'type',
            'group',
            'key',
            'value',
            'modified',
            'children',
            'parent_id',
            'parent']

        self.assertEqual(sorted(fields), sorted(config1.dict))

        for field in fields:
            self.assertEqual(getattr(config1, field), config1.dict[field])

    # __repr__ tests
    def test_returns_config_as_string(self):
        ds = DatasetFactory()
        config1 = ConfigFactory(d_vid=ds.vid)
        repr_str = config1.__repr__()
        self.assertIsInstance(repr_str, binary_type)
        self.assertIn(config1.d_vid, repr_str)
        self.assertIn(config1.group, repr_str)
        self.assertIn(config1.key, repr_str)
        self.assertIn(config1.value, repr_str)

    # before_insert tests
    @fudge.patch(
        'ambry.orm.config.next_sequence_id',
        'ambry.orm.config.Config.before_update')
    def test_populates_id_field(self, fake_next, fake_before_update):
        fake_next.expects_call().returns(1)
        fake_before_update.expects_call()
        ds = DatasetFactory()
        config1 = ConfigFactory.build(d_vid=ds.vid)
        assert config1.id is None

        mapper = fudge.Fake().is_a_stub()
        conn = fudge.Fake().is_a_stub()
        Config.before_insert(mapper, conn, config1)

        self.assertIsNotNone(config1.id)
        self.assertTrue(config1.id.startswith('Fds'))

    # before_update tests
    @fudge.patch(
        'ambry.orm.config.time')
    def test_updates_modified_field(self, fake_time):
        FAKE_TIME = 111111
        fake_time\
            .expects_call().returns(time())\
            .next_call().returns(FAKE_TIME)

        ds = DatasetFactory()
        config1 = ConfigFactory(d_vid=ds.vid)
        config1.value = 'new-value'
        assert config1.modified != FAKE_TIME

        mapper = fudge.Fake().is_a_stub()
        conn = fudge.Fake().is_a_stub()
        Config.before_update(mapper, conn, config1)
        self.assertEqual(config1.modified, FAKE_TIME)
