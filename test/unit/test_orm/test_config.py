# -*- coding: utf-8 -*-
from time import time

from six import text_type

import fudge

from ambry.orm.config import Config

from test.test_base import ConfigDatabaseTestBase
from test.factories import ConfigFactory, DatasetFactory


class TestConfig(ConfigDatabaseTestBase):

    def setUp(self):
        super(TestConfig, self).setUp()
        self._my_library = self.library()
        ConfigFactory._meta.sqlalchemy_session = self._my_library.database.session
        DatasetFactory._meta.sqlalchemy_session = self._my_library.database.session

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
    def test_returns_config_repr(self):
        ds = DatasetFactory()
        self._my_library.database.session.commit()
        config1 = ConfigFactory(d_vid=ds.vid)
        repr_str = config1.__repr__()
        self.assertIsInstance(repr_str, text_type)
        self.assertIn(config1.d_vid, repr_str)
        self.assertIn(config1.group, repr_str)
        self.assertIn(config1.key, repr_str)
        self.assertIn(config1.value, repr_str)

    # before_insert tests
    @fudge.patch(
        'ambry.orm.config.Config.before_update')
    def test_populates_id_field(self, fake_before_update):
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
    @fudge.patch('ambry.orm.config.time')
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
