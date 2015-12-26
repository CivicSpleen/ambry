# -*- coding: utf-8 -*-
from time import time

from six import text_type

try:
    # py2, mock is external lib.
    from mock import patch, Mock
except ImportError:
    # py3, mock is included
    from unittest.mock import patch, Mock

from ambry.orm.config import Config

from test.test_base import TestBase
from test.factories import ConfigFactory, DatasetFactory


class TestConfig(TestBase):

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
    @patch('ambry.orm.config.Config.before_update')
    def test_populates_id_field(self, fake_before_update):
        ds = DatasetFactory()
        config1 = ConfigFactory.build(d_vid=ds.vid)
        assert config1.id is None

        mapper = Mock()
        conn = Mock()
        Config.before_insert(mapper, conn, config1)

        self.assertIsNotNone(config1.id)
        self.assertTrue(config1.id.startswith('Fds'))
        self.assertEqual(len(fake_before_update.mock_calls), 1)

    # before_update tests
    @patch('ambry.orm.config.time')
    def test_updates_modified_field(self, fake_time):
        FAKE_TIME = 111111
        fake_time.side_effect = [time(), FAKE_TIME]

        ds = DatasetFactory()
        config1 = ConfigFactory(d_vid=ds.vid)
        config1.value = 'new-value'
        assert config1.modified != FAKE_TIME

        mapper = Mock()
        conn = Mock()
        Config.before_update(mapper, conn, config1)
        self.assertEqual(config1.modified, FAKE_TIME)
        self.assertEqual(len(fake_time.mock_calls), 2)
