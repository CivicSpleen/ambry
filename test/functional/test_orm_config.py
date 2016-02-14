# -*- coding: utf-8 -*-

from sqlalchemy.exc import IntegrityError

from ambry.orm.config import Config

from test.factories import DatasetFactory
from test.proto import TestBase


class Test(TestBase):

    def setUp(self):
        super(self.__class__, self).setUp()
        self._my_library = self.library()
        DatasetFactory._meta.sqlalchemy_session = self._my_library.database.session

    def test_id_generation(self):

        # set some configs
        dataset = DatasetFactory()

        dataset.config.metadata.identity.id = 'd02'
        dataset.config.metadata.identity.version = '0.0.1'
        self._my_library.database.commit()

        # check ids
        query = self._my_library.database.session.query(Config)
        for config in query.filter_by(d_vid=dataset.vid).all():
            self.assertTrue(config.id.startswith('F'))
            self.assertIn(dataset.id[1:], config.id)

    def test_dataset_config_operations(self):

        dataset = DatasetFactory()
        dataset.config.metadata.identity.id = 'd02'
        dataset.config.metadata.identity.version = '0.0.1'
        self._my_library.database.commit()

        # Refresh dataset after commit
        dataset = self._my_library.dataset(dataset.vid)

        self.assertEqual(dataset.config.metadata.identity.id, 'd02')
        self.assertEqual(dataset.config.metadata.identity.version, '0.0.1')

        self.assertEqual(10, len(dataset.config.metadata.identity))

        identity_keys = [
            'subset', 'variation', 'dataset', 'btime', 'source',
            'version', 'bspace', 'type', 'id', 'revision']

        self.assertEqual(
            sorted(identity_keys),
            sorted([v for v in dataset.config.metadata.identity]))

        with self.assertRaises(AttributeError):
            dataset.config.metadata = 'foo'

        try:
            dataset.config.metadata.identity = [1, 2, 3]
        except AssertionError as exc:
            self.assertIn('Dictionary is required', str(exc))

    def test_unique(self):
        """ d_vid, type, group and key are unique together. """
        dataset = DatasetFactory()
        conf1 = Config(
            sequence_id=1, d_vid=dataset.vid, type='metadata',
            group='identity', key='key1', value='value1')
        self._my_library.database.session.add(conf1)
        self._my_library.database.session.commit()

        dupe = Config(
            sequence_id=2, d_vid=dataset.vid, type='metadata',
            group='identity', key='key1', value='value1')
        self._my_library.database.session.add(dupe)
        try:
            self._my_library.database.session.commit()
            raise AssertionError('Dupe unexpectedly saved. It seems unique constraint is broken.')
        except IntegrityError as exc:
            self.assertIn('UNIQUE constraint failed', str(exc))
