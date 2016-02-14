# -*- coding: utf-8 -*-
import os
from unittest import TestCase

import ambry.run
from test.proto import ProtoLibrary
import test.support

try:
    # py2, mock is external lib.
    from mock import patch
except ImportError:
    # py3, mock is included
    from unittest.mock import patch


class Test(TestCase):

    def _assert_dsn(self, states):
        """
        Args:
            states (dict): dictionary with environment variable, initial and final state of the config.
                Example: {
                    'env.AMBRY_TEST_DB': '',
                    'config.library.database': {
                        'initial': 'sqlite:///tmp/my-database1.db',
                        'final': 'sqlite:///tmp/my-database1-test1k.db'
                    }
                }
        """

        config_path = os.path.join(os.path.dirname(test.support.__file__), 'test-config')
        patched_config = ambry.run.load_config(config_path)
        patched_config.library.database = states['config.library.database']['initial']

        with patch.object(ambry.run, 'load_config') as fake_load:
            with patch.dict(os.environ, {'AMBRY_TEST_DB': states['env.AMBRY_TEST_DB']}):
                fake_load.return_value = patched_config
                proto = ProtoLibrary()
                self.assertEqual(
                    proto.config.library.database,
                    states['config.library.database']['final'])

    def test_uses_sqlite_library_database_extended_with_test(self):
        states = {
            'env.AMBRY_TEST_DB': '',
            'config.library.database': {
                'initial': 'sqlite:///tmp/my-database1.db',
                'final': 'sqlite:///tmp/my-database1-test1k.db'
            }
        }
        self._assert_dsn(states)

    def test_uses_postgres_library_database_extended_with_test(self):
        states = {
            'env.AMBRY_TEST_DB': '',
            'config.library.database': {
                'initial': 'postgresql+psycopg2://ambry:secret@127.0.0.1/ambry',
                'final': 'postgresql+psycopg2://ambry:secret@127.0.0.1/ambry-test1k'
            }
        }
        self._assert_dsn(states)

    def test_uses_dsn_from_environment_variable(self):
        """ Populate AMBRY_TEST_DB with dsn and checks that library uses that dsn. """
        env_dsn = 'sqlite:///tmp/my-env-database1.db'
        states = {
            'env.AMBRY_TEST_DB': env_dsn,
            'config.library.database': {
                'initial': 'sqlite:///tmp/my-library-config-database1.db',
                'final': env_dsn
            }
        }
        self._assert_dsn(states)
