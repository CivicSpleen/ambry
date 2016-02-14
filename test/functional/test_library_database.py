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

    def test_uses_sqlite_library_database_extended_with_test(self):
        # Prepare appropriate config.
        config_path = os.path.join(os.path.dirname(test.support.__file__), 'test-config')
        patched_config = ambry.run.load_config(config_path)
        patched_config.library.database = 'sqlite:///tmp/my-database1.db'

        with patch.object(ambry.run, 'load_config') as fake_load:
            with patch.dict(os.environ, {'AMBRY_TEST_DB': ''}):
                fake_load.return_value = patched_config
                proto = ProtoLibrary()
                self.assertEqual(
                    proto.config.library.database,
                    'sqlite:///tmp/my-database1-test1k.db')

    def test_uses_postgres_library_database_extended_with_test(self):
        config_path = os.path.join(os.path.dirname(test.support.__file__), 'test-config')
        patched_config = ambry.run.load_config(config_path)
        patched_config.library.database = 'postgresql+psycopg2://ambry:secret@127.0.0.1/ambry'

        with patch.object(ambry.run, 'load_config') as fake_load:
            with patch.dict(os.environ, {'AMBRY_TEST_DB': ''}):
                fake_load.return_value = patched_config
                proto = ProtoLibrary()
                self.assertEqual(
                    proto.config.library.database,
                    'postgresql+psycopg2://ambry:secret@127.0.0.1/ambry-test1k')

    def test_uses_dsn_from_environment_variable(self):
        """ Populate AMBRY_TEST_DB with dsn and checks that library uses that dsn. """
        config_path = os.path.join(os.path.dirname(test.support.__file__), 'test-config')
        patched_config = ambry.run.load_config(config_path)
        ambry.run.update_config(patched_config, use_environ=False)
        library_config_dsn = 'sqlite:///tmp/my-library-config-database1.db'
        environment_dsn = 'sqlite:///tmp/my-env-database1.db'
        patched_config.library.database = library_config_dsn

        with patch.object(ambry.run, 'load_config') as fake_load:
            with patch.dict(os.environ, {'AMBRY_TEST_DB': environment_dsn}):
                fake_load.return_value = patched_config
                proto = ProtoLibrary()
                self.assertEqual(
                    proto.config.library.database,
                    environment_dsn)
