""""
These are random tests used in development. They aren't meant to be comprehensive or to exercise any specific bugs. """

import unittest

from six import u

from test.test_base import TestBase
from six.moves.urllib.parse import urlparse


class Test(TestBase):

    @unittest.skip("Development Test")
    def test_install(self):
        """Test copying a bundle to a remote, then streaming it back"""
        from boto.exception import S3ResponseError
        b = self.setup_bundle('simple', source_url='temp://')
        l = b._library

        b.sync_in()

        b.run()

        self.assertEqual(1, len(list(l.bundles)))

        p = list(b.partitions)[0]
        p_vid = p.vid

        self.assertEqual(497054, int(sum(row[3] for row in p.stream(skip_header=True))))

        self.assertEqual('build', l.partition(p_vid).location)

        try:
            remote_name, path = b.checkin()
        except S3ResponseError as exc:
            if exc.status == 403:  # Forbidden.
                raise unittest.SkipTest(
                    'Skip S3 error - {}. It seems S3 credentials are not valid.'.format(exc))
            else:
                raise

        print(remote_name, path)

    @unittest.skip("Development Test")
    def test_search(self):
        """Test copying a bundle to a remote, then streaming it back"""
        from ambry.library import new_library

        l = new_library(self.get_rc())

        l.sync_remote('test')

        b = list(l.bundles)[0]
        p = list(b.partitions)[0]

        self.assertEqual(1, len(list(l.bundles)))

        self.assertEqual('remote', l.partition(p.vid).location)

        #self.assertEquals(497054, int(sum(row[3] for row in p.stream(skip_header=True))))
        #self.assertEqual(10000, len(list(p.stream(skip_header=True))))
        #self.assertEqual(10001, len(list(p.stream(skip_header=False))))

        search = l.search

        search.index_library_datasets()

        self.assertEqual([u('d000simple003'), u('p000simple002003')], list(search.list_documents()))

        print(search.search_datasets('d000simple003')[0].vid)

        print(search.search_datasets('Example')[0].vid)

        print(search.search_datasets('2010'))

    @unittest.skip("Development Test")
    def test_sequence(self):
        from ambry.orm import Database

        conf = self.get_rc()

        if 'database' in conf.dict and 'postgresql-test' in conf.dict['database']:
            dsn = conf.dict['database']['postgresql-test']
            parsed_url = urlparse(dsn)
            db_name = parsed_url.path.replace('/', '')
            self.postgres_dsn = parsed_url._replace(path='postgres').geturl()
            self.postgres_test_db = '{}_test_db1ae'.format(db_name)
            self.postgres_test_dsn = parsed_url._replace(path=self.postgres_test_db).geturl()

        db = Database(self.postgres_test_dsn)

        for i in range(10):

            print(b.dataset.next_number('foobar'))

