""""
These are random tests used in development. They aren't meant to be comprehensive or to exercise any specific bugs. """

import unittest

from six import u

from test.proto import TestBase


class Test(TestBase):

    @unittest.skip('Development Test')
    def test_search(self):
        """Test copying a bundle to a remote, then streaming it back"""
        l = self.library()

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
