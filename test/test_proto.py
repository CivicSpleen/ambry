import unittest


class TestProto(unittest.TestCase):

    def _test_basic(self, dsn):

        from proto import ProtoLibrary

        pl = ProtoLibrary(dsn)

        l = pl.init_library()

        self.assertTrue(len(list(l.bundles)) >= 4)
        self.assertEqual('ingested', l.bundle('ingest.example.com-basic').state)
        self.assertEqual('finalized', l.bundle('build.example.com-coverage').state)
        self.assertEqual('finalized', l.bundle('build.example.com-generators').state)
        self.assertEqual('finalized', l.bundle('build.example.com-casters').state)

        l.close()

        l = pl.init_library(use_proto=False)
        self.assertEqual(0, len(list(l.bundles)))

    def test_basic_sqlite(self):
        self._test_basic(dsn=None)

    def test_basic_pq(self):
        self._test_basic(dsn='postgres://test:Ml6GQjLGdPESxlVe@192.168.1.30:33094/test')


if __name__ == '__main__':
    unittest.main()
