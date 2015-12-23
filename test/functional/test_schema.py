# -*- coding: utf-8 -*-

from test.test_base import TestBase


class Test(TestBase):

    def test_schema_basic(self):
        import unicodecsv as csv

        b = self.import_single_bundle('ingest.example.com/headerstypes')
        b.clean()
        self.assertEquals(0,len(b.sources))
        b.sync_in()
        self.assertEquals(7,len(b.sources))

        # Run the first ingestion.
        b.ingest(tables=['types'])

        # Create source tables from the ingested files, then sync out
        # to files. This will create tables for only the tables that
        # got ingested
        b.source_schema()

        print b.library.database.dsn
        print [ st.name for st in b.source_tables]
        self.assertEquals(7,len(b.source_tables))

        self.assertEqual([u'int', u'float', u'string', u'time', u'date'],
                         [ c.dest_header for c in b.dataset.source_table('types1').columns])

        #
        # Modify the source schema files
        rows = []
        with b.source_fs.open('source_schema.csv',encoding='utf8') as f:
            r = csv.reader(f)
            headers = next(r)

            for row in r:
                d = dict(zip(headers, row))
                d['dest_header'] = 'X'+d['source_header']
                rows.append(d)

        # Fails with: TypeError: must be unicode, not str
        # with b.source_fs.open('source_schema.csv', 'w',encoding='utf8') as f:

        path = b.source_fs.getsyspath('source_schema.csv')
        with open(path, 'w') as f:
            w = csv.DictWriter(f,fieldnames=headers)
            w.writeheader()
            for row in rows:
                w.writerow(row)

        b.sync_in()

        self.assertEqual([u'int', u'float', u'string', u'time', u'date'],
                         [ c.source_header for c in b.dataset.source_table('types1').columns])


        b.clean_ingested()
        b.ingest(tables=['types'])

        self.assertEqual([u'int', u'float', u'string', u'time', u'date'],
                         [ c.source_header for c in b.dataset.source_table('types1').columns])

        b.source_schema()

        self.assertEqual([u'int', u'float', u'string', u'time', u'date'],
                         [ c.source_header for c in b.dataset.source_table('types1').columns])
