# -*- coding: utf-8 -*-

from test.proto import TestBase


class Test147(TestBase):

    def test_sequence_number_conflicts(self):

        b = self.import_single_bundle('ingest.example.com/stages')
        b.clean_except_files()  # Clean objects, but leave the import files
        b.sync_objects_in()  # Sync from file records to objects.
        b.commit()

        b.run()

        # Error happens on the section time.
        b = self.import_single_bundle('ingest.example.com/stages', False)
        b.clean_except_files()  # Clean objects, but leave the import files
        b.sync_objects_in()  # Sync from file records to objects.
        b.commit()

        # print [(st.sequence_id, st.vid) for st in b.dataset.source_tables]

        # AssertionError: A conflicting state is already present in the identity map for key
        # (<class 'ambry.orm.dataset.Dataset'>, (u'duamFSJzB5001',))
        b.run()
