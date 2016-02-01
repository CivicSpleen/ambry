# -*- coding: utf-8 -*-

from test.test_base import TestBase


class Test(TestBase):



    def test_headerstypes(self):
        b = self.import_single_bundle('ingest.example.com/headerstypes')
        try:
            b.ingest()
        finally:
            b.close()

    def test_basic(self):
        b = self.import_single_bundle('ingest.example.com/basic')
        try:
            b.ingest()
        finally:
            b.clean_all()
            b.close()

    def test_stages(self):
        b = self.import_single_bundle('ingest.example.com/stages')
        try:
            b.run_stages()
        finally:
            b.clean_all()
            b.close()

    def test_casters(self):
        b = self.import_single_bundle('build.example.com/casters')
        try:
            b.ingest()
            b.source_schema()
            b.schema()
            b.build()
        finally:
            b.clean_all()
            b.close()

    def test_coverage(self):
        b = self.import_single_bundle('build.example.com/coverage')
        try:
            b.ingest()
            b.source_schema()
            b.schema()
            b.build()
        finally:
            b.clean_all()
            b.close()

    def test_generators(self):
        b = self.import_single_bundle('build.example.com/generators')
        try:
            b.run()
        finally:
            b.clean_all()
            b.close()

