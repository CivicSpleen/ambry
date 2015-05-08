"""
Created on Jun 30, 2012

@author: eric
"""

import unittest
import os.path
import logging

from test_base import TestBase
from testbundle.bundle import Bundle
from ambry.run import get_runconfig
from ambry.run import RunConfig
from ambry.source.repository import new_repository
import ambry.util


global_logger = ambry.util.get_logger(__name__)
global_logger.setLevel(logging.DEBUG)


class Test(TestBase):
    def setUp(self):
        import testbundle.bundle
        import shutil
        import os

        self.bundle_dir = os.path.dirname(testbundle.bundle.__file__)
        self.rc = get_runconfig((os.path.join(self.bundle_dir, 'source-test-config.yaml'),
                                 os.path.join(self.bundle_dir, 'bundle.yaml'),
                                 RunConfig.USER_ACCOUNTS))

        self.copy_or_build_bundle()

        bundle = Bundle()

        self.source_save_dir = str(self.rc.group('filesystem').root) + '-source'

        self.setup_source_dir()

        print "Deleting: {}".format(self.rc.group('filesystem').root)
        ambry.util.rm_rf(self.rc.group('filesystem').root)

        bdir = os.path.join(self.rc.sourcerepo.dir, 'testbundle')

        pats = shutil.ignore_patterns('build', 'build-save', '*.pyc', '.git', '.gitignore', '.ignore', '__init__.py')

        print "Copying test dir tree to ", bdir
        shutil.copytree(bundle.bundle_dir, bdir, ignore=pats)

        # Import the bundle file from the directory
        from ambry.run import import_file

        rp = os.path.realpath(os.path.join(bdir, 'bundle.py'))
        mod = import_file(rp)

        dir_ = os.path.dirname(rp)
        self.bundle = mod.Bundle(dir_)

        print self.bundle.bundle_dir

    def setup_source_dir(self):

        l = self.get_library()
        s = l.source
        s.base_dir = self.source_save_dir

        if not os.path.exists(s.base_dir):
            print "Cloning source to ", s.base_dir
            s.clone("https://github.com/clarinova-data/example.com-random-ambry.git")
            s.clone("https://github.com/clarinova-data/example.com-altdb-orig-429e-dbundle.git")
            s.clone("https://github.com/clarinova-data/example.com-segmented-orig-429e-dbundle.git")

        else:
            print "Source is already cloned at", s.base_dir

    def tearDown(self):
        pass

    def get_library(self, name='default'):
        """Clear out the database before the test run"""
        from ambry.library import new_library

        config = self.rc.library(name)

        l = new_library(config, reset=True)

        return l

    def testBasic(self):
        repo = new_repository(self.rc.sourcerepo('clarinova.data'))

        repo.bundle_dir = self.bundle.bundle_dir

        repo.delete_remote()
        import time

        time.sleep(3)
        repo.init_descriptor()
        repo.init_remote()

        repo.push(repo.service.user, repo.service.password)

    def testSync(self):
        for repo in self.rc.sourcerepo.list:
            print repo.service.list()

    def build_bundle(self, s, l, term):

        bundle = s.resolve_bundle(term)
        bundle.library = l
        bundle.clean()
        bundle.pre_prepare()
        bundle.prepare()
        bundle.post_prepare()
        bundle.pre_build()
        bundle.build
        bundle.post_build()
        bundle.install()

    def test_source_get(self):
        from ambry.util import rm_rf
        import shutil
        from ambry.orm import Dataset

        l = self.get_library()

        s = l.source
        print 'Source Dir: ', s.base_dir, l.database.dsn

        rm_rf(s.base_dir)
        shutil.copytree(self.source_save_dir, s.base_dir)

        s.sync_source()

        snames = {'example.com-altdb-orig', 'example.com-random', 'example.com-segmented-orig'}

        self.assertEquals(snames, {ident.sname for ident in s._dir_list().values()})
        self.assertEquals(snames, {ident.sname for ident in s.list().values()})

        self.build_bundle(s, l, "example.com-random-0.0.1")

        s.sync_repos()

        codes = {ident.vname: ident.locations.codes for ident in l.list().values()}

        for key, ident in sorted(l.list().items(), key=lambda x: x[1].vname):
            print str(ident.locations), ident.vname

        self.assertIn(Dataset.LOCATION.SOURCE, codes['example.com-random-0.0.1'])
        self.assertIn(Dataset.LOCATION.LIBRARY, codes['example.com-random-0.0.1'])
        self.assertNotIn(Dataset.LOCATION.UPSTREAM, codes['example.com-random-0.0.1'])

        self.assertIn(Dataset.LOCATION.SOURCE, codes['example.com-segmented-orig-0.1.1'])

        l.push()  # Also stores upstream ref in Files and Datasets

        codes = {ident.vname: ident.locations.codes for ident in l.list().values()}

        for key, ident in sorted(l.list().items(), key=lambda x: x[1].vname):
            print str(ident.locations), ident.vid, ident.fqname

        self.assertIn(Dataset.LOCATION.UPSTREAM, codes['example.com-random-0.0.1'])


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite


if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())