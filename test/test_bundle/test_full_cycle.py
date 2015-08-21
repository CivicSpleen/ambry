""""
Test large-scale builds of all of the test bundles, installing them to a library, and performing other operations.
"""

from test.test_base import TestBase
import unittest

from ambry.bundle import Bundle

class Test(TestBase):

    def test_build_all(self):
        from test import bundles
        import os
        from os.path import join, dirname
        from fs.opener import fsopendir
        from ambry.library import new_library
        import yaml
        from ambry.orm.exc import NotFoundError

        rc = self.get_rc()

        l = new_library(rc)

        source_dir = fsopendir('temp://')
        base_dir = fsopendir(join(dirname(bundles.__file__), 'example.com'))

        for f in base_dir.walkfiles():
            source_dir.makedir(os.path.dirname(f), recursive = True, allow_recreate=True)
            source_dir.setcontents(f,base_dir.getcontents(f))

        for f in source_dir.walkfiles(wildcard='bundle.yaml'):

            config = yaml.load(base_dir.getcontents(f))

            if not config:
                continue

            bid = config['identity']['id']

            try:
                b = l.bundle(bid)
            except NotFoundError:
                b = l.new_from_bundle_config(config)

            b.set_file_system(source_url=os.path.dirname(source_dir.getsyspath(f)))

            b.sync()

            print "Loaded bundle: {}".format(b.identity.fqname)

        for bi in l.bundles:

            if 'casters' in bi.identity.vid or 'process' in bi.identity.vid:
                continue # This one has caster errors in it.

            print 'Running bundle', str(bi.identity.fqname)
            b = l.bundle(bi.identity.vid)
            b.sync_in()
            b = b.cast_to_subclass()
            b.run()
            b.metadata.about.remote = 'test'
            b.checkin()

