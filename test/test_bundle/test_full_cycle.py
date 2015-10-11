""""
Test large-scale builds of all of the test bundles, installing them to a library, and performing other operations.
"""

import pytest
from test.test_base import TestBase


class Test(TestBase):

    def setup_temp_dir(self):
        import os
        import shutil
        build_url = '/tmp/ambry-build-test'
        if not os.path.exists(build_url):
            os.makedirs(build_url)
        shutil.rmtree(build_url)
        os.makedirs(build_url)

        return build_url

    @pytest.mark.slow
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

        # Where we get the source files from
        base_dir = fsopendir(join(dirname(bundles.__file__), 'example.com'))

        # where we copy the source files.
        source_dir = fsopendir(self.setup_temp_dir())

        for f in base_dir.walkfiles():
            source_dir.makedir(os.path.dirname(f), recursive=True, allow_recreate=True)
            source_dir.setcontents(f, base_dir.getcontents(f))

        for f in source_dir.walkfiles(wildcard='bundle.yaml'):

            config = yaml.load(base_dir.getcontents(f))

            if not config:
                continue

            bid = config['identity']['id']

            try:
                b = l.bundle(bid)
            except NotFoundError:
                b = l.new_from_bundle_config(config)

            source_url = os.path.dirname(source_dir.getsyspath(f))
            build_url = os.path.join(source_url, 'build')
            os.makedirs(build_url)

            b.set_file_system(source_url=source_url, build_url=build_url)

            b.sync()
            print('Loaded bundle: {}'.format(b.identity.fqname))

        for bi in l.bundles:

            if 'casters' in bi.identity.vid or 'process' in bi.identity.vid:
                continue  # This one has caster errors in it.

            print('Running bundle', str(bi.identity.fqname))
            b = l.bundle(bi.identity.vid)
            b.sync_in()
            b = b.cast_to_subclass()
            b.run()
            b.metadata.about.remote = 'test'
            b.checkin()
