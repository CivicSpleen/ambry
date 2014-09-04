"""Services for publishing to differentn destinations"""

from ..util import memoize
import os

class InstallResult(object):
    """Returned form the publish() calls to hold references to the main URLs or paths in the publication"""

    def __init__(self, manifest, index):
        self.index = index
        self.manifest = manifest
        self.warehouse = manifest.warehouse

    def _repr_html_(self):
        return """
<table>
<tr><td>Directory</td><td>{base_dir}</td></tr>
<tr><td>Documentation</td><td>{index}</td></tr>
<tr><td>Warehouse</td><td>{warehouse}</td></tr>
</table>""".format(warehouse=self.warehouse.database.dsn, index=self.index, uid=self.manifest.uid, base_dir = self.manifest.base_dir)


class ManifestPublisher(object):
    pass

    def __init__(self,  manifest, logger):
        from ..cache import new_cache

        self._logger = logger
        self.m = manifest
        self.working_cache = new_cache(self.m.abs_work_dir) # Where the installed manifest files are

    @property
    @memoize
    def logger(self):
        """Return the logger if it was defined, or a dummy logger if not"""
        if self._logger:
            return self._logger

        # return a dummy logger that does nothing.

        class DummyObject():
            def __init__(self):
                pass

            def __getattr__(self, name):
                return (lambda *x: None)

        return DummyObject()

class CachePublisher(ManifestPublisher):

    """Publish to an Ambry cache object, with ACL support for S3"""

    def __init__(self, manifest, cache, logger=None):
        from ..cache.filesystem import FsCompressionCache

        super(CachePublisher, self).__init__(manifest, logger)

        # Need to remote compression on the root.
        if isinstance(FsCompressionCache, cache):
            self.root_cache = cache.upstream.clone()
        else:
            self.root_cache = cache.clone()

        self.cache = cache.clone() # We're going to change the prefix

        self.cache.prefix = os.path.join(self.cache.prefix, self.m.uid).rstrip('/')

    def publish(self):
        from ..util import md5_for_file

        import json

        cache = self.cache

        if self.m.access == 'public' or self.m.access == 'private-data':
            doc_acl = 'public-read'
        else:
            doc_acl = 'private'

        if self.m.access == 'private' or self.m.access == 'private-data':
            data_acl = 'private'
        else:
            data_acl = 'public-read'


        self.logger.info("Publishing to {}".format(cache))

        doc_url = None

        for p in self.m.file_installs:

            if p == self.m.warehouse.database.path and not self.m.install_db:
                self.logger.info("Not installing database, skipping : {}".format(self.m.warehouse.database.path))
                continue

            rel = p.replace(self.m.abs_work_dir, '', 1).strip('/')

            md5 = md5_for_file(p)

            if cache.has(rel):
                meta = cache.metadata(rel)
                if meta.get('md5', False) == md5:
                    self.logger.info("Md5 match, skipping : {}".format(rel))
                    if 'index.html' in rel:
                        doc_url = cache.path(rel, public_url=True, use_cname=True)
                    continue
                else:
                    self.logger.info("Remote has, but md5's don't match : {}".format(rel))
            else:
                self.logger.info("Publishing: {}".format(rel))

            meta = {
                'md5': md5
            }

            if rel.endswith('.html'):
                meta['Content-Type'] = 'text/html'
                meta['acl'] = doc_acl
            else:
                meta['acl'] = data_acl

            cache.put(p, rel, metadata=meta)

            self.logger.info("Published: {}".format(cache.path(rel, public_url=True, use_cname=True)))

            if 'index.html' in rel:
                doc_url = cache.path(rel, public_url=True, use_cname=True)


        # Write the Metadata to the root cache

        meta = self.m.meta
        meta['url'] = doc_url

        rel = os.path.join('meta', self.m.uid + '.json')

        s = self.root_cache.put_stream(rel)
        s.write(json.dumps(meta))
        s.close()

        self.logger.info("Finished publication. Documentation at: {}".format(doc_url))



class CKANPublisher(ManifestPublisher):
    pass

