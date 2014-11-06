"""
Accessor class to produce dictionary representations of bundles, cached as json.
"""

class Json(object):

    def __init__(self, cache):

        self.cache = cache

    def path(self, t, **kwargs):

        import sys

        return  t.format(**kwargs)

    def _has(self, rel_path):

        return self.cache.has(rel_path)

    def _get(self, rel_path, f):
        import json

        if self.cache.has(rel_path):
            with self.cache.get_stream(rel_path) as s:
                return json.load(s)

        fv = f()

        with self.cache.put_stream(rel_path) as s:
            json.dump(fv, s, indent=2)


class BundleJson(Json):

    bundle_template = '{vid}/bundle.json'

    def __init__(self, cache, bundle=None):
        self._bundle = bundle

        super(BundleJson, self).__init__(cache)


    def resolve_vid(self, vid):
        import sys, re

        if vid and self._bundle:
            vid =  Exception("Can't construct on bundle and specify vid")

        if not vid and not self._bundle:
            raise Exception("Must construct on bundle or specify vid")

        if vid:
            vid = vid
        else:
            vid = self._bundle.identity.vid

        if sys.platform == 'darwin':
            # Mac OS has case-insensitive file systems which cause aliasing in vids,
            # so we add a '_' before the uppercase letters.

            return re.sub(r'([A-Z])', lambda p: '_' + p.group(1), vid)
        else:

            return vid


    def has(self, vid=None):
        """Return true if the bundle has already been gneerated. Assume that all other files have been
        enerated as well"""

        return self._has(self.path(self.bundle_template, vid=self.resolve_vid(vid)))

    def bundle(self, vid=None):
        vid = self.resolve_vid(vid)

        path = self.path(self.bundle_template, vid=vid)

        return self._get(path, lambda: self._bundle.dict)

    def schema(self, vid=None):
        vid = self.resolve_vid(vid)

        path = self.path('{vid}/schema.json', vid=vid)

        return self._get(path, lambda: self._bundle.schema.dict)

    def table(self, vid=None, tvid=None):
        import sys
        import re

        vid = self.resolve_vid(vid)

        path = self.path('{vid}/tables/{tvid}.json', vid=vid,
                         tvid=re.sub(r'([A-Z])', lambda p: '_' + p.group(1), tvid) if sys.platform == 'darwin' else '')

        return self._get(path, lambda: self._bundle.schema.table(tvid).nonull_col_dict)


    def tables(self, vid=None):
        """Generates all tables. Doens't return anythin. """
        for t in self._bundle.schema.tables:
            self.table(vid, t.vid)

class LibraryJson(Json):

    def __init__(self, cache, library=None):
        self._library = library

        super(LibraryJson, self).__init__(cache)

        self.path = self.path('library.json')

    def put(self):
        """Always creates anew"""

        self.cache.remove(self.path)

        return self.get()

    def get(self):

        return self._get(self.path, lambda: self._library.dict)

