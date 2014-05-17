"""
A Cache that multiplexes to multiple underlying caches

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from . import CacheInterface, NullCache, PassthroughCache

class MultiCache(CacheInterface):
    """Read and write to multiple underlying caches"""


    upstreams = None

    def __init__(self,upstreams):
        self.upstreams = upstreams

    def repo_id(self):
        import hashlib

        hashlib.sha224(':'.join([x.repo_id for x in self.upstreams])).hexdigest()

    def path(self, rel_path, propatate = True, **kwargs):
        return self.first_has(rel_path).path(rel_path, propatate=propatate, **kwargs)


    def get(self, rel_path, cb=None):
        return self.first_has(rel_path).get(rel_path, cb=cb)

    def get_stream(self, rel_path, cb=None):
        return self.first_has(rel_path).get_stream(rel_path, cb=cb)

    def has(self, rel_path, md5=None, propagate=True):
        return self.first_has(rel_path).has(rel_path, md5=md5, propagate=propagate)

    def first_has(self, rel_path, md5=None):

        for upstream in self.upstreams:
            h = upstream.has(rel_path, md5=md5)
            if h:
                return upstream

        return NullCache()


    def put(self, source, rel_path, metadata=None):
        """Puts to only the first upstream. This is to be symmetric with put_stream."""
        return self.upstreams[0].put(source, rel_path, metadata)


    def put_stream(self,rel_path, metadata=None):
        """Puts to only the first stream"""
        return self.upstreams[0].put(rel_path, metadata)

    def find(self,query): raise NotImplementedError()

    def list(self, path=None, with_metadata=False, include_partitions=False):
        """Combine a listing of all of the upstreams, and add a metadata item for the
        repo_id"""

        l = {}
        for upstream in reversed(self.upstreams):

            for k, v in upstream.list(path, with_metadata, include_partitions).items():

                upstreams = (l[k]['caches'] if k in l else []) + v.get('caches',upstream.repo_id)

                l[k] = v
                l[k]['caches'] = upstreams

        return l

    def remove(self, rel_path, propagate=False):

        for upstream in self.upstreams:
            upstream.remove(rel_path, propagate)


    def clean(self):

        for upstream in self.upstreams:
            upstream.clean()



    def get_upstream(self, type_):
        """Returns only the first upstream"""

        return self.upstream[0]


    def last_upstream(self):
        """Return the last upstream of the first upstream."""

    def attach(self, upstream): raise NotImplementedError(type(self))

    def detach(self): raise NotImplementedError(type(self))


class AltReadCache(PassthroughCache):
    """Like PasthroughCache, but if the object doesn't exist in the main cache, it is read
        from the alternate, and stored in the main"""

    def __init__(self,upstream, alternate):
        super(AltReadCache, self).__init__(upstream)
        self.alternate = alternate



    def has(self, rel_path, md5=None, propagate=True):

        ush = self.upstream.has(rel_path, md5, propagate)

        if ush:
            return ush

        return self.alternate.has(rel_path, md5, propagate)

    def path(self, rel_path, md5=None, propagate=True):

        ush = self.upstream.path(rel_path, md5, propagate)

        if ush:
            return ush

        return self.alternate.path(rel_path, md5, propagate)

    def list(self, path=None, with_metadata=False, include_partitions=False):
        """Combine a listing of all of the upstreams, and add a metadata item for the
        repo_id"""

        l = {}
        for upstream in [self.alternate, self.upstream]:

            for k, v in upstream.list(path, with_metadata, include_partitions).items():
                upstreams = (l[k]['caches'] if k in l else []) + v.get('caches', upstream.repo_id)

                l[k] = v
                l[k]['caches'] = upstreams

        return l

    def _copy_across(self, rel_path, cb=None):
        from ..util.flo import copy_file_or_flo

        if not self.upstream.has(rel_path):

            if not self.alternate.has(rel_path):
                return None

            source = self.alternate.get_stream(rel_path)

            sink = self.upstream.put_stream(rel_path, metadata=source.meta)

            try:
                copy_file_or_flo(source, sink, cb=cb)
            except:
                self.cache.remove(rel_path, propagate=True)
                raise

            source.close()
            sink.close()

    def get(self, rel_path, cb=None):
        self._copy_across(rel_path, cb)
        return self.upstream.get(rel_path, cb)

    def get_stream(self, rel_path, cb=None):
        self._copy_across(rel_path, cb)
        return self.upstream.get_stream(rel_path, cb)
