""""""

# Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
# the Revised BSD License, included in this distribution as LICENSE.txt
import string
import platform
from collections import deque

from sqlalchemy.orm.exc import NoResultFound

from ckcache.dictionary import DictCache


class DocCache(object):

    def __init__(self, library, cache=None):

        self.library = library

        if self.library._doc_cache:
            # Dict interface on an upstream filesystem cache
            self._cache = DictCache(self.library._doc_cache)
        else:
            self._cache = {}  # An actual dict.

        self.all_bundles = None
        self.times = deque([], maxlen=10000)
        # if True, assume the next quest to cache the key does not exist
        self.ignore_cache = False

        # Some OS X file systems are case insensitive, causing aliasing with
        # gvid keys
        self.prefix_upper = platform.system() == 'Darwin'

    def _munge_key(self, *args, **kwargs):


        if '_key' in kwargs and kwargs['_key'] is None:
            del kwargs['_key']

        if '_key' in kwargs and kwargs['_key']:
            key = kwargs['_key']
            del kwargs['_key']
        else:
            key = ''
            if args:
                key += '_'.join(str(arg) for arg in args)

            if kwargs:
                key += '_'.join(str(arg) for arg in kwargs.values())

        assert bool(key)

        # Prefix uppercase letters to avoid aliasing on case-insensitive OS X
        # file systems
        if self.prefix_upper:
            key = ''.join(
                '_' +
                x if x in string.ascii_uppercase else x for x in key)

        if '_key_prefix' in kwargs:
            pk = kwargs['_key_prefix'] + '/' + key[0] + '/' + key[1]
            del kwargs['_key_prefix']
        else:
            pk = key[0] + '/' + key[1]

        key = pk + '/' + key

        return key, args, kwargs

    def cache(self, f, *args, **kwargs):
        """Cache the return value of a method.

        Normally, we'd use @memoize, but
        we want this to run in the context of the object.

        """

        force = 'force' in kwargs

        if force:
            del kwargs['force']

        key, args, kwargs = self._munge_key(*args, **kwargs)

        if force or key not in self._cache or kwargs.get('force') or self.ignore_cache:
            self._cache[key] = f(*args, **kwargs)
        return self._cache[key]

    def clean(self):

        try:
            self._cache.clean()
        except AttributeError:
            assert isinstance(self._cache, dict)
            self._cache = {}

    def remove(self, *args, **kwargs):

        key, args, kwargs = self._munge_key(*args, **kwargs)

        if key in self._cache:
            del self._cache[key]

    #
    # Index, low-information lists of all items in a category.
    #

    def library_info(self):
        return self.cache(lambda: self.library.summary_dict, _key='library_info')

    def bundle_index(self):
        return self.cache(lambda: self.library.versioned_datasets(), _key='bundle_index')

    def table_index(self):
        pass

    ##
    # Single Object acessors
    ##

    def dataset(self, vid):
        # Add a 'd' to the datasets, since they are just the dataset record and must
        # be distinguished from the full output with the same vid in bundle()

        # Some of the older bundles don't have the title and summary in the darta for the dataset,
        # so we have to gfet it from the config
        def dict_and_summary(vid):
            ds = self.library.dataset(vid)
            d = ds.dict

            if not d.get('title', False) or not d.get('summary', False):
                try:
                    d['title'] = ds.config('about.title').value
                    d['summary'] = ds.config('about.summary').value
                except NoResultFound:
                    # Probably bad -- every bundle should have about.title and about.summary by now
                    pass

            return d

        return self.cache(
            lambda vid: dict_and_summary(vid),
            vid, _key_prefix='ds')

    def bundle_summary(self, vid):

        return self.cache(
            lambda vid: self.library.bundle(vid).summary_dict,
            vid, _key_prefix='bs')

    def bundle(self, vid):

        return self.cache(lambda vid: self.library.bundle(vid).dict, vid)

    def bundle_schema(self, vid):
        pass

    def partition(self, vid):
        return self.cache(lambda vid: self.library.partition(vid).dict, vid)

    def table(self, vid):

        def table_(vid):
            t = self.library.table(vid).nonull_col_dict

            t['foreign_indexes'] = list(set([c['index'].split(':')[0] for c in t['columns'].values() if c.get('index', False)]))

            return t

        return self.cache(lambda vid: table_(vid), vid)

    def table_schema(self, vid):
        pass

    def warehouse(self, vid):

        return self.cache(lambda vid: self.library.warehouse(vid).dict, vid)

    def manifest(self, vid):

        def f(vid):
            f, m = self.library.manifest(vid)
            return m.dict

        self.cache(f, vid)

    def table_version_map(self):
        """Map unversioned table ids to vids."""

        def f():
            tm = {}

            # The no_columns version is a lot faster.
            for t in self.library.tables_no_columns:

                if t.id_ not in tm:
                    tm[t.id_] = [t.vid]
                else:
                    tm[t.id_].append(t.vid)
            return tm
        return self.cache(f, _key='table_version_map')

    #
    # Manifests

    def manifest_relpath(self, uid):
        return self.path(self.templates['manifest'], uid=self.resolve_vid(uid))
