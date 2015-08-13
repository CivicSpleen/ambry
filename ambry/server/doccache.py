""""""

# Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
# the Revised BSD License, included in this distribution as LICENSE.txt

from collections import deque


class DocCache(object):

    def __init__(self, library, cache=None):
        import platform

        self.library = library

        if self.library._doc_cache:
            from ckcache.dictionary import DictCache
            self._cache = DictCache(self.library._doc_cache) # Dict interface on an upstream filesystem cache
        else:
            self._cache = {} # An actual dict.

        self.all_bundles = None
        self.times = deque([], maxlen=10000)
        # if True, assume the next quest to cache the key does not exist
        self.ignore_cache = False

        # Some OS X file systems are case insensitive, causing aliasing with
        # gvid keys
        self.prefix_upper = platform.system() == 'Darwin'

    def _munge_key(self, *args, **kwargs):

        import string

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
                key += '_'.join(str(arg) for arg in list(kwargs.values()))

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

        force =  'force' in kwargs

        if force:
            del kwargs['force']


        key, args, kwargs = self._munge_key(*args, **kwargs)

        if force or key not in self._cache or kwargs.get('force') or self.ignore_cache:
            self._cache[key] = f(*args, **kwargs)
            from_cache = False
        else:
            from_cache = True


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

    def library_info(self):
        pass

    #
    # Index, low-information lists of all items in a category.
    #

    def library_info(self):
        return self.cache(lambda: self.library.summary_dict, _key='library_info')

    def bundle_index(self):
        return self.cache(lambda: self.library.versioned_datasets(),_key='bundle_index')

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
                from sqlalchemy.orm.exc import NoResultFound
                try:
                    d['title'] =  ds.config('about.title').value
                    d['summary'] = ds.config('about.summary').value
                except NoResultFound:
                    pass # Probably bad -- every bundle should have about.title and about.summary by now

            return d

        return self.cache(lambda vid: dict_and_summary(vid),vid,_key_prefix='ds')

    def bundle_summary(self, vid):

        return self.cache(lambda vid: self.library.bundle(vid).summary_dict,vid,_key_prefix='bs')

    def bundle(self, vid):

        return self.cache(lambda vid: self.library.bundle(vid).dict, vid)

    def bundle_schema(self, vid):
        pass

    def partition(self, vid):

        return self.cache(lambda vid: self.library.partition(vid).dict, vid)

    def table(self, vid):

        def table(vid):
            t = self.library.table(vid).nonull_col_dict

            t['foreign_indexes'] = list(set([c['index'].split(':')[0] for c in list(t['columns'].values()) if c.get('index', False) ]))

            return t

        return self.cache(lambda vid: table(vid),vid)

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

    def put_manifest(self, m, f):
        """WARNING!

        This method must be run after all of the bundles are already
        cached, or at least the bundles used in this manifest

        """

        raise NotImplementedError()

        from ambry.identity import ObjectNumber

        d = m.dict
        d['file'] = f.dict
        d['text'] = str(m)

        # d['files'] = f.dict['data'].get('files')

        # del d['file']['data']

        # Update the partitions to include bundle references,
        # then add bundle information.

        partitions = {pvid: str(ObjectNumber.parse(pvid).as_dataset) for pvid in f.dict.get('partitions', [])}

        d["partitions"] = partitions

        d['tables'] = {tvid: {
            k: v for k, v in (list(self.get_table(tvid).items()) + [('installed_names', [])]) if k != 'columns'
        } for tvid in f.dict.get('tables', [])}

        d['bundles'] = {vid: self.get_bundle(vid) for vid in list(partitions.values())}

        for vid, b in list(d['bundles'].items()):
            b['installed_partitions'] = [pvid for pvid, pbvid in list(partitions.items()) if vid == pbvid]

        # Generate entries for the tables, using the names that they are installed with. These tables aren't
        # nessiarily installed; this maps the instllation names to vids if they
        # are installed.

        installed_table_names = {}

        def inst_table_entry(b, p, t):
            return dict(
                t_vid=t['vid'],
                t_name=t['name'],
                p_vid=p['vid'],
                p_vname=p['vname'],
                b_vid=b['identity']['vid'],
                b_vname=b['identity']['vname']
            )

        for vid, b in list(d['bundles'].items()):
            for pvid, bvid in list(d['partitions'].items()):
                b = d['bundles'][bvid]
                p = b['partitions'][pvid]
                for tvid in p['table_vids']:

                    t = b['tables'][tvid]
                    inst_table_entry(b, p, t)

        d['installed_table_names'] = installed_table_names

        # Collect the views and mviews

        views = {}

        for s in d['sections']:
            if s['tag'] in ('view', 'mview'):
                views[s['args']] = dict(
                    tag=s['tag'],
                    tc_names=s.get('content', {}).get('tc_names'),
                    html=s.get('content', {}).get('html'),
                    text=s.get('content', {}).get('text'),
                )

        d['views'] = views

        return self.put(self.manifest_relpath(m.uid), d)
