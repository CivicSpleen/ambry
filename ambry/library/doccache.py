"""
"""

# Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

class DocCache(object):

    all_bundles = None

    _cache = None

    def __init__(self, library, cache = None):
        self.library = library

        if self.library._doc_cache:
            from ckcache.dictionary import DictCache
            self._cache = DictCache(self.library._doc_cache)
        else:
            self._cache = {}

        self.ignore_cache = False # if True, assume the next quest to cache the key does not exist

    def cache(self, f, *args, **kwargs):
        """Cache the return value of a method. Normally, we'd use @memoize, but
        we want this to run in the context of the object. """

        if '_key' in kwargs:
            key = kwargs['_key']
            del kwargs['_key']
        else:
            key = ''
            if args:
                key += '_'.join(str(arg) for arg in  args)

            if kwargs:
                key += '_'.join(str(arg) for arg in kwargs.values() )


        key = key[0] + '/' + key[1:4] +'/' + key

        assert bool(key)

        if key not in self._cache or self.ignore_cache:
            self._cache[key] = f(*args, **kwargs)

        return self._cache[key]

    def library_info(self):
        pass

    ##
    ## Index, low-information lists of all items in a category.
    ##


    def library_info(self):
        return self.cache(lambda: self.library.summary_dict, _key='library_info')

    def bundle_index(self):
        return self.cache(lambda: self.library.versioned_datasets() , _key='bundle_index')

    def collection_index(self):
        pass

    def warehouse_index(self):
        return self.cache(lambda: {f.ref: dict(
            title=f.data['title'],
            summary=f.data['summary'] if f.data['summary'] else '',
            dsn=f.path,
            manifests=[m.ref for m in f.linked_manifests],
            cache=f.data['cache'],
            class_type=f.type_) for f in self.library.stores}, _key = 'warehouse_index')


    def table_index(self):
        pass

    ##
    ## Single Object acessors
    ##


    def dataset(self, vid):
        return self.cache(lambda vid: self.library.dataset(vid).dict, vid)

    def bundle(self, vid):
        return self.cache(lambda vid: self.library.bundle(vid).dict, vid)

    def bundle_schema(self, vid):
        pass

    def partition(self, vid):

        return self.cache(lambda vid: self.library.partition(vid).dict, vid)

    def table(self, vid):
        return self.cache(lambda vid: self.library.table(vid).nonull_col_dict, vid)

    def table_schema(self, vid):
        pass


    def warehouse(self, vid):
        return self.cache(lambda vid: self.library.warehouse(vid).dict, vid)

    def manifest(self, vid):

        def f(vid):
            f, m =  self.library.manifest(vid)
            return m.dict

        self.cache(f, vid)

    def table_version_map(self):
        """Map unversioned table ids to vids. """

        def f():
            tm = {}

            for  t in self.library.tables:

                if not t.id_ in tm:
                    tm[t.id_] = [t.vid]
                else:
                    tm[t.id_].append(t.vid)

            return tm

        return self.cache(f,_key = 'table_version_map')

    ##
    ## Manifests

    def manifest_relpath(self, uid):
        return self.path(self.templates['manifest'], uid=self.resolve_vid(uid))

    def put_manifest(self, m,f):
        """WARNING! This method must be run after all of the bundles are already cached, or at least
        the bundles used in this manifest"""

        from ambry.identity import ObjectNumber

        d = m.dict
        d['file'] = f.dict
        d['text'] = str(m)

        #d['files'] = f.dict['data'].get('files')

        #del d['file']['data']

        # Update the partitions to include bundle references,
        # then add bundle information.

        partitions = {pvid: str(ObjectNumber.parse(pvid).as_dataset) for pvid in f.dict.get('partitions',[])}

        d["partitions"] = partitions

        d['tables'] = {tvid:  {
                          k:v for k,v in (self.get_table(tvid).items()+[('installed_names',[])]) if k != 'columns'
                       } for tvid in f.dict.get('tables',[])
                      }

        d['bundles'] = {vid: self.get_bundle(vid) for vid in partitions.values()}

        for vid, b in d['bundles'].items():
            b['installed_partitions'] = [pvid for pvid, pbvid in partitions.items() if vid == pbvid]

        ## Generate entries for the tables, using the names that they are installed with. These tables aren't
        ## nessiarily installed; this maps the instllation names to vids if they are installed.

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

        for vid, b in d['bundles'].items():
            for pvid, bvid in d['partitions'].items():
                b = d['bundles'][bvid]
                p = b['partitions'][pvid]
                for tvid in p['table_vids']:

                    t = b['tables'][tvid]
                    e = inst_table_entry(b, p, t)


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

        return self.put(self.manifest_relpath(m.uid),  d)
