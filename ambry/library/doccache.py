"""
"""

# Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

from ..util import memoize, expiring_memoize
import functools


def memoize_vid(self, obj):
    @functools.wraps(obj)
    def memoizer(vid):
        if vid not in self.obj_cache:
            self.obj_cache[vid] = obj(vid)
        return self.obj_cache[vid]

    return memoizer

class DocCache(object):

    all_bundles = None

    obj_cache = None

    def __init__(self, library):
        self.library = library

        self.all_bundles = {}

        self.obj_cache = None


    def library_info(self):
        pass

    ##
    ## Index, low-information lists of all items in a category.
    ##

    def bundle_index(self):
        return { d.vid : d.dict for d in self.library.datasets() }

    def collection_index(self):
        pass

    def warehouse_index(self):
        pass

    def table_index(self):
        pass

    ##
    ## Single Object acessors
    ##


    def dataset(self, vid):
        return self.library.dataset(vid).dict

    def bundle(self, vid):
        return self.library.bundle(vid).dict

    def bundle_schema(self, vid):
        pass

    def partition(self, vid):
        return self.library.partition(vid).dict

    def table(self, vid):
        return self.library.table(vid).dict

    def table_schema(self, vid):
        pass

    def warehouse(self, vid):
        return self.library.warehouse(vid).dict

    def manifest(self, vid):
        return self.library.manifest(vid).dict


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






