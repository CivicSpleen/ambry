
"""

"""

# Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt



import os




def load_bundle(bundle_dir):
    from ambry.run import import_file

    rp = os.path.realpath(os.path.join(bundle_dir, 'bundle.py'))
    mod = import_file(rp)

    return mod.Bundle

class SourceTree(object):

    def __init__(self, base_dir):
        self.base_dir = base_dir
        self._library = None

    def list(self, datasets=None, key='vid'):
        from ..identity import LocationRef, Identity

        if datasets is None:
            datasets =  {}

        # Walk the subdirectory for the files to build, and
        # add all of their dependencies
        for root, _, files in os.walk(self.base_dir):
            if 'bundle.yaml' in files:

                bundle_class = load_bundle(root)
                bundle = bundle_class(root)

                ident = bundle.identity
                ck = getattr(ident, key)

                if ck not in datasets:
                    datasets[ck] = ident

                datasets[ck].locations.set(LocationRef.LOCATION.SOURCE)
                datasets[ck].data['path'] = root



        return sorted(datasets.values(), key=lambda x: x.vname)


    def watch(self):
        pass



class SourceTreeLibrary(object):


    @property
    def library(self):
        if not self._library:
            self._library = self._create_library()

            if len(self._library.database.datasets()) == 0:
                self._load_database()

        return self._library


    def check_bundle_config(self, path):
        pass

    def check_bundle_db(self,path):
        pass

    def check_bundle_repo(self, path):
        pass

    def _add_file(self,path, identity, state='loaded',type_=None,
                  data=None, source_url=None):

        import os.path
        from ..util import md5_for_file

        hash = None

        if os.path.is_file(path):
            hash = md5_for_file(path)

        self._library.database.add_file(
            path=path,
            group='source',
            ref=identity.id_,
            state=state,
            type_=type_,
            data=data,
            source_url=source_url)

    def _remove_file(self, path):
        pass

    def _create_library(self):
        from ..cache.filesystem import FsCache
        from ..library import _new_library

        import os

        cache_dir = os.path.join(self.base_dir, '_source', 'cache')

        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        library = _new_library({
            '_name': 'source-library',
            'filesystem': {
                'dir': cache_dir

            },
            'database': {
                'driver': 'sqlite',
                'dbname': os.path.join(self.base_dir, '_source', 'source.db')

            }
        })

        return library


    def _load_database(self):
        from ..util import md5_for_file

        for dsid in self.list():
            print 'Loading ', dsid
            self._library.database.install_dataset_identity(dsid)

            f = dsid.data['path']

            bundle_file = f + '/bundle.yaml'

            self._library.database.add_file(
                path=bundle_file,
                group='source',
                ref=dsid.id_,
                state='loaded',
                type_='bundle_config',
                data=dsid.dict,
                hash=md5_for_file(bundle_file),
                source_url=None)

            self._library.database.add_file(
                path=f,
                group='source',
                ref=dsid.id_,
                state='loaded',
                type_='source_dir',
                data=None,
                source_url=None)

            self._library.database.add_file(
                path=f + '/.git',
                group='source',
                ref=dsid.id_,
                state='loaded',
                type_='git_dir',
                data=None,
                source_url=None)

            bundle_file = os.path.join(f, 'build', dsid.cache_key)

            self._library.database.add_file(
                path=bundle_file,
                group='source',
                ref=dsid.id_,
                state='loaded',
                type_='bundle_file',
                data=None,
                source_url=None)


    def _get_bundle(self, root):
        bundle_class = load_bundle(root)
        bundle = bundle_class(root)


class SourceTreeWatcher(object):

    def __init__(self, base_dir):
        self.base_dir = base_dir
        self._library = None


    def watch(self, cb=None):
        import time
        from watchdog.observers import Observer
        from watchdog.events import PatternMatchingEventHandler

        library = self.library
        this = self

        class EventHandler(PatternMatchingEventHandler):

            def __init__(self):
                super(EventHandler, self).__init__(
                    patterns=['*/bundle.yaml',
                              '*/bundle.py',
                              '*/.git',
                              '*/build/*.db'

                    ]
                )

            def on_any_event(self, event):
                print event

                l = this._create_library()
                f = l.database.get_file_by_path(event.src_path)

                if f:
                    print f.dict


        print 'Watching ', self.base_dir

        event_handler = EventHandler()
        observer = Observer()
        observer.schedule(event_handler, path=self.base_dir, recursive=True)
        observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()