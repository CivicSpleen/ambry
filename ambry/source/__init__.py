
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

    def __init__(self, base_dir, logger = None):
        self.base_dir = base_dir
        self._library = None
        self.logger = logger

        if not self.logger:
            self.logger = lambda x: x

        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)

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

    @property
    def library(self):
        if not self._library:
            self._library = SourceTreeLibrary(self, self.base_dir)

        return self._library


    def temp_repo(self):
        from uuid import uuid4
        from ..source.repository.git import GitShellService
        tmp = os.path.join(self.base_dir, '_source', 'temp',str(uuid4()))

        if not os.path.exists(os.path.dirname(tmp)):
            os.makedirs(os.path.dirname(tmp))

        return GitShellService(tmp)


    def watch(self):
        pass


    def sync_org(self, repo):
        '''Sync all fo the bundles in an organization or account'''
        from ..identity import Identity

        self.logger("Sync repo: {}".format(str(repo)))
        for e in repo.service.list():

            ident = Identity.from_dict(e)
            self.logger("   Sync repo entry: {} -> {} ".format(ident.fqname, e['clone_url']))

            self.library.add_source_ref(ident, repo=repo.ident, url=e['clone_url'], data=e)

    def sync_repo(self, url):
        pass

    def clone(self,url):
        '''Clone a new bundle insto the source tree'''

        import shutil

        repo = self.temp_repo()
        repo.clone(url)

        bundle_class = load_bundle(repo.path)
        bundle = bundle_class(repo.path)

        bundle_dir = os.path.join(self.base_dir, bundle.identity.source_path)

        if os.path.exists(bundle_dir):
            raise Exception("{} already exists".format(bundle_dir))

        if not os.path.exists(os.path.dirname(bundle_dir)):
            os.makedirs(os.path.dirname(bundle_dir))

        shutil.move(repo.path, bundle_dir)

        bundle_class = load_bundle(bundle_dir)
        bundle = bundle_class(bundle_dir)

        self.library.load_bundle(bundle_dir, bundle.identity)


class SourceTreeLibrary(object):


    def __init__(self, tree, base_dir):
        from ..library import _new_library
        import os

        self.tree = tree
        self.base_dir = base_dir

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

        self._library = library


        if len(self._library.database.datasets()) == 0:
            self.sync()

    def sync(self):
        self._load_database()

    def check_bundle_config(self, path):
        pass

    def check_bundle_db(self,path):
        pass

    def check_bundle_repo(self, path):
        pass


    def resolve(self, term):

        # The terms come from the command line args as a list
        try: term = term.pop(0)
        except: pass

        ident = self._library.resolve(term)

        if not ident:
            return None

        f = self._library.database.get_file_by_ref(ident.id_, type_= 'source_url')

        if f:

            f = f.pop(0)

            ident.url = f.source_url
            ident.data['repo'] = f.group

        return ident


    def add_source_ref(self, ident, url, repo, data):

        self._library.database.install_dataset_identity(ident)

        self._library.database.add_file(
            path=ident.fqname,
            group=repo,
            ref=ident.id_,
            state='synced',
            type_='source_url',
            data=data,
            source_url=data['clone_url'])


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


    def _load_database(self):

        for ident in self.tree.list():
            print 'Loading ', ident
            path = ident.data['path']
            self.load_bundle(path, ident)

    def load_bundle(self, path, ident):
        from ..util import md5_for_file

        self._library.database.install_dataset_identity(ident)

        bundle_file = path + '/bundle.yaml'

        self._library.database.add_file(
            path=bundle_file,
            group='source',
            ref=ident.id_,
            state='loaded',
            type_='bundle_config',
            data=ident.dict,
            hash=md5_for_file(bundle_file),
            source_url=None)

        self._library.database.add_file(
            path=path,
            group='source',
            ref=ident.id_,
            state='loaded',
            type_='source_dir',
            data=None,
            source_url=None)

        self._library.database.add_file(
            path=path + '/.git',
            group='source',
            ref=ident.id_,
            state='loaded',
            type_='git_dir',
            data=None,
            source_url=None)

        bundle_file = os.path.join(path, 'build', ident.cache_key)

        self._library.database.add_file(
            path=bundle_file,
            group='source',
            ref=ident.id_,
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