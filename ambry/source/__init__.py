
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
        return self.library.list(datasets, key)

    def _source_lists(self, datasets=None, key='vid'):
        pass

    def _dir_list(self, datasets=None, key='vid'):
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

                if bundle.is_built:
                    datasets[ck].locations.set(LocationRef.LOCATION.SOURCE)
                else:
                    datasets[ck].locations.set(LocationRef.LOCATION.SOURCE.lower())

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


    def sync(self, repos):

        self.library.clear_datasets()

        # Sync all of the registered repositories
        for repo in repos:
            self.sync_org(repo)

        self.sync_source()

    def sync_source(self):
        self.library.sync_source()

    def sync_org(self, repo):
        '''Sync all fo the bundles in an organization or account'''
        from ..identity import Identity

        self.logger("Sync repo: {}".format(str(repo)))

        self.library.clear_source_refs(repo.ident)

        for e in repo.service.list():

            ident = Identity.from_dict(e)
            self.logger("   Sync repo entry: {} -> {} ".format(ident.fqname, e['clone_url']))

            self.library.add_source_ref(ident, repo=repo.ident, url=e['clone_url'], data=e)


    def sync_repo(self, url):
        pass

    def sync_bundle(self, path):
        self.library.update_bundle(path)

    def set_bundle_state(self, ref, state):
        self.library.set_bundle_state(ref,state)


    def clone(self,url):
        '''Clone a new bundle insto the source tree'''

        import shutil
        from ..dbexceptions import ConflictError

        repo = self.temp_repo()
        repo.clone(url)

        bundle_class = load_bundle(repo.path)
        bundle = bundle_class(repo.path)

        bundle_dir = os.path.join(self.base_dir, bundle.identity.source_path)

        if os.path.exists(bundle_dir):
            raise ConflictError("Bundle directory '{}' already exists".format(bundle_dir))

        if not os.path.exists(os.path.dirname(bundle_dir)):
            os.makedirs(os.path.dirname(bundle_dir))

        shutil.move(repo.path, bundle_dir)

        bundle_class = load_bundle(bundle_dir)
        bundle = bundle_class(bundle_dir)

        self.library.update_bundle(bundle_dir, bundle.identity)

        return bundle


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


    def list(self, datasets=None, key='vid'):
        from ..identity import Identity, LocationRef

        if datasets is None:
            datasets = {}

        for file_ in self._library.database.get_file_by_type(type_='bundle_source'):

            ident = Identity.from_dict(file_.data['identity'])

            ck = getattr(ident, key)

            if ck not in datasets:
                datasets[ck] = ident

            try:
                bundle = self._get_bundle(ident.source_path)
            except ImportError:
                raise Exception("Failed to load bundle from {}".format(ident.source_path))

            if bundle.is_built:
                datasets[ck].locations.set(LocationRef.LOCATION.SOURCE)
            else:
                datasets[ck].locations.set(LocationRef.LOCATION.SOURCE.lower())
            datasets[ck].data = file_.dict

        for file_ in self._library.database.get_file_by_type(type_='source_url'):

            ident = Identity.from_dict(file_.data)

            ck = getattr(ident, key)

            if ck not in datasets:
                datasets[ck] = ident

            datasets[ck].locations.set(LocationRef.LOCATION.SREPO)

        return datasets

    def sync_source(self):
        self._load_database()

    def resolve(self, term):
        from ..identity import LocationRef
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
            ident.data['path'] = os.path.join(self.base_dir, ident.source_path)
            ident.locations.set(LocationRef.LOCATION.SREPO)

        f = self._library.database.get_file_by_ref(ident.id_, type_='bundle_source')

        if f:
            f = f.pop(0)

            ident.data['path'] = os.path.join(self.base_dir, ident.source_path)
            ident.locations.set(LocationRef.LOCATION.SOURCE)

        return ident

    def resolve_bundle(self, term):

        ident = self.resolve(term)

        if not ident:
            return None

        return self._get_bundle(os.path.join(self.base_dir, ident.source_path))


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

    def clear_datasets(self):
        from ..orm import Dataset
        from ..library.database import ROOT_CONFIG_NAME_V

        self._library.database.session.query(Dataset).filter(Dataset.vid != ROOT_CONFIG_NAME_V).delete()

        self._library.database.session.commit()

    def clear_source_refs(self,repo):
        from ..orm import File

        self._library.database.session.query(File).filter(File.group == repo).delete()

        self._library.database.session.commit()


    def _load_database(self):
        from ..orm import File

        self._library.database.session.query(File).filter(File.type_ == 'bundle_source').delete()
        self._library.database.session.commit()

        for ident in self.tree._dir_list():
            path = ident.data['path']
            self.update_bundle(path, ident)



    def set_bundle_state(self, ref, state):

        f = self._library.database.get_file_by_ref(ref, type_='bundle_source')

        if  f:
            f = f[0]

            f.data['bundle_state'] = state

            self._library.database.merge_file(f)

    def update_bundle(self, path, ident=None, bundle=None):
        from ..util import md5_for_file

        self.tree.logger("Sync source bundle: {} ".format(path))

        if not bundle and os.path.exists(path):
            bundle = self._get_bundle(path)

        if not ident and bundle:
            ident = bundle.identity

        self._library.database.install_dataset_identity(ident)

        f =  self._library.database.get_file_by_ref(ident.id_, type_='bundle_source')

        if not f:

            from ..orm import File

            f = File(
                path=path,
                group='source',
                ref=ident.id_,
                state='',
                type_='bundle_source',
                data=None,
                source_url=None)

            d = dict(
                identity=ident.dict,
                bundle_config = None,
                bundle_state=None,
                process = None,
                git_state = None,
                rev = 0
            )

        else:
            assert len(f) == 1
            f = f[0]

            d = f.data

        if bundle and bundle.is_built:
            config = dict(bundle.db_config.dict)
            d['process'] = config['process']

        d['rev'] = d['rev']+ 1

        f.data = d

        self._library.database.merge_file(f)


    def _add_file(self, path, identity, state='loaded', type_=None,
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

    def _remove_file_by_ref(self, ref, type_):

        self._library.remove_file(self, ref, type_=type_)


    def _get_bundle(self, path):

        if path[0] != '/':
            root = os.path.join(self.base_dir, path)
        else:
            root = path

        bundle_class = load_bundle(root)
        bundle = bundle_class(root)
        return bundle


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