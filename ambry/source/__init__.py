
"""

"""

# Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt



import os
from ..identity import Identity
from ..orm import Dataset

def load_bundle(bundle_dir):
    from ambry.run import import_file

    rp = os.path.realpath(os.path.join(bundle_dir, 'bundle.py'))
    mod = import_file(rp)

    return mod.Bundle


class SourceTree(object):

    def __init__(self, base_dir, repos, library, logger=None):
        self.repos = repos
        self.base_dir = base_dir
        self.library = library
        self.logger = logger

        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)


    def list(self, datasets=None, key='vid'):
        from ..identity import Identity, LocationRef

        if datasets is None:
            datasets = {}

        for file_ in self.library.files.query.type(Dataset.LOCATION.SOURCE).all:

            ident = Identity.from_dict(file_.data['identity'])

            ck = getattr(ident, key)

            if ck not in datasets:
                datasets[ck] = ident

            try:
                bundle = self.bundle(ident.source_path)
            except ImportError:
                raise Exception("Failed to load bundle from {}".format(ident.source_path))

            if bundle.is_built:
                datasets[ck].locations.set(LocationRef.LOCATION.SOURCE)
            else:
                datasets[ck].locations.set(LocationRef.LOCATION.SOURCE.lower())
            datasets[ck].data = file_.dict
            import pprint

            datasets[ck].bundle_path = file_.path
            datasets[ck].bundle_state = file_.state
            datasets[ck].git_state = file_.data['git_state']

        for file_ in self.library.files.query.type(Dataset.LOCATION.SREPO).all:

            ident = Identity.from_dict(file_.data)

            ck = getattr(ident, key)

            if ck not in datasets:
                datasets[ck] = ident

            datasets[ck].locations.set(LocationRef.LOCATION.SREPO)

        return datasets



    def temp_repo(self):
        from uuid import uuid4
        from ..source.repository.git import GitShellService
        tmp = os.path.join(self.base_dir, '_source', 'temp',str(uuid4()))

        if not os.path.exists(os.path.dirname(tmp)):
            os.makedirs(os.path.dirname(tmp))

        return GitShellService(tmp)


    def watch(self):
        pass


    def set_bundle_state(self, ident, state):

        f = self.library.files.query.ref(ident.vid).type(Dataset.LOCATION.SOURCE).one_maybe

        if f:
            f.state = state
            self.library.files.merge(f)


    def clone(self,url):
        '''Clone a new bundle into the source tree'''

        import shutil
        from ..dbexceptions import ConflictError
        from ..bundle import BundleFileConfig

        repo = self.temp_repo()
        repo.clone(url)

        bfc = BundleFileConfig(repo.path)
        ident = bfc.get_identity()
        bundle_dir = os.path.join(self.base_dir, ident.source_path)

        if os.path.exists(bundle_dir):
            raise ConflictError("Bundle directory '{}' already exists".format(bundle_dir))

        if not os.path.exists(os.path.dirname(bundle_dir)):
            os.makedirs(os.path.dirname(bundle_dir))

        shutil.move(repo.path, bundle_dir)

        try:

            self.sync_bundle(bundle_dir, ident)

            bundle_class = load_bundle(bundle_dir)
            bundle = bundle_class(bundle_dir)

            return bundle
        except Exception as e:
            self.logger.error("Failed to load bundle source file: {}".format(e.message))
            self.set_bundle_state(ident, 'error:load')
            return None


    def sync_repos(self):

        self.library.database.session.query(Dataset).filter(Dataset.location == Dataset.LOCATION.SREPO).delete()
        self.library.files.query.type(Dataset.LOCATION.SREPO).delete()

        for repo in self.repos:
            self._sync_repo(repo)


    def _sync_repo(self, repo):
        '''Sync all fo the bundles in an organization or account'''

        self.logger.info("Sync repo: {}".format(str(repo)))

        for e in repo.service.list():
            ident = Identity.from_dict(e)
            self.logger.info("   Sync repo entry: {} -> {} ".format(ident.fqname, e['clone_url']))

            self.add_source_url(ident, repo=repo.ident, data=e)

    def add_source_url(self, ident, repo, data):

        self.library.database.install_dataset_identity(ident, location=Dataset.LOCATION.SREPO)

        self.library.files.new_file(
            merge=True,
            path=ident.fqname,
            group=repo,
            ref=ident.vid,
            state='synced',
            type_=Dataset.LOCATION.SREPO,
            data=data,
            source_url=data['clone_url'])


    def sync_source(self):

        self.library.database.session.query(Dataset).filter(Dataset.location == Dataset.LOCATION.SOURCE).delete()
        self.library.files.query.type(Dataset.LOCATION.SOURCE).delete()

        for ident in self._dir_list().values():
            self.sync_bundle(ident.bundle_path, ident, ident.bundle)


    def sync_bundle(self, path, ident=None, bundle=None):


        self.logger.info("Sync source bundle: {} ".format(path))

        if not bundle and os.path.exists(path):
            bundle = self.bundle(path)

        if not ident and bundle:
            ident = bundle.identity

        self.library.database.install_dataset_identity(ident, location=Dataset.LOCATION.SOURCE)

        f = self.library.files.query.type(Dataset.LOCATION.SOURCE).ref(ident.vid).one_maybe

        if not f:

            from ..orm import File

            f = File(
                path=path,
                group='source',
                ref=ident.vid,
                state='',
                type_=Dataset.LOCATION.SOURCE,
                data=None,
                source_url=None)

            d = dict(
                identity=ident.dict,
                bundle_config=None,
                bundle_state=None,
                process=None,
                git_state=None,
                rev=0
            )

        else:

            d = f.data

        if bundle and bundle.is_built:
            config = dict(bundle.db_config.dict)
            d['process'] = config['process']
            f.state = 'built'

        d['rev'] = d['rev'] + 1

        f.data = d

        self.library.files.merge(f)




    def _dir_list(self, datasets=None, key='vid'):
        from ..identity import LocationRef, Identity
        from ..bundle import BuildBundle

        if datasets is None:
            datasets = {}

        # Walk the subdirectory for the files to build, and
        # add all of their dependencies
        for root, dirs, files in os.walk(self.base_dir):

            # Yes! can edit dirs in place!
            dirs[:] = [d for d in dirs if not d.startswith('_')]

            if 'bundle.yaml' in files:

                bundle = BuildBundle(root)

                ident = bundle.identity
                ck = getattr(ident, key)

                if ck not in datasets:
                    datasets[ck] = ident

                if bundle.is_built:
                    datasets[ck].locations.set(LocationRef.LOCATION.SOURCE)
                else:
                    datasets[ck].locations.set(LocationRef.LOCATION.SOURCE.lower())

                datasets[ck].bundle_path = root
                datasets[ck].bundle = bundle

        return datasets



    #
    # Bundles
    #

    def source_path(self, term=None, ident=None):

        if ident is None:
            ident = self.library.resolve(term, location=Dataset.LOCATION.SOURCE)

        if not ident:
            return None

        return os.path.join(self.base_dir, ident.source_path)

    def bundle(self, path):
        '''Return an  Bundle object, using the class defined in the bundle source'''
        if path[0] != '/':
            root = os.path.join(self.base_dir, path)
        else:
            root = path

        bundle_class = load_bundle(root)
        bundle = bundle_class(root)
        return bundle



    def resolve_bundle(self, term):
        from ambry.orm import Dataset
        ident = self.library.resolve(term, location=Dataset.LOCATION.SOURCE)

        if not ident:
            return None

        return self.bundle(os.path.join(self.base_dir, ident.source_path))


    def resolve_build_bundle(self, term):
        '''Return an Bundle object, using the base BuildBundle class'''
        from ..bundle import BuildBundle

        ident = self.library.resolve(term, location=Dataset.LOCATION.SOURCE)

        if not ident:
            return None

        path =  os.path.join(self.base_dir, ident.source_path)

        if path[0] != '/':
            root = os.path.join(self.base_dir, path)
        else:
            root = path

        return BuildBundle(root)

class SourceTreeLibrary(object):



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
            ref=identity.vid,
            state=state,
            type_=type_,
            data=data,
            source_url=source_url)

    def _remove_file_by_ref(self, ref, type_):

        self._library.remove_file(self, ref, type_=type_)





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