
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

    def __init__(self, base_dir, library, logger=None):

        self.base_dir = base_dir
        self.library = library
        self.logger = logger

        if not os.path.exists(self.base_dir):
            from ..dbexceptions import ConfigurationError
            raise ConfigurationError("Source directory {} does not exist".format(self.base_dir))


    def list(self, datasets=None, key='vid'):
        from ..identity import Identity, LocationRef

        if datasets is None:
            datasets = {}

        for file_ in self.library.files.query.type(Dataset.LOCATION.SOURCE).all:

            #ident = Identity.from_dict(file_.data['identity'])

            ident = self.library.resolve(file_.ref, location=None)

            ck = getattr(ident, key)

            try:
                bundle = self.bundle(file_.path)
            except ImportError:
                raise Exception("Failed to load bundle from {}".format(file_.path))


            if ck not in datasets:
                datasets[ck] = ident

            if bundle.is_built:
                datasets[ck].locations.set(LocationRef.LOCATION.SOURCE)
            else:
                datasets[ck].locations.set(LocationRef.LOCATION.SOURCE.lower())

            bundle.close()

            # We want all of the file data, and the 'data' field, at the same level
            d = file_.dict
            sub_dict = d['data']
            del d['data']
            d.update(sub_dict)

            datasets[ck].data = d

            datasets[ck].bundle_path = file_.path
            datasets[ck].bundle_state = file_.state


        return datasets


    def dependencies(self, term=None):
        '''Return a topologically sorted tree of dependencies, for all sources if a term is not given,
        or for a single bundle if it is. '''
        from ..util import toposort
        from ..orm import Dataset
        from ..identity import Identity
        from collections import defaultdict
        from ..dbexceptions import NotFoundError

        l = self.library

        errors = defaultdict(set)
        deps = defaultdict(set)

        # The [:-3] converts a vid to an id: we don't want version numbers here.
        all_sources = {f.ref[:-3]:f for f in l.files.query.type(l.files.TYPE.SOURCE).all}

        def deps_for_sources(sources):
            '''Get dependencies for a set of sources, which may be a subset of all sources. '''
            new_sources = set()
            for source in sources:

                if not ('dependencies' in source.data
                        and source.data['dependencies']
                        and source.data['identity']):
                    continue

                bundle_ident = Identity.from_dict(source.data['identity'])

                if not bundle_ident:
                    print 'A'
                    errors[bundle_ident.sname].add(None)
                    continue

                if bundle_ident.sname in deps:
                    continue

                for v in source.data['dependencies'].values():
                    try:
                        ident = l.resolve(v, location=None, use_remote=True)
                    except:
                        ident = None

                    if not ident:
                        print 'B', v
                        errors[bundle_ident.sname].add(v)
                        continue

                    deps[bundle_ident.sname].add(ident.sname)

                    try:
                        new_source = all_sources[ident.id_]
                        new_sources.add(new_source)
                    except KeyError:
                        print 'C', ident.sname
                        errors[bundle_ident.sname].add(ident.sname)


                if not bundle_ident.sname in deps:
                    deps[bundle_ident.sname].add(None)

            return deps, list(new_sources)

        #
        # First, get the starting list of sources
        #


        if term:
            ident = l.resolve(term, location=Dataset.LOCATION.SOURCE, use_remote=True)

            if ident:
                f = l.files.query.type(l.files.TYPE.SOURCE).ref(ident.vid).first

                if not f:
                    raise NotFoundError("Didn't find a source bundle for term: {} ".format(term))

                sources = [f]

            else:
                raise NotFoundError("Didn't find a source bundle for term: {} ".format(term))

        else:
            sources = all_sources.values()


        while True:

            deps, new_sources = deps_for_sources(sources)

            if not new_sources:
                break

            sources = new_sources


        graph = toposort(deps)


        return graph, errors


    def watch(self):
        pass


    def set_bundle_state(self, ident, state):
        from sqlalchemy.exc import InvalidRequestError

        try:
            f = self.library.files.query.ref(ident.vid).type(Dataset.LOCATION.SOURCE).one_maybe
        except InvalidRequestError:
            # Happens when there is an error installing into the library.
            self.library.database.session.rollback()
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
        bundle_dir = self.source_path(ident=ident)

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


    def sync_source(self, clean = False):

        if clean:
            self.library.database.session.query(Dataset).filter(Dataset.location == Dataset.LOCATION.SOURCE).delete()
            self.library.files.query.type(Dataset.LOCATION.SOURCE).delete()

        for ident in self._dir_list().values():
            try:
                self.sync_bundle(ident.bundle_path, ident)
            except Exception as e:
                raise
                self.logger.error("Failed to sync: bundle_path={} : {} ".format(ident.bundle_path, e.message))

    def _bundle_data(self, ident, bundle):

        try:
            dependencies = bundle.metadata.dependencies
        except:
            print "Dependencies error: ", bundle.bundle_dir
            dependencies = {}

        return dict(
            identity=ident.dict,
            bundle_config=None,
            bundle_state=None,
            process=None,
            rev=0,
            dependencies=dict(dependencies)
        )

    def sync_bundle(self, path, ident=None, bundle=None):
        from ..orm import File
        self.logger.info("Sync source bundle: {} ".format(path))


        if not bundle and os.path.exists(path):
            try:
                bundle = self.bundle(path)
            except Exception as e:
                raise
                self.logger.error("Failed to load bundle for {}".format(path))
                pass


        if not ident and bundle:
            ident = bundle.identity

        files = self.library.files

        f = files.query.type(Dataset.LOCATION.SOURCE).ref(ident.vid).one_maybe

        # Update the file if it already exists.
        if not f:

            f = File(
                path=path,
                group='source',
                ref=ident.vid,
                state='',
                type_=Dataset.LOCATION.SOURCE,
                data=None,
                source_url=None)

            d = self._bundle_data(ident,bundle)
            reattach = False

        else:

            d = f.data
            reattach = f.oid

        #for p in bundle.config.group('partitions'):
        #    if isinstance(p, dict):
        #        print "X!!!",p

        # NOTE -- this code closes ( commits ) the session so the
        # file f is no longer valid if it came from the database, which is
        # why we have to refetch it.
        self.library.database.install_dataset_identity(
            ident, location=Dataset.LOCATION.SOURCE, data=d)

        if reattach:
            f = files.db.session.query(File).get(reattach)


        if bundle and bundle.is_built:

            d['process'] = bundle.get_value_group('process')
            f.state = 'built'

        d['rev'] = d['rev'] + 1  # Marks bundles that have already beein imported.

        f.data = d
        files.merge(f)

        bundle.close()


    def _dir_list(self, datasets=None, key='vid'):
        '''Get a list of sources from the directory, rather than the library '''
        from ..identity import LocationRef, Identity
        from ..bundle import BuildBundle
        from ..dbexceptions import ConfigurationError

        if datasets is None:
            datasets = {}

        if not os.path.exists(self.base_dir):
            raise ConfigurationError("Could not find source directory: {}".format(self.base_dir))

        # Walk the subdirectory for the files to build, and
        # add all of their dependencies
        for root, dirs, files in os.walk(self.base_dir):

            # Yes! can edit dirs in place!
            dirs[:] = [d for d in dirs if not d.startswith('_')]

            if 'bundle.yaml' in files:

                try:
                    bundle = BuildBundle(root)
                except:
                    print 'ERROR: Failed to open bundle dir={}'.format(root)
                    raise

                ident = bundle.identity

                ident.data = ident.dict.update(self._bundle_data(ident, bundle))

                ck = getattr(ident, key)

                if ck not in datasets:
                    datasets[ck] = ident

                if bundle.is_built:
                    datasets[ck].locations.set(LocationRef.LOCATION.SOURCE)
                else:
                    datasets[ck].locations.set(LocationRef.LOCATION.SOURCE.lower())

                datasets[ck].bundle_path = root

                bundle.close()

        return datasets

    #
    # Bundles
    #

    def source_path(self, term=None, ident=None):

        if ident is None:
            ident = self.library.resolve(term, location=Dataset.LOCATION.SOURCE)

        if not ident:
            return None

        f = self.library.files.query.ref(ident.vid).type(Dataset.LOCATION.SOURCE).first

        if not f:
            return None

        return f.path


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

        return self.bundle(os.path.join(self.base_dir, self.source_path(ident=ident)))


    def resolve_build_bundle(self, term):
        '''Return an Bundle object, using the base BuildBundle class'''
        from ..bundle import BuildBundle

        ident = self.library.resolve(term, location=Dataset.LOCATION.SOURCE)

        if not ident:
            return None

        path =  self.source_path(ident=ident)

        if not path:
            return None

        if path[0] != '/':
            root = os.path.join(self.base_dir, path)
        else:
            root = path

        return BuildBundle(root)


    def new_bundle(self, rc, repo_dir, source, dataset, type=None, subset=None, bspace=None, btime=None,
                   variation=None, revision=1, throw=True, examples=True):

        from ..source.repository import new_repository
        from ..identity import DatasetNumber, Identity
        from ..identity import NumberServer

        from requests.exceptions import HTTPError
        from collections import OrderedDict
        from ..dbexceptions import ConflictError, ConfigurationError, SyncError
        import shutil
        import yaml

        if not repo_dir:
            raise ValueError("Must specify a repo_dir")


        if not os.path.exists(repo_dir):
            raise IOError("Repository directory '{}' does not exist".format(repo_dir))

        l  = self.library

        nsconfig = rc.group('numbers')

        ns = NumberServer(**nsconfig)

        d = dict(
            source = source,
            dataset = dataset,
            subset = subset,
            bspace = bspace,
            btime = btime,
            variation = variation,
            revision = revision,
            type = type
        )

        # A) Make the ident the first time to get a path to the bundle
        d['id'] = 'dxxx'  # Fake it, just to get the path.
        ident = Identity.from_dict(d)


        bundle_dir = os.path.join(repo_dir, ident.name.source_path)

        if not os.path.exists(bundle_dir):
            os.makedirs(bundle_dir)

        elif os.path.isdir(bundle_dir):
            if throw:
                raise ConflictError("Directory already exists: " + bundle_dir)
            else:
                return bundle_dir


        # Then (B) if the directory doesn't already exist, get the
        # id number and make it again.
        try:
            if type == 'analysis':
                d['id'] = str(DatasetNumber())
            else:
                d['id'] = str(ns.next())
                self.logger.info("Got number from number server: {}".format(d['id']))
        except HTTPError as e:
            self.logger.warn("Failed to get number from number server: {}".format(e.message))
            self.logger.warn(
                "Using self-generated number. There is no problem with this, but they are longer than centrally generated numbers.")
            d['id'] = str(DatasetNumber())

        ident = Identity.from_dict(d)



        try:
            ambry_account = rc.group('accounts').get('ambry', {})
        except:
            ambry_account = None

        if not ambry_account:
            raise ConfigurationError("Failed to get an accounts.ambry entry from the configuration. ( It's usually in {}. ) ".format(
                rc.USER_ACCOUNTS))

        if not ambry_account.get('name') or not ambry_account.get('email'):
            from ambry.run import RunConfig as rc

            raise ConfigurationError("Must set accounts.ambry.email and accounts.ambry.name, usually in {}".format(rc.USER_ACCOUNTS))

        config = {
            'identity': ident.ident_dict,
            'about': {
                'author': ambry_account.get('name'),
                'author_email': ambry_account.get('email'),
                'description': "**include**",  # Can't get YAML to write this properly
                'groups': ['group1', 'group2'],
                'homepage': "https://civicknowledge.org",
                'license': "other-open",
                'maintainer': ambry_account.get('name'),
                'maintainer_email': ambry_account.get('email'),
                'tags': ['tag1', 'tag2'],
                'title': "Bundle title"
            }
        }

        os.makedirs(os.path.join(bundle_dir, 'meta'))

        file_ = os.path.join(bundle_dir, 'bundle.yaml-in')

        yaml.dump(config, file(file_, 'w'), indent=4, default_flow_style=False)

        # Need to edit the YAML file because the !include line is special metadata
        # that is hard ( or impossible ) to write through serialization

        with file(file_, 'r') as f_in:
            with file(os.path.join(bundle_dir, 'bundle.yaml'), 'w') as f_out:
                f_out.write(f_in.read().replace("'**include**'", "!include 'README.md'"))

        os.remove(file_)

        p = lambda x: os.path.join(os.path.dirname(__file__), '..', 'support', x)

        shutil.copy(p('bundle.py'), bundle_dir)


        shutil.copy(p('README.md'), bundle_dir)
        if examples:
            shutil.copy(p('schema.csv'), os.path.join(bundle_dir, 'meta'))
            #shutil.copy(p('about.description.md'), os.path.join(bundle_dir, 'meta')  )

        try:
            self.sync_bundle(bundle_dir)
        except ConflictError as e:

            from ..util import rm_rf

            rm_rf(bundle_dir)
            raise SyncError("Failed to sync bundle at {} ('{}') . Bundle deleted".format(bundle_dir, e.message))
        else:
            self.logger.info("CREATED: {}, {}", ident.fqname, bundle_dir)

        return bundle_dir


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