"""git repository service.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from . import RepositoryInterface, RepositoryException  # @UnresolvedImport
from ambry.dbexceptions import ConfigurationError

from sh import git
from sh import ErrorReturnCode_1, ErrorReturnCode_128

from ambry.util import get_logger

import logging

global_logger = get_logger(__name__)
global_logger.setLevel(logging.FATAL)


class GitShellService(object):

    """Interact with GIT services using the shell commands."""

    def __init__(self, dir_):
        import os

        self.dir_ = dir_

        if self.dir_:
            self.saved_path = os.getcwd()
            try:
                os.chdir(self.dir_)
            except:
                self.saved_path = None
        else:
            self.saved_path = None

    def __del__(self):  # Should be ContextManager, but not right model ...
        import os
        if self.saved_path:
            os.chdir(self.saved_path)

    @property
    def path(self):
        return self.dir_

    def init(self):
        o = git.init_descriptor()

        if o.exit_code != 0:
            raise RepositoryException("Failed to init git repo: {}".format(o))

        return True

    def init_remote(self, url):

        return git.remote('add', 'origin', url)

    def deinit(self):
        import os
        fn = os.path.join(self.dir_, '.gitignore')
        if os.path.exists(fn):
            os.remove(fn)

        dn = os.path.join(self.dir_, '.git')
        if os.path.exists(dn):
            from ambry.util import rm_rf
            rm_rf(dn)

    def has(self, path):
        pass

    def add(self, path):

        o = git.add(path)

        if o.exit_code != 0:
            raise RepositoryException(
                "Failed to add file {} to  git repo: {}".format(
                    path,
                    o))

        return True

    def stash(self):

        o = git.stash()

        if o.exit_code != 0:
            raise RepositoryException(
                "Failed to stash in  git repo: {}".format(o))

        return True

    def commit(self, message="."):

        try:
            git.commit(a=True, m=message)
        except ErrorReturnCode_1:
            pass

        return True

    def needs_commit(self):
        import os

        try:
            for line in git.status(porcelain=True):
                if line.strip():
                    return True

            return False
        except ErrorReturnCode_128:
            global_logger.error(
                "Needs_commit failed in {}".format(
                    os.getcwd()))
            return False

    def needs_push(self):
        import os

        try:
            for line in git.push('origin', 'master', n=True, porcelain=True):
                if '[up to date]' in line:
                    return False

            return True

        except ErrorReturnCode_128:
            global_logger.error("Needs_push failed in {}".format(os.getcwd()))
            return False

    def needs_init(self):
        import os

        dot_git = os.path.join(os.getcwd(), '.git')
        return not (os.path.exists(dot_git) and os.path.isdir(dot_git))

    def ignore(self, pattern):
        import os

        fn = os.path.join(self.dir_, '.gitignore')

        if os.path.exists(fn):
            with open(fn, 'rb') as f:
                lines = set([line.strip() for line in f])
        else:
            lines = set()

        lines.add(pattern)

        with open(fn, 'wb') as f:
            for line in lines:
                f.write(line + '\n')

    def char_to_line(self, line_proc):

        import StringIO
        sio = StringIO.StringIO('bingo')

        def _rcv(chr_, stdin):
            sio.write(chr_)
            if chr == '\n' or chr_ == ':':
                # This is a total hack, but there is no other way to detect when the line is
                # done being displayed that looking for the last character,
                # which is not a \n
                if not sio.getvalue().endswith('http:') and not sio.getvalue().endswith('https:'):
                    line_proc(sio.getvalue(), stdin)
                    sio.truncate(0)
        return _rcv

    def push(self, username="Noone", password="None"):
        """Push to  remote."""

        def line_proc(line, stdin):

            if "Username for" in line:
                stdin.put(username + "\n")

            elif "Password for" in line:
                stdin.put(password + "\n")

            else:
                print "git-push: ", line.strip()

        rcv = self.char_to_line(line_proc)

        try:
            # This is a super hack. See http://amoffat.github.io/sh/tutorials/2-interacting_with_processes.html
            # for some explaination.
            p = git.push(
                '-u',
                'origin',
                'master',
                _out=rcv,
                _out_bufsize=0,
                _tty_in=True)
            p.exit_code
        except ErrorReturnCode_128:
            raise Exception(
                """Push to repository repository failed. You will need to store or cache credentials.
            You can do this by using ssh, .netrc, or a credential maanger.
            See: https://www.kernel.org/pub/software/scm/git/docs/gitcredentials.html""")

        return True

    def pull(self, username="Noone", password="None"):
        """pull to  remote."""

        def line_proc(line, stdin):

            if "Username for" in line:
                stdin.put(username + "\n")

            elif "Password for" in line:
                stdin.put(password + "\n")

            else:
                print "git-push: ", line.strip()

        rcv = self.char_to_line(line_proc)

        try:
            # This is a super hack. See http://amoffat.github.io/sh/tutorials/2-interacting_with_processes.html
            # for some explaination.
            p = git.pull(_out=rcv, _out_bufsize=0, _tty_in=True)
            p.exit_code
        except ErrorReturnCode_128:
            raise Exception(
                """Push to repository repository failed. You will need to store or cache credentials.
            You can do this by using ssh, .netrc, or a credential maanger.
            See: https://www.kernel.org/pub/software/scm/git/docs/gitcredentials.html""")

        return True

    def clone(self, url, dir_=None):
        import os
        from ambry.orm import ConflictError

        dir_ = self.dir_ if not dir_ else dir_

        if not os.path.exists(dir_):
            git.clone(url, dir_)
        else:
            raise ConflictError("{} already exists".format(dir_))


class GitRepository(RepositoryInterface):

    """classdocs."""

    SUFFIX = '-ambry'

    # @ReservedAssignment
    def __init__(self, service, dir, bundle_dir=None, **kwargs):

        self.service = service
        # Needs to be 'dir' for **config, from yaml file, to work
        self.dir_ = dir
        self._bundle = None
        self._bundle_dir = None
        self._impl = None

        if bundle_dir:
            self.bundle_dir = bundle_dir

        self._dependencies = None

    ##
    # Only a few of the methods use self.service. They should be factored out
    ##

    @property
    def ident(self):
        """Return an identifier for this service."""
        return self.service.ident

    def init_remote(self):
        self.bundle.log("Check existence of repository: {}".format(self.name))

        if not self.service.has(self.name):
            pass
            # raise ConfigurationError("Repo {} already exists. Checkout instead?".format(self.name))
            self.bundle.log("Creating repository: {}".format(self.name))
            self.service.create(self.name)

        self.impl.init_remote(self.service.repo_url(self.name))

    def delete_remote(self):

        if self.service.has(self.name):
            self.bundle.log("Deleting remote: {}".format(self.name))
            self.service.delete(self.name)

    ##
    # Only a few methods use self.dir_
    ##

    @property
    def dir(self):
        """The directory of ..."""
        return self.dir_

    @property
    def dependencies(self):
        """Return a set of dependencies for the source packages."""
        from collections import defaultdict
        import os
        from ambry.identity import Identity
        from ambry.run import import_file

        if not self._dependencies:

            depset = defaultdict(set)

            for root, _, files in os.walk(self.dir_):
                if 'bundle.yaml' in files:

                    rp = os.path.realpath(os.path.join(root, 'bundle.py'))
                    mod = import_file(rp)

                    bundle = mod.Bundle(root)
                    deps = bundle.library.dependencies

                    for _, v in deps.items():
                        ident = Identity.parse_name(v)  # Remove revision
                        # print "XXX {:50s} {:30s} {}".format(v, ident.name,
                        # ident.to_dict())
                        depset[bundle.identity.name].add(ident.name)

            self._dependencies = depset

        return dict(self._dependencies.items())

    ##
    # Only a few methods use self._bundle_dir
    ##

    @property
    def bundle_dir(self):
        if not self._bundle and not self._bundle_dir:
            raise ConfigurationError(
                "Must assign bundle or bundle_dir to repostitory before this operation")

        if self._bundle_dir:
            return self._bundle_dir
        else:
            return self.bundle.bundle_dir

    @bundle_dir.setter
    def bundle_dir(self, bundle_dir):
        self._bundle_dir = bundle_dir

        # Import the bundle file from the directory
        from ambry.run import import_file
        import os
        rp = os.path.realpath(os.path.join(bundle_dir, 'bundle.py'))
        mod = import_file(rp)

        dir_ = os.path.dirname(rp)
        self.bundle = mod.Bundle(dir_)

    @property
    def bundle(self):
        if not self._bundle:
            raise ConfigurationError(
                "Must assign bundle to repostitory before this operation")

        return self._bundle

    @bundle.setter
    def bundle(self, b):
        from ambry.bundle import BuildBundle

        self._bundle = b

        if not isinstance(b, BuildBundle):
            raise ValueError("B parameter must be a build bundle ")

        self._impl = GitShellService(b.bundle_dir)

    @property
    def bundle_ident(self):
        if not self._bundle:
            raise ConfigurationError(
                "Must assign bundle or bundle_dir to repostitory before this operation")

        return self._bundle.identity

    @bundle_ident.setter
    def bundle_ident(self, ident):
        self.bundle_dir = self.source_path(ident)

    @property
    def impl(self):
        if not self._impl:
            raise ConfigurationError(
                "Must assign bundle to repostitory before this operation")

        return self._impl

    # ----

    def source_path(self, ident):
        """Return the absolute directory for a bundle based on its identity."""
        import os

        return os.path.join(self.dir, ident.source_path)

    def init(self):
        """Initialize the repository, both load and the upstream."""
        import os

        self.impl.deinit()

        self.bundle.log("Create .git directory")
        self.impl.init_descriptor()

        self.bundle.log("Create .gitignore")
        for p in ('*.pyc', 'build', '.project', '.pydevproject', 'meta/schema-revised.csv', 'meta/schema-old.csv'):
            self.impl.ignore(p)

        self.bundle.log("Create remote {}".format(self.name))

        self.add('bundle.py')
        self.add('bundle.yaml')
        self.add('README.md')

        if os.path.exists(self.bundle.filesystem.path('meta')):
            self.add('meta/*')

        if os.path.exists(self.bundle.filesystem.path('config')):
            self.add('config/*')

        self.add('.gitignore')

        self.commit('Initial commit')

    def de_init(self):
        self.impl.deinit()

    def is_initialized(self):
        """Return true if this repository has already been initialized."""

    @property
    def name(self):
        return self.bundle.identity.sname + self.SUFFIX

    def create_upstream(self):
        raise NotImplemented()

    def add(self, path):
        """Add a file to the repository."""
        return self.impl.add(path)

    def commit(self, message):
        self.bundle.log("Commit {}".format(self.name))
        return self.impl.commit(message=message)

    def stash(self):
        return self.impl.stash()

    def needs_commit(self):
        return self.impl.needs_commit()

    def needs_push(self):
        return self.impl.needs_push()

    def needs_init(self):
        return self.impl.needs_init()

    def clone(self, url, path):
        """Locate the source for the named bundle from the library and retrieve
        the source."""
        import os

        d = os.path.join(self.dir, path)

        impl = GitShellService(None)

        impl.clone(url, d)

        return d

    def push(self, username="Noone", password="None"):
        """Push any changes to the repository to the origin server."""
        self.bundle.log("Push to remote: {}".format(self.name))
        return self.impl.push(username=username, password=password)

    def pull(self, username="Noone", password="None"):
        """Pull any changes to the repository from the origin server."""
        self.bundle.log("Pull from remote: {}".format(self.name))
        return self.impl.pull(username=username, password=password)

    def register(self, library):
        """Register the source location with the library, and the library
        upstream."""
        raise NotImplemented()

    def ignore(self, path):
        """Ignore a file."""
        raise NotImplemented()

    def bundle_deps(self, name, reverse=False):
        """Dependencies for a particular bundle."""
        from ambry.identity import Identity

        ident = Identity.parse_name(name)
        name = ident.name

        out = []
        all_deps = self.dependencies

        if reverse:

            out = set()

            def reverse_set(name):
                o = set()
                for k, v in all_deps.items():
                    if name in v:
                        o.add(k)
                return o

            deps = reverse_set(name)
            while len(deps):

                out.update(deps)

                next_deps = set()
                for name in deps:
                    next_deps.update(reverse_set(name))

                deps = next_deps

            out = list(out)

        else:

            deps = all_deps.get(ident.name, [])
            while len(deps) > 0:
                out += deps
                next_deps = []
                for d in deps:
                    if d in all_deps:
                        next_deps += all_deps[d]

                deps = next_deps

        final = []

        for n in reversed(out):
            if n not in final:
                final.append(n)

        return final

    @property
    def topo_deps(self):
        """Return the dependencies in topologically sorted groups."""

    def __str__(self):
        return "<GitRepository: account={}, dir={}".format(
            self.service,
            self.dir_)
