#!/usr/bin/env python
# -*- coding: utf-8 -*-

import imp
import os
import sys
import uuid

from pip.req import parse_requirements
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
from distutils.cmd import Command

if sys.version_info <= (2, 6):
    error = 'ERROR: ambry requires Python Version 2.7 or above...exiting.'
    print >> sys.stderr, error
    sys.exit(1)

# Avoiding import so we don't execute ambry.__init__.py, which has imports
# that aren't installed until after installation.
ambry_meta = imp.load_source('_meta', 'ambry/_meta.py')

long_description = open('README.rst').read()


def find_package_data():
    """ Returns package_data, because setuptools is too stupid to handle nested directories.

    Returns:
        dict: key is "ambry", value is list of paths.
    """

    l = list()
    for start in ('ambry/support', 'ambry/bundle/default_files'):
        for root, dirs, files in os.walk(start):

            for f in files:

                if f.endswith('.pyc'):
                    continue

                path = os.path.join(root, f).replace('ambry/', '')

                l.append(path)

    return {'ambry': l}


class PyTest(TestCommand):
    user_options = [
        ('pytest-args=', 't', 'Arguments to pass to py.test'),

        ('all', 'a', 'Run all tests.'),
        ('unit', 'u', 'Run unit tests only.'),
        ('functional', 'f', 'Run functional tests only.'),
        ('bundle', 'b', 'Run bundle tests only.'),
        ('regression', 'r', 'Run regression tests only.'),

        ('sqlite', 's', 'Run tests on sqlite.'),
        ('postgres', 'p', 'Run tests on postgres.'),
    ]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = ''
        self.unit = 0
        self.regression = 0
        self.bundle = 0
        self.functional = 0
        self.all = 0
        self.sqlite = 0
        self.postgres = 0

    def finalize_options(self):
        TestCommand.finalize_options(self)

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        if self.all:
            self.pytest_args += 'test'
        elif self.unit or self.regression or self.bundle or self.functional:
            if self.unit:
                self.pytest_args += 'test/unit'
            if self.regression:
                self.pytest_args += 'test/regression'
            if self.bundle:
                self.pytest_args += 'test/bundle_tests'
            if self.functional:
                self.pytest_args += 'test/functional'
        else:
            # default case - functional.
            self.pytest_args += 'test/functional'

        if 'capture' not in self.pytest_args:
            # capture arg is not given. Disable capture by default.
            self.pytest_args = self.pytest_args + ' --capture=no'

        if self.postgres and self.sqlite:
            # run tests for both
            print('ERROR: You can not run both - postgres and sqlite. Select exactly one.')
            sys.exit(1)
        elif self.postgres:
            os.environ['AMBRY_TEST_DB'] = 'postgres'
        if self.sqlite:
            os.environ['AMBRY_TEST_DB'] = 'sqlite'

        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


class Docker(Command):

    description = "build or launch a docker image"

    user_options = [
        ('clean', 'C', 'Build without the image cache -- completely rebuild'),
        ('all', 'a', 'Build all of the images'),
        ('base', 'B', 'Build the base docker image, civicknowledge/ambry-base'),
        ('build', 'b', 'Build the ambry docker image, civicknowledge/ambry'),
        ('dev', 'd', 'Build the dev version of the ambry docker image, civicknowledge/ambry'),
        ('db', 'D', 'Build the database image, civicknowledge/postgres'),
        ('numbers', 'n', 'Build the numbers server docker image, civicknowledge/numbers'),
        ('tunnel', 't', 'Build the ssh tunnel docker image, civicknowledge/tunnel'),
        ('ui', 'u', 'Build the user interface image, civicknowledge/ambryui'),
        ('volumes', 'v', 'Build the user interface image, civicknowledge/volumes'),
        ('ckan', 'c', 'Build the CKAN image, civicknowledge/ckan'),
    ]

    def initialize_options(self):

        for long, short, desc in self.user_options:
            setattr(self, long, False)


    def finalize_options(self):

        if self.all:
            for long, short, desc in self.user_options:
                setattr(self, long, True)


    def run(self):
        import os, sys, shutil

        init_args_a = ['docker', 'build'] +(['--no-cache'] if self.clean else [] ) + ['-f' ]

        def tag(n):
            from ambry._meta import __version__
            import subprocess
            import json

            # Inspect the image to get the image id, so we can tag it.
            # FIXME. Instead of parsing the JSON, this should be:
            # docker inspect --format='{{.Id}}' civicknowledge/ambry
            proc = subprocess.Popen("docker inspect civicknowledge/ambry:latest", stdout=subprocess.PIPE, shell=True)
            (out, err) = proc.communicate()
            d = json.loads(out)

            self.spawn(['docker', 'tag', '-f', d[0]['Id'], 'civicknowledge/{}:{}'.format(n,__version__)])


        if self.base:
            self.spawn(init_args_a+[ 'support/docker/base/Dockerfile', '-t', 'civicknowledge/ambry-base', '.'])

        if self.numbers:
            self.spawn(init_args_a+[ 'support/docker/numbers/Dockerfile', '-t', 'civicknowledge/ambry-numbers', '.'])

        if self.build:
            self.spawn(init_args_a+[ 'support/docker/ambry/Dockerfile','-t', 'civicknowledge/ambry', '.'])
            tag('ambry')

        if self.dev:
            self.spawn(init_args_a+[ 'support/docker/dev/Dockerfile', '-t', 'civicknowledge/ambry', '.'])
            tag('ambry')

        def d_build(name):
            """Builder for containers that don't need the context of the while source distribution"""
            init_args = ['docker', 'build'] +(['--no-cache'] if self.clean else [] ) + ['-t']

            self.spawn(init_args + ['civicknowledge/' + name, 'support/docker/' + name + '/'])
            tag(name)

        if self.db:
            d_build('postgres')

        if self.tunnel:
            d_build('tunnel')

        if self.ui:
            d_build('ambryui')

        if self.volumes:
            d_build('volumes')

        if self.ckan:
            d_build('ckan')

tests_require = ['pytest']

if sys.version_info >= (3, 0):
    requirements = parse_requirements('requirements-3.txt', session=uuid.uuid1())
else:
    requirements = parse_requirements('requirements.txt', session=uuid.uuid1())
    tests_require.append('mock')


d = dict(
    name='ambry',
    version=ambry_meta.__version__,
    description='Data packaging and distribution framework',
    long_description=long_description,
    author=ambry_meta.__author__,
    author_email=ambry_meta.__email__,
    url='https://github.com/CivicKnowledge/ambry',
    packages=find_packages(),
    scripts=['scripts/bambry', 'scripts/ambry', 'scripts/ambry-aliases.sh', 'scripts/ambry-ui.sh'],
    package_data=find_package_data(),
    license=ambry_meta.__license__,
    cmdclass={'test': PyTest, 'docker': Docker},
    platforms='Posix; MacOS X; Linux',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Other Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Topic :: Utilities'
    ],
    # zip_safe=False,
    install_requires=[x for x in reversed([str(x.req) for x in requirements])],
    tests_require=tests_require,
    extras_require={
        'server': ['paste', 'bottle']
    }
)

setup(**d)
