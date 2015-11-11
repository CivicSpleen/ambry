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
    for start in ('ambry/support', 'ambry/geo/support', 'ambry/ui/templates'):
        for root, dirs, files in os.walk(start):

            for f in files:

                if f.endswith('.pyc'):
                    continue

                path = os.path.join(root, f).replace('ambry/', '')

                l.append(path)

    return {'ambry': l}


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', 'Arguments to pass to py.test')]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = ''

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        if 'capture' not in self.pytest_args:
            # capture arg is not given. Disable capture by default.
            self.pytest_args = self.pytest_args + ' --capture=no'

        errno = pytest.main(self.pytest_args)
        sys.exit(errno)

class Docker(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import os

        self.spawn(['docker', 'build', '-f','support/ambry-docker/Dockerfile',
                    '-t','civicknowledge/ambry', '.'])


requirements = parse_requirements('requirements.txt', session=uuid.uuid1())

d = dict(
    name='ambry',
    version=ambry_meta.__version__,
    description='Data packaging and distribution framework',
    long_description=long_description,
    author=ambry_meta.__author__,
    author_email=ambry_meta.__email__,
    url='https://github.com/CivicKnowledge/ambry',
    packages=find_packages(),
    scripts=['scripts/bambry', 'scripts/ambry', 'scripts/ambry-aliases.sh'],
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
    tests_require=['pytest'],
    extras_require={
        'server': ['paste', 'bottle']
    }
)

setup(**d)
