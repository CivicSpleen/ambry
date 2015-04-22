#!/usr/bin/env python
# -*- coding: utf-8 -*-

import imp
import os
import sys
import uuid
from pip.req import parse_requirements
from setuptools import setup, find_packages


if sys.version_info <= (2, 6):
    error = 'ERROR: ambry requires Python Version 2.7 or above...exiting.'
    print >> sys.stderr, error
    sys.exit(1)

# Avoiding import so we don't execute ambry.__init__.py, which has imports
# that aren't installed until after installation.
ambry_meta = imp.load_source('_meta', 'ambry/_meta.py')

try:
    # Convert README.md to *.rst expected by pypi
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except (IOError, ImportError):
    long_description = open('README.md').read()


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
    scripts=['scripts/bambry', 'scripts/bambry.bat',
             'scripts/ambry', 'scripts/ambry.bat',
             'scripts/xambry',
             'scripts/ambry-load-sqlite', 'scripts/ambry_build_all'],
    package_data=find_package_data(),
    license=ambry_meta.__license__,
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
    extras_require={
        'pgsql': ['psycopg2'],
        'geo': ['sh', 'gdal'],
        'server': ['paste', 'bottle']}
)

setup(**d)
