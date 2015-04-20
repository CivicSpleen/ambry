#!/usr/bin/env python


from setuptools import setup, find_packages
import sys, re, os.path
from pip.req import parse_requirements

# Avoiding import so we don't execute ambry.__init__.py, which has imports
# that aren't installed until after installation.

__version__ = None # "Declare", actually set in the execfile
__author__ = None
__email__ = None

# Load in the metadata. 
execfile(os.path.join(os.path.dirname(__file__),'ambry/_meta.py'))

if sys.version_info <= (2, 6):
    error = "ERROR: ambry requires Python Version 2.7 or above...exiting."
    print >> sys.stderr, error
    sys.exit(1)

def readme():
    with open(os.path.join(os.path.dirname(__file__),"README.md")) as f:
        return f.read()

def read_requirements(file_name):

    with open(file_name, 'r') as f:
        for line in f.read().split('\n'):

            if re.match(r'(\s*#)|(\s*$)', line):
                continue

            yield line


def find_package_data():
    """Return package_data, because setuptools is too stupid to handle nested directories """
    #
    #return {"ambry": ["support/*"]}

    l = list()

    import os
    for start in ("ambry/support", "ambry/geo/support", "ambry/ui/templates"):
        for root, dirs, files in os.walk(start):

            for f in files:

                if f.endswith('.pyc'):
                    continue

                path = os.path.join(root,f).replace("ambry/",'')

                l.append(path)

    return {"ambry": l }

d = dict(
    name="ambry",
    version=__version__,
    description="Data packaging and distribution framework",
    long_description=readme(),
    author=__author__,
    author_email=__email__,
    url="https://github.com/CivicKnowledge/ambry",
    packages=find_packages(),
    scripts=['scripts/bambry', 'scripts/bambry.bat',
             'scripts/ambry', 'scripts/ambry.bat',
             'scripts/xambry',
             'scripts/ambry-load-sqlite', 'scripts/ambry_build_all'],
    package_data=find_package_data(),
    license="",
    platforms="Posix; MacOS X; Linux",
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
    install_requires=[x for x in reversed([str(x.req) for x in parse_requirements('requirements.txt')])],
    extras_require={"pgsql": ["psycopg2"], "geo": ["sh", "gdal"], "server": ["paste", "bottle"]}
)

setup(**d)
