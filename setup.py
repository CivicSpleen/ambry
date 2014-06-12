#!/usr/bin/env python

#from distribute_setup import use_setuptools
#use_setuptools()

from setuptools import setup, find_packages


import sys, re, os.path

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



def parse_requirements(file_name):
    requirements = []
    for line in read_requirements(file_name):

        if re.match(r'\s*-e\s+', line):  # '-e' is the pip option for 'editable'
            requirements.append(re.sub(r'\s*-e\s+.*#egg=(.*)$', r'\1', line))

        elif re.match(r'\s*-f\s+', line): # '-e' is the pip option for 'install a file HTML list of links'
            pass

        else:
            requirements.append(line)


    if 'linux' in sys.platform:
        requirements.append('pysqlite')

    return requirements

def parse_dependency_links(file_name):
    dependency_links = []

    for line in read_requirements(file_name):
        if re.match(r'\s*-[ef]\s+', line):
            dependency_links.append(re.sub(r'\s*-[ef]\s+', '', line))


    if 'linux' in sys.platform:
        dependency_links.append('git+https://github.com/clarinova/pysqlite.git#egg=pysqlite')

    return dependency_links

def find_package_data():
    """Return package_data, because setuptools is too stupid to handle nested directories """
    #
    #return {"ambry": ["support/*"]}

    l = list()

    import os
    for root, dirs, files in os.walk("ambry/support"):

        for f in files:

            if f.endswith('.pyc'):
                continue

            path = os.path.join(root,f).replace("ambry/support",'support')

            l.append(path)

    return {"ambry": l }

setup(name = "ambry",
      version = __version__,
      description = "Data packaging and distribution framework",
      long_description = readme(),
      author = __author__,
      author_email = __email__,
      url = "https://github.com/clarinova/ambry",
      packages = find_packages(), 
      scripts=['scripts/bambry', 'scripts/ambry', 'scripts/xambry', 'scripts/ambry-load-sqlite'],
      package_data = find_package_data(),
      license = "",
      platforms = "Posix; MacOS X; Linux",
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Environment :: Other Environment',
          'Intended Audience :: Developers',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'Programming Language :: Python :: 2.7',
          'Topic :: Utilities'
          ],
      #zip_safe=False,
      install_requires = parse_requirements('requirements.txt'),
      dependency_links = parse_dependency_links('requirements.txt'),
      extras_require = {"pgsql": ["psycopg2"], "geo": ["sh", "gdal"], "server": ["paste", "bottle"]}
      )
