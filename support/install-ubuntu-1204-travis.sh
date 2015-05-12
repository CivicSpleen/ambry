#!/bin/bash
# Install script version of the installtion in the .travis file.
# apt-get update && apt-get install -y curl && sh -c "$(curl -fsSL https://raw.githubusercontent.com/ericbusboom/ambry/develop/support/install-ubuntu-1204.sh)"

TRAVIS_PYTHON_VERSION=2.7
travis_retry=


sudo apt-get update -qq
sudo apt-get install -qq wget bzip2 libpq-dev libspatialindex-dev libffi-dev
sudo apt-get install -qq libspatialite5 libspatialite-dev spatialite-bin libspatialindex-dev libspatialindex-dev
wget http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -O miniconda.sh
chmod +x miniconda.sh
./miniconda.sh -b
export PATH=/home/travis/miniconda/bin:$PATH
conda update --yes conda
travis_retry conda install --yes python=$TRAVIS_PYTHON_VERSION pip numpy scipy
travis_retry pip install git+https://github.com/clarinova/pysqlite.git#egg=pysqlite

travis_retry pip install -r requirements.txt
travis_retry python setup.py develop

ambry config install
