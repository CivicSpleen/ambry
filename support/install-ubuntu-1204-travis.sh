#!/bin/bash
# Install script for Ubuntu 12.04, which is used in Travis CI
# ACCOUNT=ericbusboom BRANCH=develop apt-get update && apt-get install -y curl
# sh -c "$(curl -fsSL https://raw.githubusercontent.com/$ACCOUNT/ambry/develop/support/install-ubuntu-1204-travis.sh)"

TRAVIS_PYTHON_VERSION=2.7

ACCOUNT=ericbusboom
BRANCH=develop

sudo apt-get update -qq
sudo apt-get install -qq wget bzip2 libpq-dev libspatialindex-dev libffi-dev python python-pip python-dev
sudo apt-get install -qq libspatialite5 libspatialite-dev spatialite-bin libspatialindex-dev libspatialindex-dev 
wget http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -O miniconda.sh
chmod +x miniconda.sh
./miniconda.sh -b
export PATH=/home/travis/miniconda/bin:$PATH
conda update --yes conda
conda install --yes python=$TRAVIS_PYTHON_VERSION pip numpy scipy

pip install -U pip

pip install git+https://github.com/clarinova/pysqlite.git#egg=pysqlite


git clone https://github.com/$ACCOUNT/ambry.git
cd ambry
git checkout $BRANCH
python setup.py install


ambry config install
python setup.py test