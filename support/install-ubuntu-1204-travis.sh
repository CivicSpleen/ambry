#!/bin/bash
# Install script for Ubuntu 12.04, which is used in Travis CI
# apt-get update && apt-get install -y curl && sh -c "$(curl -fsSL https://raw.githubusercontent.com/ericbusboom/ambry/develop/support/install-ubuntu-1204.sh)"

TRAVIS_PYTHON_VERSION=2.7
travis_retry=

apt-get update
locale-gen en_US.UTF-8

apt-get install -y git gcc g++ wget bzip2 libpq-dev libspatialindex-dev

wget http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -O miniconda.sh
chmod +x miniconda.sh
./miniconda.sh -b
export PATH=/home/travis/miniconda/bin:/miniconda/bin:$PATH
conda update --yes conda
travis_retry conda install --yes python=$TRAVIS_PYTHON_VERSION pip numpy scipy
travis_retry conda install --yes python=$TRAVIS_PYTHON_VERSION https://conda.binstar.org/kalefranz pysqlite
travis_retry conda install --yes python=$TRAVIS_PYTHON_VERSION https://conda.binstar.org/jgomezdans libspatialite

git clone https://github.com/CivicKnowledge/ambry.git
cd ambry
git checkout develop

python setup.py install

ambry config install # Installs a development config

