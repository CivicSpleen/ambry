#!/bin/bash
# Install script for Ubuntu 12.04, which is used in Travis CI

apt-get update
locale-gen en_US.UTF-8

apt-get install -y git gcc wget
apt-get install -y g++ python-dev sqlite3 libsqlite3-dev libpq-dev
apt-get install -y libgdal-dev gdal-bin python-gdal
apt-get install -y libsqlite3-dev libspatialite5 libspatialite-dev spatialite-bin libspatialindex-dev

wget https://bootstrap.pypa.io/get-pip.py
python get-pip.py

pip install git+https://github.com/clarinova/pysqlite.git#egg=pysqlite

git clone https://github.com/CivicKnowledge/ambry.git

cd ambry
git checkout develop

python setup.py install

ambry config install # Installs a development config
